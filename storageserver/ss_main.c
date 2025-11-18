#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include "../common/protocol.h"
#include "ss_globals.h"
#include "ss_logger.h"
#include "ss_handler.h"
#include "ss_file_manager.h"
#include "ss_replicator.h"
#include "../common/net_utils.h"

// Checkpoint interval in seconds
#define CHECKPOINT_INTERVAL_SECONDS 60

// --- Global Variable Definitions ---
int g_ns_sock = -1;
char g_ss_ip[16];
int g_ss_client_port = -1;
int g_ss_id = -1;
char g_backup_ip[16];
int g_backup_port = -1;
FileLockMap g_file_lock_map;
int g_repl_listen_port = -1; // Local replication listener port (argv[5])
ReplicationQueue g_repl_queue;
MetadataHashTable* g_metadata_table = NULL;
volatile int g_shutdown = 0;  // Graceful shutdown flag
volatile int g_is_syncing = 0; // Recovery sync flag
pthread_mutex_t g_sync_mutex = PTHREAD_MUTEX_INITIALIZER;  // Protects g_is_syncing

// Data directories (set dynamically based on SS ID)
char g_ss_root_dir[256];
char g_ss_files_dir[256];
char g_ss_undo_dir[256];
char g_ss_checkpoint_dir[256];
char g_metadata_db_path[512];
// ---

static void* client_listener_thread(void* arg);
static void* replication_listener_thread(void* arg);
static void* ns_heartbeat_thread(void* arg);
static void* ns_control_listener_thread(void* arg);  // NEW: Listen for NS control messages
static void* checkpoint_thread(void* arg);

// Signal handler for graceful shutdown
static void sigint_handler(int sig) {
    printf("\nReceived signal %d, initiating graceful shutdown...\n", sig);
    g_shutdown = 1;
}

// Checkpoint thread - saves metadata periodically
static void* checkpoint_thread(void* arg) {
    ss_log("CHECKPOINT: Thread started (interval: %d seconds)", 
           CHECKPOINT_INTERVAL_SECONDS);
    
    while (!g_shutdown) {
        // Sleep in small intervals to check shutdown flag frequently
        for (int i = 0; i < CHECKPOINT_INTERVAL_SECONDS && !g_shutdown; i++) {
            sleep(1);
        }
        
        if (g_shutdown) break;  // Exit if shutting down
        
        ss_log("CHECKPOINT: Saving metadata to disk...");
        
        if (metadata_table_save(g_metadata_table, METADATA_DB_PATH) < 0) {
            ss_log("ERROR: Checkpoint save failed!");
        } else {
            ss_log("CHECKPOINT: Metadata saved successfully (%u entries)", 
                   metadata_table_get_count(g_metadata_table));
        }
    }
    
    ss_log("CHECKPOINT: Thread exiting");
    return NULL;
}

// Performs initial registration with the Name Server BEFORE we know our persistent
// storage directory. We intentionally register with file_count = 0. After receiving
// our assigned SS ID we initialize (or reuse) the persistent data directory
// `ss_data_{g_ss_id}` and then load any existing metadata / files.
static pthread_t g_client_tid, g_repl_tid, g_hb_tid, g_ns_control_tid, g_checkpoint_tid; // Thread IDs for proper joins

static void _connect_and_register(const char* ns_ip, int ns_port) {
    g_ns_sock = connect_to_server(ns_ip, ns_port);
    if (g_ns_sock < 0) {
        ss_log("FATAL: Could not connect to Name Server at %s:%d", ns_ip, ns_port);
        exit(1);
    }
    ss_log("MAIN: Connected to Name Server. Performing initial registration (no local files yet loaded)...");

    Req_SSRegister reg;
    strncpy(reg.ip, g_ss_ip, 16);
    reg.client_port = g_ss_client_port;
    // FIX: backup_ip should be THIS SS's IP (for replication listener), not target backup IP
    strncpy(reg.backup_ip, g_ss_ip, 16);  // Both ports are on the same SS!
    reg.backup_port = g_repl_listen_port;  // Advertise LOCAL replication listener port
    reg.file_count = 0; // We will load files AFTER we learn our SS ID
    
    // 1. Send registration header + payload
    send_response(g_ns_sock, MSG_S2N_REGISTER, &reg, sizeof(reg));
    
    // No file list payload (file_count == 0)
    
    // 3. Wait for ACK
    MsgHeader header;
    if (recv_header(g_ns_sock, &header) <= 0 || header.type != MSG_N2S_REGISTER_ACK) {
        ss_log("FATAL: Failed to receive registration ACK from NS");
        close(g_ns_sock);
        exit(1);
    }
    
    Res_SSRegisterAck ack;
    recv_payload(g_ns_sock, &ack, header.payload_len);
    
    g_ss_id = ack.new_ss_id;
    ss_log("MAIN: Registration complete. This SS ID is %d", g_ss_id);

    // Reinitialize log file to match persistent SS ID naming convention (ss_{ss_id}.log)
    log_cleanup();
    char final_log_filename[64];
    snprintf(final_log_filename, sizeof(final_log_filename), "ss_%d.log", g_ss_id);
    log_init(final_log_filename);
    ss_log("MAIN: Persistent log file initialized for SS ID %d", g_ss_id);

    // Initialize directory paths based on REAL SS ID (create only if first time)
    snprintf(g_ss_root_dir, sizeof(g_ss_root_dir), "ss_data_%d", g_ss_id);
    snprintf(g_ss_files_dir, sizeof(g_ss_files_dir), "%s/files", g_ss_root_dir);
    snprintf(g_ss_undo_dir, sizeof(g_ss_undo_dir), "%s/undo", g_ss_root_dir);
    snprintf(g_ss_checkpoint_dir, sizeof(g_ss_checkpoint_dir), "%s/checkpoints", g_ss_root_dir);
    snprintf(g_metadata_db_path, sizeof(g_metadata_db_path), "%s/metadata.db", g_ss_root_dir);
    ss_log("MAIN: Using persistent data directory: %s", g_ss_root_dir);

    ss_create_dirs(); // Creates if missing; idempotent on restart

    // Initialize / load metadata AFTER directory selection
    ss_log("MAIN: Loading metadata hash table (post-registration)...");
    g_metadata_table = metadata_table_load(METADATA_DB_PATH);
    if (!g_metadata_table) {
        ss_log("MAIN: No existing metadata DB found for SS ID %d, creating new table", g_ss_id);
        g_metadata_table = metadata_table_init(OUTER_TABLE_SIZE);
        if (!g_metadata_table) {
            ss_log("FATAL: Failed to initialize metadata hash table");
            exit(1);
        }
    } else {
        ss_log("MAIN: Loaded metadata for %u files from disk", metadata_table_get_count(g_metadata_table));
    }

    // Start replication worker and listener threads EARLY so we don't miss
    // immediate backup assignment / re-replication messages from NS.
    repl_start_worker();

    if (pthread_create(&g_client_tid, NULL, client_listener_thread, NULL) != 0) { ss_log("FATAL: Failed to create client listener thread"); exit(1); }
    if (pthread_create(&g_repl_tid, NULL, replication_listener_thread, NULL) != 0) { ss_log("FATAL: Failed to create replication listener thread"); exit(1); }
    if (pthread_create(&g_hb_tid, NULL, ns_heartbeat_thread, NULL) != 0) { ss_log("FATAL: Failed to create heartbeat thread"); exit(1); }
    if (pthread_create(&g_ns_control_tid, NULL, ns_control_listener_thread, NULL) != 0) { ss_log("FATAL: Failed to create NS control listener thread"); exit(1); }
    if (pthread_create(&g_checkpoint_tid, NULL, checkpoint_thread, NULL) != 0) { ss_log("FATAL: Failed to create checkpoint thread"); exit(1); }
    ss_log("MAIN: Core listener & worker threads started (pre-file-scan). Ready for NS control messages.");

    // Now scan physical files to populate any missing metadata entries
    FileMetadata* file_list_after;
    int scan_count = ss_scan_files(&file_list_after);
    if (scan_count > 0) {
        int added = 0;
        for (int i = 0; i < scan_count; i++) {
            FileMetadata* meta = &file_list_after[i];
            if (!metadata_table_exists(g_metadata_table, meta->filename)) {
                metadata_table_insert(g_metadata_table, meta->filename, meta->owner,
                                      meta->size_bytes, meta->word_count, meta->char_count,
                                      meta->last_access_time, meta->last_modified_time, false);
                added++;
            }
        }
        ss_log("MAIN: File scan complete (%d existing files added post-startup)", added);
        free(file_list_after);
    } else {
        ss_log("MAIN: File scan complete (no existing files found)");
    }

    // Threads will be joined in main() during shutdown. (No detach here.)
    
    // FIX: Use backup info from NS (where to SEND replications)
    if (ack.backup_of_ss_id != -1 && strlen(ack.backup_ss_ip) > 0) {
        strncpy(g_backup_ip, ack.backup_ss_ip, 16);
        g_backup_port = ack.backup_ss_port;
        ss_log("MAIN: Will send replications to: %s:%d", g_backup_ip, g_backup_port);
    } else {
        // No backup assigned
        memset(g_backup_ip, 0, 16);
        g_backup_port = 0;
        ss_log("MAIN: No replication target assigned (single SS or no backup available)");
    }
    
    // Note: Recovery is coordinated by NS via MSG_N2S_SYNC_TO_PRIMARY
    // The NS will initiate recovery if ack.must_recover is true
    if (ack.must_recover) {
        ss_log("MAIN: This is a recovery. Waiting for NS to coordinate recovery sync...");
    }
}

int main(int argc, char* argv[]) {
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <ns_ip> <ns_port> <my_ip> <my_client_port> <my_backup_port>\n", argv[0]);
        fprintf(stderr, "Note: <my_backup_port> is the port this SS listens on for replication.\n");
        exit(1);
    }
    
    char* ns_ip = argv[1];
    int ns_port = atoi(argv[2]);
    strncpy(g_ss_ip, argv[3], 16);
    g_ss_client_port = atoi(argv[4]);
    g_repl_listen_port = atoi(argv[5]); // Local replication listener port
    
    // Initialize g_backup_ip to empty - will be set by NS during registration
    memset(g_backup_ip, 0, 16);
    
    // Install signal handlers for graceful shutdown
    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);
    signal(SIGPIPE, SIG_IGN);  // Ignore SIGPIPE (prevents crash on client disconnect)
    
    // Temporary startup log (will be reinitialized after registration with real SS ID)
    log_init("ss_startup.log");
    ss_log("MAIN: Starting Storage Server...");
    lock_map_init(&g_file_lock_map, 64);
    
    _connect_and_register(ns_ip, ns_port);
    ss_log("MAIN: Server initialization complete (threads already running). Press Ctrl+C for graceful shutdown.");

    // Main loop: wait until shutdown flag is set by signal handler
    while(!g_shutdown) {
        sleep(1); // Low overhead wait
    }

    ss_log("MAIN: Shutdown detected. Joining threads and performing final cleanup...");
    pthread_join(g_client_tid, NULL);
    pthread_join(g_repl_tid, NULL);
    pthread_join(g_hb_tid, NULL);
    pthread_join(g_ns_control_tid, NULL);
    pthread_join(g_checkpoint_tid, NULL);
    
    // Save metadata table to disk (final save on shutdown)
    if (g_metadata_table) {
        ss_log("MAIN: Performing final metadata save (%u entries)...", 
               metadata_table_get_count(g_metadata_table));
        if (metadata_table_save(g_metadata_table, METADATA_DB_PATH) == 0) {
            ss_log("MAIN: Final metadata save successful");
        } else {
            ss_log("ERROR: Final metadata save failed");
        }
        metadata_table_free(g_metadata_table);
    }
    
    close(g_ns_sock);
    repl_shutdown_worker();
    lock_map_destroy(&g_file_lock_map);
    log_cleanup();
    
    return 0;
}

static void* client_listener_thread(void* arg) {
    // FIX: Bind to specific IP address to avoid port conflicts with other SS instances
    int server_fd = setup_listener_socket_on_ip(g_ss_ip, g_ss_client_port);
    if (server_fd < 0) {
        ss_log("FATAL: Could not listen on %s:%d", g_ss_ip, g_ss_client_port);
        exit(1);
    }
    ss_log("MAIN: Listening for Clients and NS on %s:%d", g_ss_ip, g_ss_client_port);
    
    while(!g_shutdown) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        
        // Set timeout on accept so we can check shutdown flag
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        
        int sock = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
        
        if (sock < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                // Timeout - check shutdown flag
                continue;
            }
            if (g_shutdown) break;  // Shutting down
            ss_log("ERROR: Client accept failed: %s", strerror(errno));
            continue;
        }
        
        // Remove timeout from accepted socket (client connections should block)
        struct timeval tv_infinite;
        tv_infinite.tv_sec = 0;
        tv_infinite.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv_infinite, sizeof(tv_infinite));
        
        ConnectionArg* conn_arg = (ConnectionArg*)malloc(sizeof(ConnectionArg));
        conn_arg->sock_fd = sock;
        inet_ntop(AF_INET, &client_addr.sin_addr, conn_arg->ip_addr, 16);
        
        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_connection, (void*)conn_arg) != 0) {
            ss_log("ERROR: Failed to create connection thread for %s", conn_arg->ip_addr);
            close(sock);
            free(conn_arg);
        }
        pthread_detach(tid);
    }
    
    ss_log("CLIENT_LISTENER: Thread exiting");
    close(server_fd);
    return NULL;
}

static void* replication_listener_thread(void* arg) {
    // FIX: Bind to specific IP address to avoid port conflicts with other SS instances
    int server_fd = setup_listener_socket_on_ip(g_ss_ip, g_repl_listen_port);
    if (server_fd < 0) {
        ss_log("FATAL: Could not listen on %s:%d", g_ss_ip, g_repl_listen_port);
        exit(1);
    }
    ss_log("MAIN: Listening for Replication on %s:%d", g_ss_ip, g_repl_listen_port);

    while(!g_shutdown) {
        // Set timeout on accept so we can check shutdown flag
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        
        int sock = accept(server_fd, NULL, NULL);
        if (sock < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                // Timeout - check shutdown flag
                continue;
            }
            if (g_shutdown) break;  // Shutting down
            ss_log("ERROR: Replication accept failed: %s", strerror(errno));
            continue;
        }
        
        // Remove timeout from accepted socket
        struct timeval tv_infinite;
        tv_infinite.tv_sec = 0;
        tv_infinite.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv_infinite, sizeof(tv_infinite));
        
        ss_log("REPL_IN: New incoming replication connection");
        // We can't spawn a thread here, as handle_replication_receive
        // is not thread-safe with itself (it writes to files).
        // This is a simple model; a better one would use a worker pool.
        handle_replication_receive(sock);
    }
    
    ss_log("REPL_LISTENER: Thread exiting");
    close(server_fd);
    return NULL;
}


static void* ns_heartbeat_thread(void* arg) {
    MsgHeader hb_header = { .type = MSG_S2N_HEARTBEAT, .payload_len = 0 };
    
    while(!g_shutdown) {
        sleep(HEARTBEAT_INTERVAL);
        
        if (g_shutdown) break;  // Check shutdown before sending
        
        if (send(g_ns_sock, &hb_header, sizeof(hb_header), 0) <= 0) {
            if (g_shutdown) break;  // Connection may be closed during shutdown
            ss_log("FATAL: Failed to send heartbeat to NS. Connection lost.");
            exit(1); // The NS will mark us as dead. We must restart.
        }
        ss_log_console("Sent heartbeat to NS.");
    }
    
    ss_log("HEARTBEAT: Thread exiting");
    return NULL;
}

// NEW: Dedicated thread to listen for control messages from NS (UPDATE_BACKUP, RE_REPLICATE_ALL, etc.)
static void* ns_control_listener_thread(void* arg) {
    ss_log("NS_CONTROL: Listener thread started for NS control messages");
    
    // Set socket timeout so recv doesn't block forever (allows checking g_shutdown)
    struct timeval tv;
    tv.tv_sec = 1;  // 1 second timeout
    tv.tv_usec = 0;
    if (setsockopt(g_ns_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        ss_log("NS_CONTROL: Warning - failed to set socket timeout");
    }
    
    while (!g_shutdown) {
        MsgHeader header;
        int recv_result = recv_header(g_ns_sock, &header);
        
        if (recv_result <= 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                // Timeout - check shutdown flag and continue
                continue;
            }
            if (g_shutdown) break;  // Expected during shutdown
            ss_log("NS_CONTROL: Lost connection to NS (recv returned %d, errno=%d)", recv_result, errno);
            break;
        }
        
        ss_log("NS_CONTROL: Received message type %d from NS", header.type);
        
        // Handle NS control messages
        if (header.type == MSG_N2S_UPDATE_BACKUP) {
            Req_UpdateBackup req;
            recv_payload(g_ns_sock, &req, header.payload_len);
            ss_handle_update_backup(g_ns_sock, &req);  // Don't send ACK back; NS doesn't expect it on this socket
        } else if (header.type == MSG_N2S_RE_REPLICATE_ALL) {
            Req_ReReplicate req;
            recv_payload(g_ns_sock, &req, header.payload_len);
            ss_handle_re_replicate_all(g_ns_sock, &req);
        } else if (header.type == MSG_N2S_SYNC_FROM_BACKUP) {
            Req_SyncFromBackup req;
            recv_payload(g_ns_sock, &req, header.payload_len);
            ss_handle_sync_from_backup(g_ns_sock, &req);
        } else if (header.type == MSG_N2S_SYNC_TO_PRIMARY) {
            Req_SyncToPrimary req;
            recv_payload(g_ns_sock, &req, header.payload_len);
            ss_handle_sync_to_primary(g_ns_sock, &req);
        } else {
            ss_log("NS_CONTROL: Unexpected message type %d from NS (ignoring)", header.type);
            // Drain payload if any
            if (header.payload_len > 0) {
                char drain_buf[4096];
                uint32_t remaining = header.payload_len;
                while (remaining > 0 && !g_shutdown) {
                    uint32_t chunk = (remaining > sizeof(drain_buf)) ? sizeof(drain_buf) : remaining;
                    ssize_t n = recv(g_ns_sock, drain_buf, chunk, 0);
                    if (n <= 0) break;
                    remaining -= n;
                }
            }
        }
    }
    
    ss_log("NS_CONTROL: Listener thread exiting");
    return NULL;
}