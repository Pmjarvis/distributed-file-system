#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
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

// --- Global Variable Definitions ---
int g_ns_sock = -1;
char g_ss_ip[16];
int g_ss_client_port = -1;
int g_ss_id = -1;
char g_backup_ip[16];
int g_backup_port = -1;
FileLockMap g_file_lock_map;
ReplicationQueue g_repl_queue;
MetadataHashTable* g_metadata_table = NULL;
// ---

static void* client_listener_thread(void* arg);
static void* replication_listener_thread(void* arg);
static void* ns_heartbeat_thread(void* arg);

static void _connect_and_register(const char* ns_ip, int ns_port) {
    g_ns_sock = connect_to_server(ns_ip, ns_port);
    if (g_ns_sock < 0) {
        ss_log("FATAL: Could not connect to Name Server at %s:%d", ns_ip, ns_port);
        exit(1);
    }
    
    ss_log("MAIN: Connected to Name Server. Scanning local files...");
    
    FileMetadata* file_list;
    int file_count = ss_scan_files(&file_list);
    
    ss_log("MAIN: Found %d local files. Registering...", file_count);
    
    // Populate metadata table for each scanned file
    for (int i = 0; i < file_count; i++) {
        FileMetadata* meta = &file_list[i];
        
        // Check if already in metadata table
        if (!metadata_table_exists(g_metadata_table, meta->filename)) {
            // Insert new entry
            metadata_table_insert(g_metadata_table, meta->filename, meta->owner,
                                meta->size_bytes, meta->word_count, meta->char_count,
                                meta->last_access_time, meta->last_modified_time);
            ss_log("MAIN: Added '%s' to metadata table", meta->filename);
        } else {
            // Update existing entry (file may have changed on disk)
            FileMetadataNode* node = metadata_table_get(g_metadata_table, meta->filename);
            if (node) {
                metadata_table_insert(g_metadata_table, meta->filename, meta->owner,
                                    meta->size_bytes, meta->word_count, meta->char_count,
                                    meta->last_access_time, meta->last_modified_time);
                ss_log("MAIN: Updated '%s' in metadata table", meta->filename);
            }
        }
    }
    
    Req_SSRegister reg;
    strncpy(reg.ip, g_ss_ip, 16);
    reg.client_port = g_ss_client_port;
    strncpy(reg.backup_ip, g_backup_ip, 16);
    reg.backup_port = g_backup_port;
    reg.file_count = file_count;
    
    // 1. Send registration header + payload
    send_response(g_ns_sock, MSG_S2N_REGISTER, &reg, sizeof(reg));
    
    // 2. Send file list
    if (file_count > 0) {
        if (send(g_ns_sock, file_list, file_count * sizeof(FileMetadata), 0) != file_count * sizeof(FileMetadata)) {
            ss_log("FATAL: Failed to send file list to NS");
            free(file_list);
            close(g_ns_sock);
            exit(1);
        }
    }
    free(file_list);
    
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
    
    if (ack.must_recover) {
        ss_log("MAIN: This is a recovery. Starting sync from backup...");
        handle_recovery_sync();
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
    g_backup_port = atoi(argv[5]); // This is *our* backup port
    
    // NOTE: The backup's IP/port for *sending* replicas is not provided in argv.
    // According to the design, the Name Server assigns backup relationships
    // dynamically after registration (see Res_SSRegisterAck.backup_of_ss_id).
    // In a complete implementation, the NS would also provide the backup SS's
    // IP and replication port in the registration response.
    // For now, we use a placeholder value. In production, this would be:
    // 1. Received from NS in the registration ACK, OR
    // 2. Obtained via a separate NS query using the backup_of_ss_id, OR
    // 3. Configured via additional command-line arguments.
    strncpy(g_backup_ip, "127.0.0.1", 16); // Placeholder - should be provided by NS
    
    log_init("ss.log");
    ss_log("MAIN: Starting Storage Server...");
    
    ss_create_dirs();
    lock_map_init(&g_file_lock_map, 64);
    
    // Initialize metadata hash table
    ss_log("MAIN: Initializing metadata hash table...");
    g_metadata_table = metadata_table_load(METADATA_DB_PATH);
    if (!g_metadata_table) {
        ss_log("MAIN: No existing metadata DB found, creating new table");
        g_metadata_table = metadata_table_init(OUTER_TABLE_SIZE);
        if (!g_metadata_table) {
            ss_log("FATAL: Failed to initialize metadata hash table");
            exit(1);
        }
    } else {
        ss_log("MAIN: Loaded metadata for %u files from disk", 
               metadata_table_get_count(g_metadata_table));
    }
    
    _connect_and_register(ns_ip, ns_port);
    
    // Start replication worker
    repl_start_worker();
    
    // Start threads
    pthread_t client_tid, repl_tid, hb_tid;
    
    if (pthread_create(&client_tid, NULL, client_listener_thread, NULL) != 0) {
        ss_log("FATAL: Failed to create client listener thread"); exit(1);
    }
    if (pthread_create(&repl_tid, NULL, replication_listener_thread, NULL) != 0) {
        ss_log("FATAL: Failed to create replication listener thread"); exit(1);
    }
    if (pthread_create(&hb_tid, NULL, ns_heartbeat_thread, NULL) != 0) {
        ss_log("FATAL: Failed to create heartbeat thread"); exit(1);
    }
    
    ss_log("MAIN: All threads started. Server is running.");
    
    pthread_join(client_tid, NULL);
    pthread_join(repl_tid, NULL);
    pthread_join(hb_tid, NULL);
    
    ss_log("MAIN: Shutting down...");
    
    // Save metadata table to disk
    if (g_metadata_table) {
        ss_log("MAIN: Saving metadata table (%u entries)...", 
               metadata_table_get_count(g_metadata_table));
        if (metadata_table_save(g_metadata_table, METADATA_DB_PATH) == 0) {
            ss_log("MAIN: Metadata table saved successfully");
        } else {
            ss_log("ERROR: Failed to save metadata table");
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
    int server_fd = setup_listener_socket(g_ss_client_port);
    if (server_fd < 0) {
        ss_log("FATAL: Could not listen on client port %d", g_ss_client_port);
        exit(1);
    }
    ss_log("MAIN: Listening for Clients and NS on port %d", g_ss_client_port);
    
    while(1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int sock = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
        
        if (sock < 0) {
            ss_log("ERROR: Client accept failed: %s", strerror(errno));
            continue;
        }
        
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
    close(server_fd);
    return NULL;
}

static void* replication_listener_thread(void* arg) {
    int server_fd = setup_listener_socket(g_backup_port);
    if (server_fd < 0) {
        ss_log("FATAL: Could not listen on replication port %d", g_backup_port);
        exit(1);
    }
    ss_log("MAIN: Listening for Replication on port %d", g_backup_port);

    while(1) {
        int sock = accept(server_fd, NULL, NULL);
        if (sock < 0) {
            ss_log("ERROR: Replication accept failed: %s", strerror(errno));
            continue;
        }
        
        ss_log("REPL_IN: New incoming replication connection");
        // We can't spawn a thread here, as handle_replication_receive
        // is not thread-safe with itself (it writes to files).
        // This is a simple model; a better one would use a worker pool.
        handle_replication_receive(sock);
    }
    close(server_fd);
    return NULL;
}


static void* ns_heartbeat_thread(void* arg) {
    MsgHeader hb_header = { .type = MSG_S2N_HEARTBEAT, .payload_len = 0 };
    
    while(1) {
        sleep(HEARTBEAT_INTERVAL);
        if (send(g_ns_sock, &hb_header, sizeof(hb_header), 0) <= 0) {
            ss_log("FATAL: Failed to send heartbeat to NS. Connection lost.");
            exit(1); // The NS will mark us as dead. We must restart.
        }
        ss_log_console("Sent heartbeat to NS.");
    }
    return NULL;
}