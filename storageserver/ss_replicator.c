#include "ss_replicator.h"
#include "ss_globals.h"
#include "ss_logger.h"
#include "../common/net_utils.h" // <--- FIX 1: ADDED THIS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <errno.h>

static pthread_t g_repl_thread;

static void _do_replication_update(const char* filename) {
    char filepath[MAX_PATH];
    snprintf(filepath, sizeof(filepath), "%s/%s", SS_FILES_DIR, filename);
    
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        ss_log("REPL: Failed to open file for replication: %s", filename);
        return;
    }
    
    struct stat file_stat;
    if (fstat(fd, &file_stat) < 0) {
        ss_log("REPL: Failed to fstat file: %s", filename);
        close(fd);
        return;
    }

    int sock = connect_to_server(g_backup_ip, g_backup_port); // <-- This now compiles
    if (sock < 0) {
        ss_log("REPL: Could not connect to backup server at %s:%d", g_backup_ip, g_backup_port);
        close(fd);
        return;
    }

    ss_log("REPL: Connected to backup. Replicating update for %s...", filename);
    
    // 1. Send header and replication request
    Req_Replicate req;
    
    // --- FIX 2: Use MAX_FILENAME ---
    strncpy(req.filename, filename, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';

    req.file_size = file_stat.st_size;
    send_response(sock, MSG_S2S_REPLICATE_FILE, &req, sizeof(req));
    
    // 2. Send file data using sendfile (efficient)
    off_t offset = 0;
    ssize_t sent_bytes = sendfile(sock, fd, &offset, file_stat.st_size);
    
    if (sent_bytes != file_stat.st_size) {
        ss_log("REPL: Error replicating file %s. Sent %ld, expected %ld", filename, sent_bytes, file_stat.st_size);
    } else {
        ss_log("REPL: Successfully replicated %s (%ld bytes)", filename, sent_bytes);
    }
    
    // 3. Wait for ACK
    MsgHeader ack_header;
    if (recv_header(sock, &ack_header) <= 0) {
        ss_log("REPL: Failed to receive ACK for file %s", filename);
    } else if (ack_header.type != MSG_S2S_ACK) {
        ss_log("REPL: Unexpected response type %d for file %s", ack_header.type, filename);
    } else {
        ss_log("REPL: ACK received for file %s", filename);
    }
    
    close(fd);
    close(sock);
}

static void _do_replication_delete(const char* filename) {
    int sock = connect_to_server(g_backup_ip, g_backup_port); // <-- This now compiles
    if (sock < 0) {
        ss_log("REPL: Could not connect to backup server for DELETE %s", filename);
        return;
    }

    ss_log("REPL: Connected to backup. Replicating delete for %s...", filename);

    Req_FileOp req;

    // --- FIX 3: Use MAX_FILENAME ---
    strncpy(req.filename, filename, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_response(sock, MSG_S2S_DELETE_FILE, &req, sizeof(req));

    // Wait for ACK
    MsgHeader ack_header;
    if (recv_header(sock, &ack_header) <= 0) {
        ss_log("REPL: Failed to receive ACK for delete %s", filename);
    } else if (ack_header.type != MSG_S2S_ACK) {
        ss_log("REPL: Unexpected response type %d for delete %s", ack_header.type, filename);
    } else {
        ss_log("REPL: Replicated delete for %s", filename);
    }

    close(sock);
}


static void* repl_worker_thread(void* arg) {
    while (true) {
        ReplQueueNode* node = repl_queue_pop(&g_repl_queue);
        if (node == NULL) {
            ss_log("REPL: Worker thread shutting down.");
            break; // Shutdown
        }
        
        if (node->operation == MSG_S2S_REPLICATE_FILE) {
            _do_replication_update(node->filename);
        } else if (node->operation == MSG_S2S_DELETE_FILE) {
            _do_replication_delete(node->filename);
        }
        
        free(node);
    }
    return NULL;
}

void repl_start_worker() {
    repl_queue_init(&g_repl_queue);
    if (pthread_create(&g_repl_thread, NULL, repl_worker_thread, NULL) != 0) {
        ss_log("FATAL: Could not create replication worker thread!");
        exit(1);
    }
}

void repl_schedule_update(const char* filename) {
    ss_log("REPL: Scheduling update for %s", filename);
    repl_queue_push(&g_repl_queue, filename, MSG_S2S_REPLICATE_FILE);
}

void repl_schedule_delete(const char* filename) {
    ss_log("REPL: Scheduling delete for %s", filename);
    repl_queue_push(&g_repl_queue, filename, MSG_S2S_DELETE_FILE);
}

void repl_shutdown_worker() {
    repl_queue_shutdown(&g_repl_queue);
    pthread_join(g_repl_thread, NULL);
    repl_queue_destroy(&g_repl_queue);
}

// --- Handle *INCOMING* Replication ---
// (This SS is a backup for another SS)

void handle_replication_receive(int sock) {
    MsgHeader header;
    if (recv_header(sock, &header) <= 0) {
        close(sock);
        return;
    }
    
    char filepath[MAX_PATH];

    if (header.type == MSG_S2S_REPLICATE_FILE) {
        Req_Replicate req;
        if (recv_payload(sock, &req, sizeof(req)) <= 0) {
            close(sock);
            return;
        }
        snprintf(filepath, sizeof(filepath), "%s/%s", SS_FILES_DIR, req.filename);
        
        ss_log("REPL_IN: Receiving replica for %s (%llu bytes)", req.filename, (unsigned long long)req.file_size);
        
        FILE* f = fopen(filepath, "wb");
        if (!f) {
            perror("REPL_IN: Failed to open file for writing");
            close(sock);
            return;
        }

        char buffer[4096];
        uint64_t bytes_received = 0;
        while (bytes_received < req.file_size) {
            ssize_t to_read = (req.file_size - bytes_received > sizeof(buffer)) ? sizeof(buffer) : (req.file_size - bytes_received);
            ssize_t n = recv(sock, buffer, to_read, 0);
            if (n <= 0) break;
            fwrite(buffer, 1, n, f);
            bytes_received += n;
        }
        fclose(f);
        ss_log("REPL_IN: Finished receiving %s", req.filename);
        
    } else if (header.type == MSG_S2S_DELETE_FILE) {
        Req_FileOp req;
        if (recv_payload(sock, &req, sizeof(req)) <= 0) {
            close(sock);
            return;
        }
        snprintf(filepath, sizeof(filepath), "%s/%s", SS_FILES_DIR, req.filename);
        
        ss_log("REPL_IN: Receiving delete for %s", req.filename);
        if (unlink(filepath) != 0) {
            ss_log("REPL_IN: Failed to delete %s: %s", req.filename, strerror(errno));
        }
    }
    
    send_response(sock, MSG_S2S_ACK, NULL, 0); // Send ACK
    close(sock);
}

void handle_recovery_sync() {
    // This SS just came online and must sync from its backup.
    ss_log("RECOVERY: Starting sync from backup at %s:%d...", g_backup_ip, g_backup_port);
    
    // IMPLEMENTATION NOTE:
    // A full recovery sync implementation would require:
    // 1. New protocol messages (MSG_S2S_SYNC_REQUEST, MSG_S2S_SYNC_FILE_LIST, etc.)
    // 2. The backup SS to maintain a list of files it's backing up for this SS
    // 3. Transfer of all files and their checkpoints
    // 4. Transfer of undo history if needed
    //
    // For a basic implementation, we can:
    // - Connect to backup SS
    // - Request a full file list
    // - Receive each file via replication protocol
    //
    // However, without the corresponding receiver implementation on the backup SS,
    // and without the protocol extensions, this would be incomplete.
    //
    // In a production system, the Name Server would coordinate this recovery by:
    // a) Telling the backup SS to send all replica data to the recovering SS
    // b) Providing the recovering SS with a manifest of expected files
    // c) Verifying integrity after transfer
    
    int sock = connect_to_server(g_backup_ip, g_backup_port);
    if (sock < 0) {
        ss_log("RECOVERY: Failed to connect to backup server. Starting with empty state.");
        ss_log("RECOVERY: In production, this would be a critical error requiring operator intervention.");
        return;
    }
    
    // For now, we just log that recovery would happen here
    ss_log("RECOVERY: Connected to backup server.");
    ss_log("RECOVERY: Full sync protocol not implemented - would transfer all files here.");
    ss_log("RECOVERY: Recommendation: Implement MSG_S2S_SYNC_REQUEST protocol or use rsync/similar tool.");
    
    close(sock);
    
    ss_log("RECOVERY: Partial recovery complete. Server may be missing files from backup.");
    ss_log("RECOVERY: Consider manual file synchronization or extending the replication protocol.");
}