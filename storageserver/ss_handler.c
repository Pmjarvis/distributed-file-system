#include "ss_handler.h"
#include "ss_globals.h"
#include "ss_logger.h"
#include "ss_file_manager.h"
#include "ss_replicator.h"
#include "../common/net_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>   // For directory scanning during catch-up replication
#ifndef DT_REG
#define DT_REG 8
#endif

void* handle_connection(void* arg) {
    ConnectionArg* conn_arg = (ConnectionArg*)arg;
    int sock = conn_arg->sock_fd;
    char ip[16];
    strncpy(ip, conn_arg->ip_addr, 16);
    ip[15] = '\0'; // Ensure null-termination
    free(conn_arg);
    
    // The first message determines the type of connection
    MsgHeader header;
    if (recv_header(sock, &header) <= 0) {
        ss_log("HANDLER: Connection from %s dropped before identifying", ip);
        close(sock);
        return NULL;
    }
    
    // --- THIS IS A CLIENT ---
    if (header.type >= MSG_C2S_READ && header.type <= MSG_C2S_CHECKPOINT_OP) {
        ss_log("HANDLER: New CLIENT connection from %s (Req: %d)", ip, header.type);
        
        // This is a single-request connection. Process it.
        if (header.type == MSG_C2S_READ) {
            ss_log("HANDLER: Routing to ss_handle_read() for message type %d", header.type);
            Req_FileOp req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_read(sock, &req);
        } else if (header.type == MSG_C2S_STREAM) {
            ss_log("HANDLER: Routing to ss_handle_stream() for message type %d", header.type);
            Req_FileOp req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_stream(sock, &req);
        } else if (header.type == MSG_C2S_WRITE) {
            if (header.payload_len > sizeof(Req_Write_Transaction)) {
                ss_log("HANDLER: Payload too large for WRITE (%d > %lu)", header.payload_len, sizeof(Req_Write_Transaction));
                close(sock);
                return NULL;
            }
            Req_Write_Transaction req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_write_transaction(sock, &req);
            // WRITE transaction handles multiple messages, so return early
            // The socket will be closed after the transaction completes
            close(sock);
            return NULL;
        } else if (header.type == MSG_C2S_UNDO) {
            Req_FileOp req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_undo(sock, &req);
        } else if (header.type == MSG_C2S_CHECKPOINT_OP) {
            Req_Checkpoint req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_checkpoint(sock, &req);
        }
        // NOTE: C2S_WRITE_DATA and C2S_WRITE_ETIRW are handled *inside* ss_handle_write_transaction

    // --- THIS IS THE NAME SERVER ---
    // Include both basic NS->SS file ops and extended recovery / replication control messages.
    } else if ((header.type >= MSG_N2S_CREATE_FILE && header.type <= MSG_N2S_EXEC_GET_CONTENT) ||
               header.type == MSG_N2S_SYNC_FROM_BACKUP ||
               header.type == MSG_N2S_SYNC_TO_PRIMARY ||
               header.type == MSG_N2S_RE_REPLICATE_ALL ||
               header.type == MSG_N2S_UPDATE_BACKUP) {
        ss_log("HANDLER: New NAME SERVER connection from %s (Req: %d)", ip, header.type);
        
        // This is a single-request connection. Process it.
        if (header.type == MSG_N2S_CREATE_FILE) {
            Req_FileOp req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_create_file(sock, &req);
        } else if (header.type == MSG_N2S_DELETE_FILE) {
            Req_FileOp req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_delete_file(sock, &req);
        } else if (header.type == MSG_N2S_GET_INFO) {
            Req_FileOp req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_get_info(sock, &req);
        } else if (header.type == MSG_N2S_EXEC_GET_CONTENT) {
            Req_FileOp req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_get_content_for_exec(sock, &req);
        } else if (header.type == MSG_N2S_SYNC_FROM_BACKUP) {
            Req_SyncFromBackup req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_sync_from_backup(sock, &req);
        } else if (header.type == MSG_N2S_SYNC_TO_PRIMARY) {
            Req_SyncToPrimary req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_sync_to_primary(sock, &req);
        } else if (header.type == MSG_N2S_RE_REPLICATE_ALL) {
            Req_ReReplicate req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_re_replicate_all(sock, &req);
        } else if (header.type == MSG_N2S_UPDATE_BACKUP) {
            Req_UpdateBackup req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_update_backup(sock, &req);
        }

    // --- THIS IS A REPLICATION/RECOVERY CONNECTION (from another SS) ---
    } else if (header.type >= MSG_S2S_REPLICATE_FILE && header.type <= MSG_S2S_RECOVERY_COMPLETE) {
        ss_log("HANDLER: SS-to-SS connection from %s (Req: %d)", ip, header.type);
        
        if (header.type == MSG_S2S_REPLICATE_FILE || header.type == MSG_S2S_DELETE_FILE) {
            // Normal replication (handled by replicator)
            handle_replication_receive(sock);
            return NULL; // Socket closed by handler
        } else if (header.type == MSG_S2S_START_RECOVERY) {
            Req_StartRecovery req;
            recv_payload(sock, &req, header.payload_len);
            ss_handle_recovery_connection(sock, &req);
            return NULL; // Socket closed by handler
        }
        
    } else {
        ss_log("HANDLER: Unknown connection type from %s (first msg %d)", ip, header.type);
    }
    
    ss_log("HANDLER: Closing connection from %s", ip);
    close(sock);
    return NULL;
}

// This function loops for the *duration* of a client's session
void handle_client_session(int client_sock, const char* client_ip) {
    // This function is NOT used in the current design.
    // The main_listener spawns a thread per *connection*,
    // and clients connect *per request*.
    // The original `handle_connection` is correct.
    ss_log("HANDLER: Client session started for %s (DEPRECATED)", client_ip);
    //close(client_sock);
}

// This function is NOT used, see above.
void handle_ns_session(int ns_sock, const char* ns_ip) {
    ss_log("HANDLER: NS session started for %s (DEPRECATED)", ns_ip);
    //close(ns_sock);
}

// Handle backup assignment update from NS
void ss_handle_update_backup(int ns_sock, Req_UpdateBackup* req) {
    ss_log("HANDLER: Received backup assignment update from NS");
    
    // Update global backup information (with mutex protection)
    pthread_mutex_lock(&g_backup_config_mutex);
    if (req->backup_ss_id != -1 && strlen(req->backup_ip) > 0) {
        strncpy(g_backup_ip, req->backup_ip, 16);
        g_backup_ip[15] = '\0';
        g_backup_port = req->backup_port;
        pthread_mutex_unlock(&g_backup_config_mutex);
        
        ss_log("HANDLER: Backup assignment updated - will replicate to %s:%d (SS ID %d)",
               req->backup_ip, req->backup_port, req->backup_ss_id);

        // Trigger immediate full catch-up replication of existing PRIMARY files
        // (Files created before we had a backup would have been skipped.)
        ss_log("HANDLER: Initiating immediate catch-up replication for existing primary files");
        DIR* dir = opendir(SS_FILES_DIR);
        if (!dir) {
            ss_log("HANDLER: Failed to open files directory '%s' for catch-up replication", SS_FILES_DIR);
        } else {
            struct dirent* entry;
            int scheduled = 0;
            while ((entry = readdir(dir)) != NULL) {
                if (entry->d_type != DT_REG) continue;
                FileMetadataNode* node = metadata_table_get(g_metadata_table, entry->d_name);
                if (node) {
                    if (!node->is_backup) {
                        repl_schedule_update(entry->d_name);
                        scheduled++;
                    }
                    free(node);
                }
            }
            closedir(dir);
            ss_log("HANDLER: Catch-up replication scheduling complete (%d primary files queued)", scheduled);
        }
    } else {
        // No backup assigned
        g_backup_ip[0] = '\0';
        g_backup_port = 0;
        pthread_mutex_unlock(&g_backup_config_mutex);
        ss_log("HANDLER: Backup assignment cleared - no backup assigned");
    }
    
    // NOTE: No ACK sent back when called from NS control listener thread
    // (NS sends this message asynchronously and doesn't wait for response)
}
