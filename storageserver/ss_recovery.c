#include "ss_handler.h"
#include "ss_globals.h"
#include "ss_logger.h"
#include "ss_file_manager.h"
#include "ss_metadata.h"
#include "ss_replicator.h"  // For repl_schedule_update
#include "../common/net_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <time.h>

// DT_REG might not be defined on all systems
#ifndef DT_REG
#define DT_REG 8
#endif

// ===================================================================
// RECOVERY SYNC HANDLERS
// ===================================================================

/**
 * Handler: NS tells this SS (backup) to send all backup files to primary SS
 * This happens when primary SS reconnects after being down
 */
void ss_handle_sync_from_backup(int ns_sock, Req_SyncFromBackup* req) {
    ss_log("RECOVERY: NS requests sync FROM backup TO primary SS %d (%s:%d)",
           req->target_ss_id, req->target_ip, req->target_port);
    
    // NOTE: We do NOT set g_is_syncing anymore!
    // Files will be locked individually during reading/sending.
    // This allows clients to access other files during recovery.
    
    // Note: No ACK sent - NS uses persistent socket for one-way notifications
    
    // Connect to target (primary) SS
    int target_sock = connect_to_server(req->target_ip, req->target_port);
    if (target_sock < 0) {
        ss_log("RECOVERY: Failed to connect to primary SS %d at %s:%d",
               req->target_ss_id, req->target_ip, req->target_port);
        // No need to clear g_is_syncing
        return;
    }
    
    ss_log("RECOVERY: Connected to primary SS %d", req->target_ss_id);
    
    // Send START_RECOVERY message
    Req_StartRecovery start_req;
    start_req.ss_id = g_ss_id;
    start_req.is_primary_recovery = true;  // Primary is recovering from us (backup)
    send_response(target_sock, MSG_S2S_START_RECOVERY, &start_req, sizeof(start_req));
    
    // Get list of all files in our storage
    DIR* dir = opendir(SS_FILES_DIR);
    if (!dir) {
        ss_log("RECOVERY: Failed to open files directory");
        close(target_sock);
        // No need to clear g_is_syncing
        return;
    }
    
    // Count files
    uint32_t file_count = 0;
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_REG) file_count++;
    }
    rewinddir(dir);
    
    ss_log("RECOVERY: Sending %u files to primary SS", file_count);
    
    // Send file list
    Req_FileList file_list;
    file_list.file_count = file_count;
    send_response(target_sock, MSG_S2S_FILE_LIST, &file_list, sizeof(file_list));
    
    // Send each file's metadata
    rewinddir(dir);
    FileMetadata* metas = (FileMetadata*)malloc(file_count * sizeof(FileMetadata));
    if (!metas && file_count > 0) {
        ss_log("RECOVERY: Failed to allocate memory for metadata array");
        closedir(dir);
        close(target_sock);
        // No need to clear g_is_syncing
        return;
    }
    int idx = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG) continue;
        
        // Get metadata from our table
        FileMetadataNode* node = metadata_table_get(g_metadata_table, entry->d_name);
        if (!node) {
            // CRITICAL ERROR: File exists but has no metadata!
            // During recovery, all files MUST have metadata entries.
            ss_log("ERROR: Recovery found file '%s' without metadata entry - SKIPPING", entry->d_name);
            ss_log("ERROR: This file will not be recovered. Metadata may be corrupted.");
            continue; // Skip this file - don't include in recovery
        }
        
        // Copy fields from node to FileMetadata
        strncpy(metas[idx].filename, node->filename, MAX_FILENAME);
        strncpy(metas[idx].owner, node->owner, MAX_USERNAME);
        metas[idx].size_bytes = node->file_size;
        metas[idx].word_count = node->word_count;
        metas[idx].char_count = node->char_count;
        free(node);  // metadata_table_get returns a copy
        
        idx++;
    }
    
    // Send all metadata
    if (file_count > 0) {
        send(target_sock, metas, file_count * sizeof(FileMetadata), 0);
    }
    
    // Now send each file
    rewinddir(dir);
    int files_sent = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG) continue;
        
        // FINE-GRAINED FIX: Acquire read lock for THIS specific file
        // This ensures file is not modified during sending
        FileLock* file_lock = lock_map_get(&g_file_lock_map, entry->d_name);
        pthread_rwlock_rdlock(&file_lock->file_lock);
        ss_log("RECOVERY: Acquired read lock for %s", entry->d_name);
        
        char filepath[MAX_PATH];
        snprintf(filepath, sizeof(filepath), "%s/%s", SS_FILES_DIR, entry->d_name);
        
        // Open file
        int fd = open(filepath, O_RDONLY);
        if (fd < 0) {
            ss_log("RECOVERY: Failed to open file %s", entry->d_name);
            pthread_rwlock_unlock(&file_lock->file_lock);
            continue;
        }
        
        struct stat st;
        fstat(fd, &st);
        
        // Send file via replication protocol
        Req_Replicate repl_req;
        strncpy(repl_req.filename, entry->d_name, MAX_FILENAME);
        repl_req.file_size = st.st_size;
        
        // FIX: Include owner information from metadata
        // CRITICAL: Metadata MUST exist during recovery
        FileMetadataNode* node_for_owner = metadata_table_get(g_metadata_table, entry->d_name);
        if (!node_for_owner) {
            // FATAL ERROR: File exists but has no metadata during recovery!
            ss_log("ERROR: Recovery - file '%s' has no metadata, SKIPPING", entry->d_name);
            close(fd);
            pthread_rwlock_unlock(&file_lock->file_lock);
            continue; // Skip this file
        }
        
        strncpy(repl_req.owner, node_for_owner->owner, MAX_USERNAME);
        free(node_for_owner);
        
        send_response(target_sock, MSG_S2S_REPLICATE_FILE, &repl_req, sizeof(repl_req));
        
        // Send file data
        off_t offset = 0;
        ssize_t sent = sendfile(target_sock, fd, &offset, st.st_size);
        close(fd);
        
        if (sent != (ssize_t)st.st_size) {
            ss_log("RECOVERY: sendfile failed for %s: sent %ld of %ld bytes",
                   entry->d_name, sent, (long)st.st_size);
            pthread_rwlock_unlock(&file_lock->file_lock);
            // Continue with other files instead of aborting entire recovery
            continue;
        }
        
        // Wait for ACK
        MsgHeader ack;
        if (recv_header(target_sock, &ack) <= 0) {
            ss_log("RECOVERY: Failed to receive ACK for %s", entry->d_name);
            pthread_rwlock_unlock(&file_lock->file_lock);
            break;  // Connection lost, abort recovery
        }
        
        if (ack.type != MSG_S2S_ACK) {
            ss_log("RECOVERY: Unexpected message type %d (expected ACK) for %s",
                   ack.type, entry->d_name);
        }
        
        // Release lock for this file
        pthread_rwlock_unlock(&file_lock->file_lock);
        ss_log("RECOVERY: Released read lock for %s", entry->d_name);
        
        files_sent++;
        ss_log("RECOVERY: Sent file %d/%u: %s (%ld bytes)",
               files_sent, file_count, entry->d_name, st.st_size);
    }
    
    closedir(dir);
    free(metas);
    
    // Send completion message
    send_response(target_sock, MSG_S2S_RECOVERY_COMPLETE, NULL, 0);
    close(target_sock);
    
    // No need to clear g_is_syncing - we're using fine-grained file locks
    
    ss_log("RECOVERY: Sync to primary SS %d complete (%d files sent)", req->target_ss_id, files_sent);
}

/**
 * Handler: NS tells this SS (primary) that it needs to recover from backup
 * This happens when this SS reconnects after being down
 */
void ss_handle_sync_to_primary(int ns_sock, Req_SyncToPrimary* req) {
    ss_log("RECOVERY: NS requests sync TO primary (us) FROM backup SS %d (%s:%d)",
           req->backup_ss_id, req->backup_ip, req->backup_port);
    
    // NOTE: We do NOT set g_is_syncing here!
    // This is a passive wait - the backup SS will connect to us when ready.
    // When the recovery connection arrives, ss_handle_recovery_connection will:
    // 1. Set g_is_syncing = 1
    // 2. Receive and restore files
    // 3. Clear g_is_syncing = 0
    // This avoids blocking operations if the backup never connects.
    
    // Note: No ACK sent - NS uses persistent socket for one-way notifications
    
    // Note: We don't initiate connection - backup will connect to us
    // We just wait for incoming SS-to-SS recovery connection
    ss_log("RECOVERY: Waiting for backup SS %d to initiate recovery connection", req->backup_ss_id);
}

/**
 * Handler: NS tells this SS (primary) to re-replicate all files to backup
 * This happens when backup SS reconnects after being down
 * 
 * NOTE: This uses the regular replication mechanism (same as catch-up replication)
 * NOT the recovery protocol (which is for SS-initiated recovery after failure)
 */
void ss_handle_re_replicate_all(int ns_sock, Req_ReReplicate* req) {
    ss_log("RECOVERY: NS requests re-replication of all files to backup SS %d (%s:%d)",
           req->backup_ss_id, req->backup_ip, req->backup_port);
    
    // Note: No ACK sent - NS uses persistent socket for one-way notifications
    
    // Update backup target (same as MSG_N2S_UPDATE_BACKUP does) - with mutex protection
    pthread_mutex_lock(&g_backup_config_mutex);
    strncpy(g_backup_ip, req->backup_ip, sizeof(g_backup_ip));
    g_backup_ip[15] = '\0';
    g_backup_port = req->backup_port;
    pthread_mutex_unlock(&g_backup_config_mutex);
    
    ss_log("RECOVERY: Initiating immediate re-replication for existing primary files");
    
    // Scan existing files and schedule replication (same as catch-up replication)
    DIR* dir = opendir(SS_FILES_DIR);
    if (!dir) {
        ss_log("RECOVERY: Failed to open files directory '%s' for re-replication", SS_FILES_DIR);
        return;
    }
    
    struct dirent* entry;
    int scheduled = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG) continue;
        FileMetadataNode* node = metadata_table_get(g_metadata_table, entry->d_name);
        if (node) {
            if (!node->is_backup) {  // Only replicate primary files
                ss_log("REPL: Scheduling update for %s", node->filename);
                repl_schedule_update(entry->d_name);
                scheduled++;
            }
            free(node);
        }
    }
    closedir(dir);
    
    ss_log("RECOVERY: Re-replication scheduling complete (%d primary files queued)", scheduled);
}

/**
 * Handler: Incoming SS-to-SS recovery connection
 * This SS is receiving files from another SS (either as primary or backup)
 */
void ss_handle_recovery_connection(int sock, Req_StartRecovery* start_req) {
    ss_log("RECOVERY: Incoming recovery connection from SS %d (primary_recovery=%d)",
           start_req->ss_id, start_req->is_primary_recovery);
    
    // NOTE: We do NOT set g_is_syncing anymore!
    // Instead, we'll acquire file locks for each file being recovered.
    // This allows non-recovered files to be accessed normally during recovery.
    
    if (start_req->is_primary_recovery) {
        ss_log("RECOVERY: We are PRIMARY recovering from backup SS %d", start_req->ss_id);
        
        // Clear our existing files first (we were down, backup has fresher data)
        // Use file locks to ensure no concurrent access during deletion
        DIR* dir = opendir(SS_FILES_DIR);
        if (dir) {
            struct dirent* entry;
            while ((entry = readdir(dir)) != NULL) {
                if (entry->d_type == DT_REG) {
                    // Acquire write lock before deleting
                    FileLock* lock = lock_map_get(&g_file_lock_map, entry->d_name);
                    pthread_rwlock_wrlock(&lock->file_lock);
                    
                    char filepath[MAX_PATH];
                    snprintf(filepath, sizeof(filepath), "%s/%s", SS_FILES_DIR, entry->d_name);
                    unlink(filepath);
                    
                    // CRITICAL FIX: Also delete metadata entry
                    // Without this, old metadata remains and may not be properly updated
                    metadata_table_remove(g_metadata_table, entry->d_name);
                    
                    ss_log("RECOVERY: Removed old file and metadata: %s", entry->d_name);
                    
                    pthread_rwlock_unlock(&lock->file_lock);
                }
            }
            closedir(dir);
        }
    } else {
        ss_log("RECOVERY: We are BACKUP receiving fresh replication from primary SS %d", start_req->ss_id);
        
        // Clear old backup files
        // Use file locks to ensure no concurrent access during deletion
        DIR* dir = opendir(SS_FILES_DIR);
        if (dir) {
            struct dirent* entry;
            while ((entry = readdir(dir)) != NULL) {
                if (entry->d_type == DT_REG) {
                    // Acquire write lock before deleting
                    FileLock* lock = lock_map_get(&g_file_lock_map, entry->d_name);
                    pthread_rwlock_wrlock(&lock->file_lock);
                    
                    char filepath[MAX_PATH];
                    snprintf(filepath, sizeof(filepath), "%s/%s", SS_FILES_DIR, entry->d_name);
                    unlink(filepath);
                    
                    // CRITICAL FIX: Also delete metadata entry
                    // Without this, old metadata remains and may not be properly updated
                    metadata_table_remove(g_metadata_table, entry->d_name);
                    
                    ss_log("RECOVERY: Removed old backup file and metadata: %s", entry->d_name);
                    
                    pthread_rwlock_unlock(&lock->file_lock);
                }
            }
            closedir(dir);
        }
    }
    
    // Receive file list
    MsgHeader header;
    recv_header(sock, &header);
    if (header.type != MSG_S2S_FILE_LIST) {
        ss_log("RECOVERY: Expected FILE_LIST, got %d", header.type);
        close(sock);
        return;
    }
    
    Req_FileList file_list;
    recv_payload(sock, &file_list, header.payload_len);
    ss_log("RECOVERY: Will receive %u files", file_list.file_count);
    
    // Receive metadata for all files
    FileMetadata* metas = (FileMetadata*)malloc(file_list.file_count * sizeof(FileMetadata));
    if (!metas && file_list.file_count > 0) {
        ss_log("RECOVERY: Failed to allocate memory for metadata array");
        close(sock);
        // No need to clear g_is_syncing - we're not using it anymore
        return;
    }
    if (file_list.file_count > 0) {
        recv(sock, metas, file_list.file_count * sizeof(FileMetadata), MSG_WAITALL);
    }
    
    // Receive each file
    uint32_t files_received = 0;
    while (files_received < file_list.file_count) {
        recv_header(sock, &header);
        
        if (header.type == MSG_S2S_RECOVERY_COMPLETE) {
            ss_log("RECOVERY: Received completion signal");
            break;
        }
        
        if (header.type != MSG_S2S_REPLICATE_FILE) {
            ss_log("RECOVERY: Expected REPLICATE_FILE, got %d", header.type);
            break;
        }
        
        Req_Replicate req;
        recv_payload(sock, &req, header.payload_len);
        
        // FINE-GRAINED FIX: Acquire write lock for THIS specific file
        // Other files can still be accessed normally during recovery
        FileLock* file_lock = lock_map_get(&g_file_lock_map, req.filename);
        pthread_rwlock_wrlock(&file_lock->file_lock);
        ss_log("RECOVERY: Acquired write lock for %s", req.filename);
        
        char filepath[MAX_PATH];
        snprintf(filepath, sizeof(filepath), "%s/%s", SS_FILES_DIR, req.filename);
        
        FILE* f = fopen(filepath, "wb");
        if (!f) {
            ss_log("RECOVERY: Failed to create file %s", req.filename);
            // Skip file data
            char buffer[4096];
            uint64_t remaining = req.file_size;
            while (remaining > 0) {
                size_t to_read = (remaining > sizeof(buffer)) ? sizeof(buffer) : remaining;
                ssize_t n = recv(sock, buffer, to_read, 0);
                if (n <= 0) break;
                remaining -= n;
            }
            send_response(sock, MSG_S2S_ACK, NULL, 0);
            
            // Release lock for this file
            pthread_rwlock_unlock(&file_lock->file_lock);
            continue;
        }
        
        // Receive file data
        char buffer[4096];
        uint64_t received = 0;
        while (received < req.file_size) {
            size_t to_read = (req.file_size - received > sizeof(buffer)) ? sizeof(buffer) : (req.file_size - received);
            ssize_t n = recv(sock, buffer, to_read, 0);
            if (n <= 0) break;
            fwrite(buffer, 1, n, f);
            received += n;
        }
        fclose(f);
        
        // FIX: Update our metadata table
        // If is_primary_recovery=true: WE are the primary recovering from backup, files are PRIMARY (is_backup=false)
        // If is_primary_recovery=false: WE are the backup recovering from primary, files are BACKUPS (is_backup=true)
        FileMetadata* meta = &metas[files_received];
        time_t now = time(NULL);
        bool is_backup_file = !start_req->is_primary_recovery;  // Opposite of is_primary_recovery flag
        metadata_table_insert(g_metadata_table, meta->filename, meta->owner,
                              meta->size_bytes, meta->word_count, meta->char_count,
                              now, now, is_backup_file);
        
        // Release lock for this file
        pthread_rwlock_unlock(&file_lock->file_lock);
        ss_log("RECOVERY: Released write lock for %s", req.filename);
        
        send_response(sock, MSG_S2S_ACK, NULL, 0);
        files_received++;
        
        ss_log("RECOVERY: Received file %u/%u: %s (%llu bytes)",
               files_received, file_list.file_count, req.filename, req.file_size);
    }
    
    free(metas);
    close(sock);
    
    // No need to clear g_is_syncing - we're using fine-grained file locks instead
    
    ss_log("RECOVERY: Recovery complete! Received %u files from SS %d", files_received, start_req->ss_id);
}
