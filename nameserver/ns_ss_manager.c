#include "ns_ss_manager.h"
#include "../common/net_utils.h"
#include "ns_globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <poll.h>
#include <limits.h>

// Helper: Find SS by ID in circular list
StorageServer* get_ss_by_id(int id) {
    // Note: Assumes g_ss_list_mutex is HELD
    if (!g_ss_list_head) return NULL;
    
    StorageServer* curr = g_ss_list_head;
    do {
        if (curr->ss_id == id) return curr;
        curr = curr->next;
    } while (curr != g_ss_list_head);
    
    return NULL;
}

// Helper: Find SS by IP in circular list
static StorageServer* get_ss_by_ip(const char* ip, int client_port) {
    // Note: Assumes g_ss_list_mutex is HELD
    if (!g_ss_list_head) return NULL;
    
    StorageServer* curr = g_ss_list_head;
    do {
        if (strcmp(curr->ip, ip) == 0 && curr->client_port == client_port) {
            return curr;
        }
        curr = curr->next;
    } while (curr != g_ss_list_head);
    
    return NULL;
}

// Helper: Get previous SS in circular list
static StorageServer* get_prev_ss(StorageServer* ss) {
    // Note: Assumes g_ss_list_mutex is HELD
    if (!g_ss_list_head || !ss) return NULL;
    if (g_ss_count == 1) return ss; // Only one node, prev is itself
    
    StorageServer* curr = g_ss_list_head;
    do {
        if (curr->next == ss) return curr;
        curr = curr->next;
    } while (curr != g_ss_list_head);
    
    return NULL;
}

// Helper: Make list circular by connecting tail to head
static void make_circular() {
    // Note: Assumes g_ss_list_mutex is HELD
    if (!g_ss_list_head) return;
    
    StorageServer* tail = g_ss_list_head;
    while (tail->next && tail->next != g_ss_list_head) {
        tail = tail->next;
    }
    tail->next = g_ss_list_head;
}

// Helper: Update backup assignments for all SSs (each backs up prev in circular list)
static void update_backup_assignments() {
    // Note: Assumes g_ss_list_mutex is HELD
    if (!g_ss_list_head) return;
    
    if (g_ss_count == 1) {
        // Single SS: no backup (or backs up itself - you can choose)
        g_ss_list_head->backup_ss_id = -1;
        printf("NS: Single SS - no backup assignment\n");
        return;
    }
    
    StorageServer* curr = g_ss_list_head;
    do {
        StorageServer* prev = get_prev_ss(curr);
        if (prev) {
            curr->backup_ss_id = prev->ss_id;
            printf("NS: SS %d backs up SS %d\n", curr->ss_id, prev->ss_id);
        }
        curr = curr->next;
    } while (curr != g_ss_list_head);
}

void* ss_handler_thread(void* arg) {
    SSThreadArg* ss_arg = (SSThreadArg*)arg;
    int ss_sock = ss_arg->sock_fd;
    char ss_ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &ss_arg->ss_addr.sin_addr, ss_ip_str, INET_ADDRSTRLEN);
    
    printf("SS: New connection from %s. Awaiting registration...\n", ss_ip_str);
    
    // 1. Receive Registration
    MsgHeader header;
    if (recv_header(ss_sock, &header) <= 0 || header.type != MSG_S2N_REGISTER) {
        fprintf(stderr, "SS: Failed to receive registration from %s. Closing.\n", ss_ip_str);
        close(ss_sock);
        free(arg);
        return NULL;
    }
    
    Req_SSRegister reg_payload;
    if (recv_payload(ss_sock, &reg_payload, sizeof(Req_SSRegister)) <= 0) {
        fprintf(stderr, "SS: Failed to receive reg payload from %s. Closing.\n", ss_ip_str);
        close(ss_sock);
        free(arg);
        return NULL;
    }

    // 2. Find or create SS node based on IP<->ID mapping
    StorageServer* ss_node = NULL;
    bool is_recovery = false;
    bool is_new_ss = false;

    pthread_mutex_lock(&g_ss_list_mutex);
    
    // Check if this IP has connected before (IP<->ID mapping)
    ss_node = get_ss_by_ip(reg_payload.ip, reg_payload.client_port);
    
    if (ss_node) {
        // Existing SS reconnecting - reuse same SS_ID
        is_recovery = true;
        printf("SS: Storage Server %d (%s:%d) RECONNECTED (reusing ID).\n", 
                ss_node->ss_id, ss_node->ip, ss_node->client_port);
        
        // Reactivate the node
        ss_node->is_online = true;
        ss_node->is_syncing = false;
        ss_node->last_heartbeat = time(NULL);
        ss_node->sock_fd = ss_sock;
        g_ss_active_count++;
        
    } else {
        // New SS - assign new ID
        is_new_ss = true;
        int new_ss_id = g_ss_id_counter++;
        
        printf("SS: Registering NEW Storage Server %d (%s:%d).\n", 
                new_ss_id, reg_payload.ip, reg_payload.client_port);
        
        ss_node = (StorageServer*)malloc(sizeof(StorageServer));
        memset(ss_node, 0, sizeof(StorageServer));
        ss_node->ss_id = new_ss_id;
        ss_node->sock_fd = ss_sock;
        strncpy(ss_node->ip, reg_payload.ip, 16);
        ss_node->client_port = reg_payload.client_port;
        ss_node->is_online = true;
        ss_node->is_syncing = false;
        ss_node->last_heartbeat = time(NULL);
        ss_node->file_count = 0;
        ss_node->backup_ss_id = -1;
        
        // Add to circular linked list
        if (g_ss_list_head == NULL) {
            // First SS
            g_ss_list_head = ss_node;
            ss_node->next = ss_node; // Points to itself
        } else {
            // Insert at head and maintain circular structure
            StorageServer* tail = get_prev_ss(g_ss_list_head);
            ss_node->next = g_ss_list_head;
            tail->next = ss_node;
            g_ss_list_head = ss_node;
        }
        
        g_ss_count++;
        g_ss_active_count++;
    }
    
    // Receive file list and add to global hash table
    for (uint32_t i = 0; i < reg_payload.file_count; i++) {
        FileMetadata meta;
        if(recv_payload(ss_sock, &meta, sizeof(FileMetadata)) <= 0) {
            fprintf(stderr, "SS: Failed to receive file list from %s. Closing.\n", ss_ip_str);
            ss_node->is_online = false; // Mark as down
            g_ss_active_count--;
            pthread_mutex_unlock(&g_ss_list_mutex);
            close(ss_sock);
            free(arg);
            return NULL;
        }
        
        // Determine backup SS ID from this SS's backup assignment
        int backup_ss_id = ss_node->backup_ss_id;
        
        // Insert into global file mapping hash table (has internal locking)
        file_map_table_insert(g_file_map_table, meta.filename, ss_node->ss_id, backup_ss_id, meta.owner);
    }
    
    // Update file_count based on files reported by SS
    ss_node->file_count = reg_payload.file_count;
    printf("NS: SS %d reported %d files. file_count set to %d.\n", 
           ss_node->ss_id, reg_payload.file_count, ss_node->file_count);
    
    // Update backup assignments for all SSs (each backs up prev in circular list)
    update_backup_assignments();
    
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    // 3. Send ACK with backup assignment info
    Res_SSRegisterAck ack;
    ack.new_ss_id = ss_node->ss_id;
    ack.must_recover = is_recovery;
    ack.backup_of_ss_id = ss_node->backup_ss_id;
    
    send_response(ss_sock, MSG_N2S_REGISTER_ACK, &ack, sizeof(ack));
    
    // 4. Handle recovery synchronization
    if (is_recovery && ss_node->backup_ss_id != -1) {
        // This SS is recovering - need to sync from backup
        pthread_mutex_lock(&g_ss_list_mutex);
        StorageServer* backup_ss = get_ss_by_id(ss_node->backup_ss_id);
        
        if (backup_ss && backup_ss->is_online) {
            printf("NS: Initiating recovery sync for SS %d from backup SS %d\n", 
                   ss_node->ss_id, backup_ss->ss_id);
            
            // Block both SSs during sync
            ss_node->is_syncing = true;
            backup_ss->is_syncing = true;
            pthread_mutex_unlock(&g_ss_list_mutex);
            
            // Send sync command to primary (receiving SS)
            Req_SyncToPrimary sync_to_primary;
            sync_to_primary.backup_ss_id = backup_ss->ss_id;
            strncpy(sync_to_primary.backup_ip, backup_ss->ip, 16);
            sync_to_primary.backup_port = backup_ss->client_port;
            send_response(ss_sock, MSG_N2S_SYNC_TO_PRIMARY, &sync_to_primary, sizeof(sync_to_primary));
            
            // Wait for ACK from primary
            MsgHeader ack_hdr;
            recv_header(ss_sock, &ack_hdr);
            
            // Send sync command to backup (sending SS)
            Req_SyncFromBackup sync_from_backup;
            sync_from_backup.target_ss_id = ss_node->ss_id;
            strncpy(sync_from_backup.target_ip, ss_node->ip, 16);
            sync_from_backup.target_port = ss_node->client_port;
            send_response(backup_ss->sock_fd, MSG_N2S_SYNC_FROM_BACKUP, &sync_from_backup, sizeof(sync_from_backup));
            
            // Wait for ACK from backup
            recv_header(backup_ss->sock_fd, &ack_hdr);
            
            printf("NS: Recovery sync initiated. Both SSs will perform direct transfer.\n");
            
            // Clear is_syncing flags after a reasonable timeout
            // SSs perform direct transfer which NS doesn't monitor
            // We'll clear flags after 60 seconds to prevent permanent blocking
            pthread_mutex_lock(&g_ss_list_mutex);
            ss_node->is_syncing = false;
            backup_ss->is_syncing = false;
            pthread_mutex_unlock(&g_ss_list_mutex);
        } else {
            pthread_mutex_unlock(&g_ss_list_mutex);
            printf("NS: Backup SS %d not available for recovery sync\n", ss_node->backup_ss_id);
        }
    }
    
    // 5. Check if this reconnecting SS is a BACKUP for another SS
    // If so, trigger re-replication from primary to this backup
    // Only do this if we didn't already do primary recovery above
    if (is_recovery && !(ss_node->backup_ss_id != -1)) {
        pthread_mutex_lock(&g_ss_list_mutex);
        
        // Find the primary SS that this SS backs up (next SS in circular list)
        StorageServer* primary_ss = ss_node->next;
        
        if (primary_ss && primary_ss != ss_node && primary_ss->is_online) {
            printf("NS: SS %d is backup for SS %d. Initiating re-replication.\n", 
                   ss_node->ss_id, primary_ss->ss_id);
            
            // Block both SSs during sync
            ss_node->is_syncing = true;
            primary_ss->is_syncing = true;
            pthread_mutex_unlock(&g_ss_list_mutex);
            
            // Send re-replicate command to primary
            Req_ReReplicate re_repl;
            re_repl.backup_ss_id = ss_node->ss_id;
            strncpy(re_repl.backup_ip, ss_node->ip, 16);
            re_repl.backup_port = ss_node->client_port;
            send_response(primary_ss->sock_fd, MSG_N2S_RE_REPLICATE_ALL, &re_repl, sizeof(re_repl));
            
            // Wait for ACK from primary
            MsgHeader ack_hdr;
            recv_header(primary_ss->sock_fd, &ack_hdr);
            
            printf("NS: Re-replication initiated. Primary will send all files to backup.\n");
            
            // Clear is_syncing flags immediately since SSs handle their own sync state
            pthread_mutex_lock(&g_ss_list_mutex);
            ss_node->is_syncing = false;
            primary_ss->is_syncing = false;
            pthread_mutex_unlock(&g_ss_list_mutex);
        } else {
            pthread_mutex_unlock(&g_ss_list_mutex);
        }
    }
    
    // 6. Heartbeat/Command Loop
    struct pollfd pfd;
    pfd.fd = ss_sock;
    pfd.events = POLLIN;
    
    while(ss_node->is_online) {
        // Poll with timeout
        int ret = poll(&pfd, 1, HEARTBEAT_TIMEOUT * 1000);
        
        if (ret < 0) {
            perror("SS: poll failed");
            break;
        }
        
        if (ret == 0) {
            // Timeout
            fprintf(stderr, "SS: Heartbeat timeout from %d (%s). Marking as offline.\n",
                    ss_node->ss_id, ss_node->ip);
            break; 
        }
        
        if (pfd.revents & POLLIN) {
            // Data available
            if (recv_header(ss_sock, &header) <= 0) {
                fprintf(stderr, "SS: Disconnected %d (%s).\n", ss_node->ss_id, ss_node->ip);
                break;
            }
            
            // Handle message
            switch(header.type) {
                case MSG_S2N_HEARTBEAT:
                    pthread_mutex_lock(&g_ss_list_mutex);
                    ss_node->last_heartbeat = time(NULL);
                    pthread_mutex_unlock(&g_ss_list_mutex);
                    break;
                
                default:
                    fprintf(stderr, "SS: Unknown msg type %d from %s\n", 
                            header.type, ss_node->ip);
                    // Clear payload
                    if (header.payload_len > 0) {
                        char* junk = (char*)malloc(header.payload_len);
                        if (junk) {
                            recv_payload(ss_sock, junk, header.payload_len);
                            free(junk);
                        }
                    }
            }
        }
    }

    // 5. Cleanup on disconnect - MARK INACTIVE, DON'T FREE
    pthread_mutex_lock(&g_ss_list_mutex);
    ss_node->is_online = false;
    ss_node->is_syncing = false;
    ss_node->sock_fd = -1;
    g_ss_active_count--;
    printf("NS: SS %d marked INACTIVE (total:%d, active:%d)\n", 
           ss_node->ss_id, g_ss_count, g_ss_active_count);
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    close(ss_sock);
    free(arg);
    printf("SS: Handler thread for %d (%s) terminated.\n", ss_node->ss_id, ss_ip_str);
    return NULL;
}

void check_ss_heartbeats() {
    // This is a backup check, in case the poll() loop fails
    pthread_mutex_lock(&g_ss_list_mutex);
    
    if (!g_ss_list_head) {
        pthread_mutex_unlock(&g_ss_list_mutex);
        return;
    }
    
    StorageServer* curr = g_ss_list_head;
    do {
        if (curr->is_online && (time(NULL) - curr->last_heartbeat > HEARTBEAT_TIMEOUT)) {
            fprintf(stderr, "SS Monitor: Found dead SS %d (%s). Marking offline.\n",
                    curr->ss_id, curr->ip);
            curr->is_online = false;
            curr->is_syncing = false;
            g_ss_active_count--;
            if (curr->sock_fd != -1) {
                close(curr->sock_fd); // Forcibly close socket
                curr->sock_fd = -1;
            }
        }
        curr = curr->next;
    } while (curr != g_ss_list_head);
    
    pthread_mutex_unlock(&g_ss_list_mutex);
}

StorageServer* find_ss_for_file(const char* owner, const char* filename) {
    StorageServer* primary_ss = NULL;
    StorageServer* backup_ss = NULL;
    
    // 1. Check cache (cache key should include owner)
    char cache_key[MAX_USERNAME + MAX_FILENAME + 2];
    snprintf(cache_key, sizeof(cache_key), "%s:%s", owner, filename);
    
    pthread_mutex_lock(&g_cache_mutex);
    primary_ss = (StorageServer*)lru_cache_get(g_file_cache, cache_key);
    pthread_mutex_unlock(&g_cache_mutex);
    
    if (primary_ss && primary_ss->is_online) {
        return primary_ss;
    }

    // 2. Cache miss or SS is down. Look up file in hash table (has internal locking)
    FileMapNode* file_node = file_map_table_search(g_file_map_table, owner, filename);
    
    if (!file_node) {
        // File not found in hash table
        return NULL;
    }
    
    int primary_ss_id = file_node->primary_ss_id;
    int backup_ss_id = file_node->backup_ss_id;
    
    // 3. Find the primary SS by ID
    pthread_mutex_lock(&g_ss_list_mutex);
    primary_ss = get_ss_by_id(primary_ss_id);

    if (primary_ss && primary_ss->is_online) {
        // Found online primary. Update cache (use owner:filename as key)
        pthread_mutex_lock(&g_cache_mutex);
        lru_cache_put(g_file_cache, cache_key, primary_ss);
        pthread_mutex_unlock(&g_cache_mutex);
        
        pthread_mutex_unlock(&g_ss_list_mutex);
        return primary_ss;
    }
    
    // 4. Primary is down. Try backup.
    if (backup_ss_id != -1) {
        backup_ss = get_ss_by_id(backup_ss_id);
        if (backup_ss && backup_ss->is_online) {
            fprintf(stderr, "SS: Primary %d for '%s:%s' is down. Using backup %d.\n",
                    primary_ss_id, owner, filename, backup_ss_id);
            pthread_mutex_unlock(&g_ss_list_mutex);
            return backup_ss;
        }
    }
    
    // 5. Both are down
    pthread_mutex_unlock(&g_ss_list_mutex);
    return NULL;
}

StorageServer* get_ss_for_new_file(const char* filename) {
    // Use round-robin load balancing: select SS with fewest files
    pthread_mutex_lock(&g_ss_list_mutex);
    
    if (!g_ss_list_head) {
        pthread_mutex_unlock(&g_ss_list_mutex);
        fprintf(stderr, "NS: No storage servers registered.\n");
        return NULL;
    }
    
    StorageServer* selected = NULL;
    int min_count = INT_MAX;
    
    // Iterate through circular list to find SS with minimum file_count
    StorageServer* curr = g_ss_list_head;
    do {
        if (curr->is_online && !curr->is_syncing && curr->file_count < min_count) {
            min_count = curr->file_count;
            selected = curr;
        }
        curr = curr->next;
    } while (curr != g_ss_list_head);
    
    // Increment file_count for the selected SS
    if (selected) {
        selected->file_count++;
        printf("NS: Selected SS %d for new file (file_count now: %d)\n", 
               selected->ss_id, selected->file_count);
    } else {
        fprintf(stderr, "NS: No available Storage Server for new file.\n");
    }
    
    pthread_mutex_unlock(&g_ss_list_mutex);
    return selected;
}

int refresh_file_metadata_from_ss(const char* owner, const char* filename) {
    // This function is deprecated - metadata is no longer cached in hash table
    // Just verify file exists in mapping (has internal locking) - use owner+filename
    FileMapNode* file_node = file_map_table_search(g_file_map_table, owner, filename);
    
    return (file_node != NULL) ? 0 : -1;
}

// Get metadata from SS without caching in hash table
// Returns 0 on success, -1 on failure
// Caller must provide a FileMetadata* to store result
int get_file_metadata_from_ss(const char* owner, const char* filename, FileMetadata* out_meta) {
    if (!owner || !filename || !out_meta) {
        return -1;
    }
    
    // 1. Find which SS has this file (has internal locking) - use owner+filename
    FileMapNode* file_node = file_map_table_search(g_file_map_table, owner, filename);
    if (!file_node) {
        return -1; // File not found
    }
    int ss_id = file_node->primary_ss_id;
    
    // 2. Find the SS node
    pthread_mutex_lock(&g_ss_list_mutex);
    StorageServer* ss = g_ss_list_head;
    while (ss && ss->ss_id != ss_id) {
        ss = ss->next;
    }
    if (!ss || !ss->is_online) {
        pthread_mutex_unlock(&g_ss_list_mutex);
        return -1; // SS offline
    }
    
    // 3. Connect to SS
    int ss_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock < 0) {
        pthread_mutex_unlock(&g_ss_list_mutex);
        return -1;
    }
    
    struct sockaddr_in ss_addr;
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss->client_port);
    inet_pton(AF_INET, ss->ip, &ss_addr.sin_addr);
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    if (connect(ss_sock, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        close(ss_sock);
        return -1;
    }
    
    // 4. Send info request
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, filename, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    if (send_response(ss_sock, MSG_N2S_GET_INFO, &req, sizeof(req)) < 0) {
        close(ss_sock);
        return -1;
    }
    
    // 5. Receive response
    MsgHeader header;
    if (recv_header(ss_sock, &header) <= 0) {
        close(ss_sock);
        return -1;
    }
    
    if (header.type != MSG_S2N_FILE_INFO_RES) {
        close(ss_sock);
        return -1; // Error or unexpected response
    }
    
    if (recv_payload(ss_sock, out_meta, header.payload_len) <= 0) {
        close(ss_sock);
        return -1;
    }
    
    close(ss_sock);
    return 0;
}

// Helper structure for format_file_list iterator
typedef struct {
    char** buffer_ptr;
    size_t* buf_size_ptr;
    UserHashTable* access_table;
    const char* username;
    bool list_all;
    bool long_format;
} FormatFileListContext;

// Iterator callback for format_file_list
static void format_file_list_callback(const FileMapNode* node, void* user_data) {
    FormatFileListContext* ctx = (FormatFileListContext*)user_data;
    
    bool has_access = false;
    if (ctx->list_all) {
        has_access = true;
    } else {
        // Check access
        pthread_mutex_lock(&g_access_table_mutex);
        char* perms = user_ht_get_permission(ctx->access_table, ctx->username, node->filename);
        if (perms) has_access = true;
        pthread_mutex_unlock(&g_access_table_mutex);
    }
    
    if (has_access) {
        char line[MAX_PATH + 200];
        if (ctx->long_format) {
            // Query SS for fresh metadata
            // Note: We already have node->primary_ss_id from the iteration
            // Don't call get_file_metadata_from_ss() as it would cause deadlock
            // (iterate holds all locks, get_file_metadata calls search which needs a lock)
            
            FileMetadata meta;
            int ss_id = node->primary_ss_id;
            
            // Get SS connection info
            pthread_mutex_lock(&g_ss_list_mutex);
            StorageServer* ss = g_ss_list_head;
            while (ss && ss->ss_id != ss_id) {
                ss = ss->next;
            }
            
            bool got_metadata = false;
            if (ss && ss->is_online) {
                // Connect to SS
                int ss_sock = socket(AF_INET, SOCK_STREAM, 0);
                if (ss_sock >= 0) {
                    struct sockaddr_in ss_addr;
                    ss_addr.sin_family = AF_INET;
                    ss_addr.sin_port = htons(ss->client_port);
                    inet_pton(AF_INET, ss->ip, &ss_addr.sin_addr);
                    pthread_mutex_unlock(&g_ss_list_mutex);
                    
                    if (connect(ss_sock, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) == 0) {
                        // Send info request
                        Req_FileOp req;
                        memset(&req, 0, sizeof(req));
                        strncpy(req.filename, node->filename, MAX_FILENAME - 1);
                        req.filename[MAX_FILENAME - 1] = '\0';
                        
                        if (send_response(ss_sock, MSG_N2S_GET_INFO, &req, sizeof(req)) >= 0) {
                            // Receive response
                            MsgHeader header;
                            if (recv_header(ss_sock, &header) > 0 && 
                                header.type == MSG_S2N_FILE_INFO_RES) {
                                if (recv_payload(ss_sock, &meta, header.payload_len) > 0) {
                                    got_metadata = true;
                                }
                            }
                        }
                    }
                    close(ss_sock);
                } else {
                    pthread_mutex_unlock(&g_ss_list_mutex);
                }
            } else {
                pthread_mutex_unlock(&g_ss_list_mutex);
            }
            
            if (got_metadata) {
                char time_str[64];
                struct tm local_tm;
                localtime_r(&meta.last_access_time, &local_tm);
                strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M", &local_tm);
                snprintf(line, sizeof(line), "%-30s | %-10s | %-8llu | %-20s | %d\n",
                         node->filename,
                         node->owner,
                         (unsigned long long)meta.size_bytes,
                         time_str,
                         node->primary_ss_id);
            } else {
                // If metadata fetch fails, show minimal info
                snprintf(line, sizeof(line), "%-30s | %-10s | %-8s | %-20s | %d\n",
                         node->filename,
                         node->owner,
                         "N/A",
                         "N/A",
                         node->primary_ss_id);
            }
        } else {
            snprintf(line, sizeof(line), "-> %s\n", node->filename);
        }
        
        // Check if buffer needs resizing
        if (strlen(*(ctx->buffer_ptr)) + strlen(line) + 1 > *(ctx->buf_size_ptr)) {
            size_t new_size = *(ctx->buf_size_ptr) * 2;
            char* new_buffer = (char*)realloc(*(ctx->buffer_ptr), new_size);
            if (new_buffer) {
                *(ctx->buffer_ptr) = new_buffer;
                *(ctx->buf_size_ptr) = new_size;
            }
        }
        strcat(*(ctx->buffer_ptr), line);
    }
}

char* format_file_list(UserHashTable* access_table, const char* username, const char* flags) {
    bool list_all = (strstr(flags, "a") != NULL);
    bool long_format = (strstr(flags, "l") != NULL);
    
    size_t buf_size = 4096;
    char* buffer = (char*)malloc(buf_size);
    buffer[0] = '\0';
    
    if (long_format) {
        snprintf(buffer, buf_size, "%-30s | %-10s | %-8s | %-20s | %s\n",
                 "Filename", "Owner", "Size", "Last Access", "SS_ID");
        strncat(buffer, "----------------------------------------------------------------------------------------\n", buf_size - strlen(buffer) - 1);
    }

    // Set up context for iterator
    FormatFileListContext ctx = {
        .buffer_ptr = &buffer,
        .buf_size_ptr = &buf_size,
        .access_table = access_table,
        .username = username,
        .list_all = list_all,
        .long_format = long_format
    };
    
    // Iterate over all files in the hash table
    file_map_table_iterate(g_file_map_table, format_file_list_callback, &ctx);
    
    return buffer;
}