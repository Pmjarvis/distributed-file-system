#include "ns_ss_manager.h"
#include "../common/net_utils.h"
#include "ns_globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <poll.h>

// Hash function (djb2) to map filename to SS
static unsigned long hash_filename(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    return hash;
}

static StorageServer* get_ss_by_id(int id) {
    // Note: Assumes g_ss_list_mutex is HELD
    StorageServer* curr = g_ss_list_head;
    while (curr) {
        if (curr->ss_id == id) return curr;
        curr = curr->next;
    }
    return NULL;
}

static StorageServer* get_primary_ss_for_file(const char* filename) {
    // Note: Assumes g_ss_list_mutex is HELD
    if (g_ss_count == 0) return NULL;
    
    unsigned long hash = hash_filename(filename);
    int index = hash % g_ss_count;
    
    StorageServer* curr = g_ss_list_head;
    for (int i = 0; i < index && curr != NULL; i++) {
        curr = curr->next;
    }
    return curr;
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

    // 2. Add/Update SS in global list
    StorageServer* ss_node = NULL;
    bool is_recovery = false;
    int new_ss_id = -1;

    pthread_mutex_lock(&g_ss_list_mutex);
    
    // Check if this SS is already known (by IP and Port)
    StorageServer* curr = g_ss_list_head;
    while(curr) {
        if(strcmp(curr->ip, reg_payload.ip) == 0 && curr->client_port == reg_payload.client_port) {
            ss_node = curr;
            is_recovery = true;
            break;
        }
        curr = curr->next;
    }
    
    if (ss_node) {
        // Recovery
        printf("SS: Storage Server %d (%s:%d) reconnected.\n", 
                ss_node->ss_id, ss_node->ip, ss_node->client_port);
        ss_node->is_online = true;
        ss_node->last_heartbeat = time(NULL);
        ss_node->sock_fd = ss_sock;
        // TODO: Clear old file list
    } else {
        // New SS
        new_ss_id = g_ss_id_counter++;
        printf("SS: Registering new Storage Server %d (%s:%d).\n", 
                new_ss_id, reg_payload.ip, reg_payload.client_port);
        ss_node = (StorageServer*)malloc(sizeof(StorageServer));
        ss_node->ss_id = new_ss_id;
        ss_node->sock_fd = ss_sock;
        strncpy(ss_node->ip, reg_payload.ip, 16);
        ss_node->client_port = reg_payload.client_port;
        ss_node->is_online = true;
        ss_node->last_heartbeat = time(NULL);
        ss_node->file_list_head = NULL;
       // ss_node->backup_ss_id = reg_payload.backup_ss_id; // Will be resolved later
        ss_node->backup_of_ss_id = -1; // Will be set by another SS
        ss_node->next = g_ss_list_head;
        g_ss_list_head = ss_node;
        g_ss_count++;
    }
    
    // Receive file list
    for (uint32_t i = 0; i < reg_payload.file_count; i++) {
        FileMetadata meta;
        if(recv_payload(ss_sock, &meta, sizeof(FileMetadata)) <= 0) {
            fprintf(stderr, "SS: Failed to receive file list from %s. Closing.\n", ss_ip_str);
            ss_node->is_online = false; // Mark as down
            pthread_mutex_unlock(&g_ss_list_mutex);
            close(ss_sock);
            free(arg);
            return NULL;
        }
        add_file_to_ss_list(ss_node, &meta);
    }
    
    // Simple backup assignment: SS N backs up SS N-1. SS 0 backs up SS N-1.
    if (!is_recovery) {
        if (g_ss_count > 1) {
            if (ss_node->next) { // Not the last one in the list
                ss_node->backup_of_ss_id = ss_node->next->ss_id;
                ss_node->next->backup_ss_id = ss_node->ss_id;
            } else { // Last one in list
                StorageServer* first = g_ss_list_head;
                while(first->next) first = first->next;
                ss_node->backup_of_ss_id = first->ss_id;
                first->backup_ss_id = ss_node->ss_id;
            }
        }
    }
    
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    // 3. Send ACK
    Res_SSRegisterAck ack;
    ack.new_ss_id = ss_node->ss_id;
    ack.must_recover = is_recovery && (ss_node->backup_of_ss_id != -1);
    ack.backup_of_ss_id = ss_node->backup_of_ss_id;
    
    send_response(ss_sock, MSG_N2S_REGISTER_ACK, &ack, sizeof(ack));
    
    // 4. Heartbeat/Command Loop
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

    // 5. Cleanup
    pthread_mutex_lock(&g_ss_list_mutex);
    ss_node->is_online = false;
    ss_node->sock_fd = -1;
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    close(ss_sock);
    free(arg);
    printf("SS: Handler thread for %d (%s) terminated.\n", ss_node->ss_id, ss_ip_str);
    return NULL;
}

void check_ss_heartbeats() {
    // This is a backup check, in case the poll() loop fails
    pthread_mutex_lock(&g_ss_list_mutex);
    StorageServer* curr = g_ss_list_head;
    while(curr) {
        if (curr->is_online && (time(NULL) - curr->last_heartbeat > HEARTBEAT_TIMEOUT)) {
             fprintf(stderr, "SS Monitor: Found dead SS %d (%s). Marking offline.\n",
                    curr->ss_id, curr->ip);
            curr->is_online = false;
            if (curr->sock_fd != -1) {
                close(curr->sock_fd); // Forcibly close socket
                curr->sock_fd = -1;
            }
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&g_ss_list_mutex);
}

StorageServer* find_ss_for_file(const char* filename) {
    StorageServer* primary_ss = NULL;
    StorageServer* backup_ss = NULL;
    
    // 1. Check cache
    pthread_mutex_lock(&g_cache_mutex);
    primary_ss = (StorageServer*)lru_cache_get(g_file_cache, filename);
    pthread_mutex_unlock(&g_cache_mutex);
    
    if (primary_ss && primary_ss->is_online) {
        return primary_ss;
    }

    // 2. Cache miss or SS is down. Find primary SS by hash.
    pthread_mutex_lock(&g_ss_list_mutex);
    primary_ss = get_primary_ss_for_file(filename);

    if (primary_ss && primary_ss->is_online) {
        // Found online primary. Update cache.
        pthread_mutex_lock(&g_cache_mutex);
        lru_cache_put(g_file_cache, filename, primary_ss);
        pthread_mutex_unlock(&g_cache_mutex);
        
        pthread_mutex_unlock(&g_ss_list_mutex);
        return primary_ss;
    }
    
    // 3. Primary is down. Find backup.
    if (primary_ss && primary_ss->backup_ss_id != -1) {
        backup_ss = get_ss_by_id(primary_ss->backup_ss_id);
        if (backup_ss && backup_ss->is_online) {
            fprintf(stderr, "SS: Primary %d for '%s' is down. Using backup %d.\n",
                    primary_ss->ss_id, filename, backup_ss->ss_id);
            pthread_mutex_unlock(&g_ss_list_mutex);
            return backup_ss;
        }
    }
    
    // 4. Both are down
    pthread_mutex_unlock(&g_ss_list_mutex);
    return NULL;
}

StorageServer* get_ss_for_new_file(const char* filename) {
    pthread_mutex_lock(&g_ss_list_mutex);
    StorageServer* primary_ss = get_primary_ss_for_file(filename);
    
    if (primary_ss && primary_ss->is_online) {
        pthread_mutex_unlock(&g_ss_list_mutex);
        return primary_ss;
    }
    
    // Primary is down, can't create new file
    pthread_mutex_unlock(&g_ss_list_mutex);
    return NULL;
}

void add_file_to_ss_list(StorageServer* ss, FileMetadata* meta) {
    // Note: Assumes g_ss_list_mutex is HELD
    SSFileNode* new_node = (SSFileNode*)malloc(sizeof(SSFileNode));
    memcpy(&new_node->meta, meta, sizeof(FileMetadata));
    new_node->next = ss->file_list_head;
    ss->file_list_head = new_node;
}

void remove_file_from_ss_list(StorageServer* ss, const char* filename) {
    // Note: Assumes g_ss_list_mutex is HELD
    SSFileNode* curr = ss->file_list_head;
    SSFileNode* prev = NULL;
    
    while(curr) {
        if (strcmp(curr->meta.filename, filename) == 0) {
            if (prev) {
                prev->next = curr->next;
            } else {
                ss->file_list_head = curr->next;
            }
            free(curr);
            return;
        }
        prev = curr;
        curr = curr->next;
    }
}

// --- REMOVED 'static' from this function ---
SSFileNode* find_file_in_ss_list(StorageServer* ss, const char* filename) {
    // Note: Assumes g_ss_list_mutex is HELD
    SSFileNode* curr = ss->file_list_head;
    while(curr) {
        if (strcmp(curr->meta.filename, filename) == 0) {
            return curr;
        }
        curr = curr->next;
    }
    return NULL;
}

char* format_file_list(UserHashTable* access_table, const char* username, const char* flags) {
    bool list_all = (strstr(flags, "a") != NULL);
    bool long_format = (strstr(flags, "l") != NULL);
    
    size_t buf_size = 4096;
    char* buffer = (char*)malloc(buf_size);
    char* ptr = buffer;
    ptr[0] = '\0';
    
    if (long_format) {
        snprintf(ptr, buf_size, "%-30s | %-10s | %-8s | %-20s | %s\n",
                 "Filename", "Owner", "Size", "Last Access", "SS_ID");
        strncat(ptr, "----------------------------------------------------------------------------------------\n", buf_size - strlen(ptr) - 1);
    }

    pthread_mutex_lock(&g_ss_list_mutex);
    StorageServer* curr_ss = g_ss_list_head;
    
    while (curr_ss) {
        if (!curr_ss->is_online) {
            curr_ss = curr_ss->next;
            continue;
        }
        
        SSFileNode* curr_file = curr_ss->file_list_head;
        while(curr_file) {
            bool has_access = false;
            if (list_all) {
                has_access = true;
            } else {
                // Check access
                pthread_mutex_lock(&g_access_table_mutex);
                char* perms = user_ht_get_permission(access_table, username, curr_file->meta.filename);
                if (perms) has_access = true;
                pthread_mutex_unlock(&g_access_table_mutex);
            }
            
            if (has_access) {
                char line[MAX_PATH + 200];
                if (long_format) {
                    char time_str[64];
                    struct tm local_tm;
                    localtime_r(&curr_file->meta.last_access_time, &local_tm);
                    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M", &local_tm);
                    snprintf(line, sizeof(line), "%-30s | %-10s | %-8llu | %-20s | %d\n",
                             curr_file->meta.filename,
                             curr_file->meta.owner,
                             (unsigned long long)curr_file->meta.size_bytes,
                             time_str,
                             curr_ss->ss_id);
                } else {
                    snprintf(line, sizeof(line), "-> %s\n", curr_file->meta.filename);
                }
                
                // Check if buffer needs resizing
                if (strlen(buffer) + strlen(line) + 1 > buf_size) {
                    size_t offset = ptr - buffer;
                    buf_size *= 2;
                    buffer = (char*)realloc(buffer, buf_size);
                    ptr = buffer + offset;
                }
                strcat(ptr, line);
            }
            curr_file = curr_file->next;
        }
        curr_ss = curr_ss->next;
    }
    
    pthread_mutex_unlock(&g_ss_list_mutex);
    return buffer;
}