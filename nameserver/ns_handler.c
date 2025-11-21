#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include "ns_handler.h"
#include "ns_globals.h"
#include "../common/protocol.h"
#include "../common/net_utils.h"
#include "ns_user_manager.h"
#include "ns_ss_manager.h"
#include "ns_access.h"
#include "ns_folders.h"

// --- Helper Functions ---
static bool handle_login(UserSession* session);
static void handle_view(UserSession* session, MsgHeader* header);
static void handle_create(UserSession* session, MsgHeader* header);
static void handle_delete(UserSession* session, MsgHeader* header);
static void handle_info(UserSession* session, MsgHeader* header);
static void handle_access_cmd(UserSession* session, MsgHeader* header);
static void handle_list_users(UserSession* session, MsgHeader* header);
static void handle_exec(UserSession* session, MsgHeader* header);
static void handle_folder_cmd(UserSession* session, MsgHeader* header);
static void handle_req_access(UserSession* session, MsgHeader* header);
static void handle_view_req_access(UserSession* session, MsgHeader* header);
// static void handle_grant_req_access(UserSession* session, MsgHeader* header); // Removed

// For READ/WRITE/STREAM/UNDO/CHECKPOINT
static void handle_ss_redirect(UserSession* session, MsgHeader* header);

// Helper to check ownership
static bool is_owner(const char* username, const char* filename) {
    pthread_mutex_lock(&g_access_table_mutex);
    char* perms = user_ht_get_permission(g_access_table, username, filename);
    // FIX: Check for 'o' character in permission string (format: "rwo")
    bool owner = (perms && strchr(perms, 'o') != NULL);
    pthread_mutex_unlock(&g_access_table_mutex);
    return owner;
}

// --- Main Thread Function ---
void* handle_client_request(void* arg) {
    UserSession* session = (UserSession*)arg;
    int client_sock = session->client_sock;
    
    if (!handle_login(session)) {
        printf("Client failed to log in. Closing connection.\n");
        close(client_sock);
        free(session);
        return NULL;
    }
    
    printf("Client '%s' logged in.\n", session->username);
    
    while(true) {
        MsgHeader header;
        if (recv_header(client_sock, &header) <= 0) {
            printf("Client '%s' disconnected.\n", session->username);
            break;
        }

        switch(header.type) {
            case MSG_C2N_VIEW:
                handle_view(session, &header);
                break;
            case MSG_C2N_CREATE:
                handle_create(session, &header);
                break;
            case MSG_C2N_DELETE:
                handle_delete(session, &header);
                break;
            case MSG_C2N_INFO:
                handle_info(session, &header);
                break;
            case MSG_C2N_LIST_USERS:
                handle_list_users(session, &header);
                break;
            case MSG_C2N_ACCESS_ADD:
            case MSG_C2N_ACCESS_REM:
                handle_access_cmd(session, &header);
                break;
            case MSG_C2N_EXEC_REQ:
                handle_exec(session, &header);
                break;
            case MSG_C2N_FOLDER_CMD:
                handle_folder_cmd(session, &header);
                break;
            
            // These just get an SS location
            case MSG_C2N_READ_REQ:
            case MSG_C2N_STREAM_REQ:
            case MSG_C2N_WRITE_REQ:
            case MSG_C2N_UNDO_REQ:
            case MSG_C2N_CHECKPOINT_REQ:
                handle_ss_redirect(session, &header);
                break;
            
            // Access Requests
            case MSG_C2N_REQ_ACCESS:
                handle_req_access(session, &header);
                break;
            case MSG_C2N_VIEW_REQ_ACCESS:
                handle_view_req_access(session, &header);
                break;
            // MSG_C2N_GRANT_REQ_ACCESS removed - use ADDACCESS instead

            default:
                fprintf(stderr, "Client %s sent unknown command: %d\n", session->username, header.type);
                // --- FIX ---
                send_error_response_to_client(client_sock, "Unknown command");
        }
    }
    
    // --- Cleanup ---
    printf("Cleaning up session for %s.\n", session->username);
    pthread_mutex_lock(&g_user_list_mutex);
    set_user_active(g_user_list, session->username, false);
    save_user_list(g_user_list);
    pthread_mutex_unlock(&g_user_list_mutex);
    
    if (session->folder_hierarchy_root) {
        freeTree(session->folder_hierarchy_root);
    }
    
    close(client_sock);
    free(session);
    return NULL;
}

// --- Handler Implementations ---

static bool handle_login(UserSession* session) {
    MsgHeader header;
    if (recv_header(session->client_sock, &header) <= 0 || header.type != MSG_C2N_LOGIN) {
        return false;
    }
    
    Req_Login payload;
    if (recv_payload(session->client_sock, &payload, header.payload_len) <= 0) {
        return false;
    }
    
    pthread_mutex_lock(&g_user_list_mutex);
    bool success = activate_user(g_user_list, payload.username);
    if (success) {
        save_user_list(g_user_list);
    }
    pthread_mutex_unlock(&g_user_list_mutex);

    if (success) {
        send_response(session->client_sock, MSG_N2C_LOGIN_OK, NULL, 0);
        strncpy(session->username, payload.username, MAX_USERNAME);
        session->folder_hierarchy_root = createNode("ROOT", NODE_ROOT, NULL);
        session->current_directory = session->folder_hierarchy_root;
        return true;
    } else {
        // --- FIX ---
        send_error_response_to_client(session->client_sock, "Login failed: User already active.");
        return false;
    }
}

static void handle_view(UserSession* session, MsgHeader* header) {
    Req_View payload;
    recv_payload(session->client_sock, &payload, header->payload_len);
    
    // format_file_list handles all logic for flags and access
    char* file_list_str = format_file_list(g_access_table, session->username, payload.flags);
    
    Res_View res; // <--- This is now a known type
    strncpy(res.data, file_list_str, MAX_PAYLOAD);
    res.data[MAX_PAYLOAD - 1] = '\0';
    free(file_list_str);
    
    send_response(session->client_sock, MSG_N2C_VIEW_RES, &res, sizeof(res));
}

// Helper: Connects to SS, sends req, gets OK/FAIL ack
static int ss_request_response(StorageServer* ss, MsgType type, void* payload, uint32_t len) {
    // This helper needs to be implemented or adapted
    // For now, let's just connect, send, and check response
    
    int ss_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock < 0) return -1;
    
    struct sockaddr_in ss_addr;
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss->client_port);
    inet_pton(AF_INET, ss->ip, &ss_addr.sin_addr);
    
    if (connect(ss_sock, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("NS: Failed to connect to SS for request");
        close(ss_sock);
        return -1;
    }
    
    // Send request
    send_response(ss_sock, type, payload, len);
    
    // Wait for ACK
    MsgHeader header;
    if (recv_header(ss_sock, &header) <= 0) {
        close(ss_sock);
        return -1;
    }
    
    close(ss_sock);
    
    if (header.type == MSG_S2N_ACK_OK) {
        return 0; // Success
    }
    
    return -1; // Failure
}


static void handle_create(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);
    
    // Check if THIS USER already has a file with this name
    // Different users CAN have files with the same filename
    FileMapNode* existing = file_map_table_search(g_file_map_table, session->username, payload.filename);
    if (existing) {
        // Same user trying to create duplicate file
        send_error_response_to_client(session->client_sock, "You already have a file with this name.");
        return;
    }
    
    StorageServer* ss = get_ss_for_new_file(payload.filename);
    if (!ss) {
        send_error_response_to_client(session->client_sock, "No available Storage Servers to create file.");
        return;
    }
    
    // Get backup SS info
    pthread_mutex_lock(&g_ss_list_mutex);
    int backup_ss_id = ss->backup_ss_id;
    StorageServer* backup_ss = NULL;
    if (backup_ss_id != -1) {
        backup_ss = get_ss_by_id(backup_ss_id);
        if (backup_ss && !backup_ss->is_online) {
            backup_ss = NULL; // Backup is offline
        }
    }
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    // 1. Tell PRIMARY SS to create the (empty) file
    strncpy(payload.username, session->username, MAX_USERNAME);
    int res = ss_request_response(ss, MSG_N2S_CREATE_FILE, &payload, sizeof(payload));
    
    if (res != 0) {
        send_error_response_to_client(session->client_sock, "Primary Storage Server failed to create file.");
        return;
    }
    
    // 2. DO NOT send CREATE to backup SS - let primary SS replicate via normal path
    // This prevents both SSs from thinking they're primary and avoids backup-of-backup issues
    // The primary SS will automatically replicate the empty file to its backup
    if (backup_ss) {
        printf("NS: File %s:%s created on primary SS %d (will be replicated to backup SS %d)\n", 
               session->username, payload.filename, ss->ss_id, backup_ss_id);
    } else {
        printf("NS: File %s:%s created on primary SS %d (no backup available)\n",
               session->username, payload.filename, ss->ss_id);
    }

    // 3. Update Access Control (set owner)
    pthread_mutex_lock(&g_access_table_mutex);
    user_ht_add_permission(g_access_table, session->username, payload.filename, "rwo");
    user_ht_save(g_access_table, DB_PATH);
    pthread_mutex_unlock(&g_access_table_mutex);
    
    // 4. Update NS's internal file mapping hash table
    FileMetadata meta;
    strncpy(meta.filename, payload.filename, MAX_FILENAME - 1);
    meta.filename[MAX_FILENAME - 1] = '\0';
    strncpy(meta.owner, session->username, MAX_USERNAME - 1);
    meta.owner[MAX_USERNAME - 1] = '\0';
    meta.size_bytes = 0;
    meta.word_count = 0;
    meta.char_count = 0;
    meta.last_access_time = time(NULL);
    
    // Insert into hash table (has internal locking)
    file_map_table_insert(g_file_map_table, payload.filename, ss->ss_id, backup_ss_id, session->username);

    // 5. Add to user's folder hierarchy
    createTreeFile(session->current_directory, payload.filename);
    
    send_success_response_to_client(session->client_sock, "File created successfully.");
}

static void handle_delete(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);
    
    // 1. Check permissions (must be owner) - file belongs to current user
    if (!is_owner(session->username, payload.filename)) {
        send_error_response_to_client(session->client_sock, "Access Denied: Only the owner can delete a file.");
        return;
    }
    
    // Get file info to find backup SS
    FileMapNode* file_node = file_map_table_search(g_file_map_table, session->username, payload.filename);
    if (!file_node) {
        send_error_response_to_client(session->client_sock, "File not found.");
        return;
    }
    
    int backup_ss_id = file_node->backup_ss_id;
    
    StorageServer* ss = find_ss_for_file(session->username, payload.filename);
    if (!ss) {
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    // 2. Tell PRIMARY SS to delete file
    int res = ss_request_response(ss, MSG_N2S_DELETE_FILE, &payload, sizeof(payload));
    if (res != 0) {
        send_error_response_to_client(session->client_sock, "Storage Server failed to delete file.");
        return;
    }
    
    // 3. Tell BACKUP SS to delete file (if exists)
    if (backup_ss_id != -1) {
        pthread_mutex_lock(&g_ss_list_mutex);
        StorageServer* backup_ss = get_ss_by_id(backup_ss_id);
        if (backup_ss && backup_ss->is_online) {
            pthread_mutex_unlock(&g_ss_list_mutex);
            printf("NS: Replicating DELETE to backup SS %d for file %s:%s\n", 
                   backup_ss_id, session->username, payload.filename);
            int backup_res = ss_request_response(backup_ss, MSG_N2S_DELETE_FILE, &payload, sizeof(payload));
            if (backup_res != 0) {
                fprintf(stderr, "NS: WARNING - Backup SS %d failed to delete file %s:%s\n",
                        backup_ss_id, session->username, payload.filename);
            }
        } else {
            pthread_mutex_unlock(&g_ss_list_mutex);
        }
    }
    
    // 4. Decrement file_count on the SS (for accurate load balancing)
    pthread_mutex_lock(&g_ss_list_mutex);
    if (ss && ss->file_count > 0) {
        ss->file_count--;
        printf("NS: Decremented file_count for SS %d (now: %d)\n", ss->ss_id, ss->file_count);
    }
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    // 5. Remove from hash table (has internal locking) - use owner+filename as key
    file_map_table_delete(g_file_map_table, session->username, payload.filename);
    
    // 6. Remove from cache (cache key includes owner)
    char cache_key[MAX_USERNAME + MAX_FILENAME + 2];
    snprintf(cache_key, sizeof(cache_key), "%s:%s", session->username, payload.filename);
    pthread_mutex_lock(&g_cache_mutex);
    lru_cache_remove(g_file_cache, cache_key);
    pthread_mutex_unlock(&g_cache_mutex);
    
    // 7. Remove from Access Control - only revoke for THIS user, not all users
    // (Different users can have files with the same filename!)
    pthread_mutex_lock(&g_access_table_mutex);
    user_ht_revoke_permission(g_access_table, session->username, payload.filename);
    user_ht_save(g_access_table, DB_PATH);
    pthread_mutex_unlock(&g_access_table_mutex);
    
    // 8. Remove from folder hierarchy (if it exists)
    Node* file_node2 = findChild(session->current_directory, payload.filename, NODE_FILE);
    if(file_node2) {
        removeChild(session->current_directory, file_node2);
        freeTree(file_node2);
    }
    
    send_success_response_to_client(session->client_sock, "File deleted successfully.");
}

static void handle_info(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);

    // 1. Check permissions (must have WRITE access OR be owner)
    // FIX: Owner always has read+write with "rwo", so checking for 'w' covers owners too
    pthread_mutex_lock(&g_access_table_mutex);
    char* perms = user_ht_get_permission(g_access_table, session->username, payload.filename);
    // FIX: Check for 'w' character in permission string (format: "rwo", "rw", "w")
    bool has_access = (perms && strchr(perms, 'w') != NULL);
    pthread_mutex_unlock(&g_access_table_mutex);
    
    if (!has_access) {
        send_error_response_to_client(session->client_sock, "Access Denied: Write access required for INFO.");
        return;
    }

    // 2. Find the actual owner of the file
    // FIX: Prioritize the requesting user's own file
    char* file_owner = NULL;
    FileMapNode* my_file = file_map_table_search(g_file_map_table, session->username, payload.filename);
    
    if (my_file) {
        file_owner = strdup(session->username);
    } else {
        file_owner = file_map_table_find_owner(g_file_map_table, payload.filename);
    }

    if (!file_owner) {
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    // 3. Find file using the actual owner
    StorageServer* ss = find_ss_for_file(file_owner, payload.filename);
    if (!ss) {
        free(file_owner);
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    // 4. Get fresh metadata from SS
    FileMetadata meta;
    if (get_file_metadata_from_ss(file_owner, payload.filename, &meta) != 0) {
        free(file_owner);
        send_error_response_to_client(session->client_sock, "Failed to get file metadata from Storage Server.");
        return;
    }
    
    // 5. Get owner from hash table (has internal locking)
    FileMapNode* file_node = file_map_table_search(g_file_map_table, file_owner, payload.filename);
    free(file_owner); // Done with the owner string
    
    if (!file_node) {
        send_error_response_to_client(session->client_sock, "File mapping not found on NS.");
        return;
    }
    char owner[MAX_USERNAME];
    strncpy(owner, file_node->owner, MAX_USERNAME - 1);
    owner[MAX_USERNAME - 1] = '\0';
    
    Res_Info res;
    char time_str[64];
    struct tm local_tm;
    localtime_r(&meta.last_access_time, &local_tm);
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M", &local_tm);
    
    snprintf(res.data, MAX_PAYLOAD,
             "File: %s\n"
             "Owner: %s\n"
             "Size: %llu bytes\n"
             "Words: %u\n"
             "Chars: %u\n"
             "Last Access: %s\n",
             payload.filename,
             owner,
             (unsigned long long)meta.size_bytes,
             meta.word_count,
             meta.char_count,
             time_str);
             
    send_response(session->client_sock, MSG_N2C_INFO_RES, &res, sizeof(res));
}

static void handle_access_cmd(UserSession* session, MsgHeader* header) {
    Req_Access payload;
    recv_payload(session->client_sock, &payload, header->payload_len);
    
    // 1. Check if user is owner
    if (!is_owner(session->username, payload.filename)) {
        send_error_response_to_client(session->client_sock, "Access Denied: Only the owner can change permissions.");
        return;
    }
    
    // 2. Check if target user exists
    pthread_mutex_lock(&g_user_list_mutex);
    // --- FIX: This function is now visible ---
    if (find_user(g_user_list, payload.target_user) == NULL) {
        pthread_mutex_unlock(&g_user_list_mutex);
        send_error_response_to_client(session->client_sock, "Target user does not exist.");
        return;
    }
    pthread_mutex_unlock(&g_user_list_mutex);
    
    // 3. Apply change
    pthread_mutex_lock(&g_access_table_mutex);
    if (header->type == MSG_C2N_ACCESS_ADD) {
        const char* perm_str = (payload.perm_flag == 'W') ? "read-write" : "read";
        user_ht_add_permission(g_access_table, payload.target_user, payload.filename, perm_str);
    } else { // MSG_C2N_ACCESS_REM
        user_ht_revoke_permission(g_access_table, payload.target_user, payload.filename);
    }
    // --- FIX ---
    user_ht_save(g_access_table, DB_PATH); // Persist
    pthread_mutex_unlock(&g_access_table_mutex);
    
    // 4. Remove request from list (if exists) - Merged from GRANTACCESS
    if (header->type == MSG_C2N_ACCESS_ADD) {
        pthread_mutex_lock(&g_access_req_mutex);
        AccessRequest* curr = g_access_requests_head;
        AccessRequest* prev = NULL;
        while(curr) {
            if (strcmp(curr->requester, payload.target_user) == 0 &&
                strcmp(curr->filename, payload.filename) == 0) {
                
                if (prev) prev->next = curr->next;
                else g_access_requests_head = curr->next;
                
                free(curr);
                break; // Assume only one request per user/file
            }
            prev = curr;
            curr = curr->next;
        }
        pthread_mutex_unlock(&g_access_req_mutex);
    }
    
    send_success_response_to_client(session->client_sock, "Permissions updated.");
}

static void handle_list_users(UserSession* session, MsgHeader* header) {
    pthread_mutex_lock(&g_user_list_mutex);
    char* user_list_str = get_all_users_string(g_user_list);
    pthread_mutex_unlock(&g_user_list_mutex);
    
    if (user_list_str) {
        Res_ListUsers res;
        strncpy(res.data, user_list_str, MAX_PAYLOAD);
        res.data[MAX_PAYLOAD - 1] = '\0';
        send_response(session->client_sock, MSG_N2C_LIST_USERS_RES, &res, sizeof(res));
        free(user_list_str);
    } else {
        send_error_response_to_client(session->client_sock, "Failed to generate user list.");
    }
}

static void handle_exec(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);

    // 1. Check Read Access
    pthread_mutex_lock(&g_access_table_mutex);
    char* perms = user_ht_get_permission(g_access_table, session->username, payload.filename);
    // FIX: Check for 'r' character in permission string (format: "rwo", "rw", "r")
    bool has_access = (perms && strchr(perms, 'r') != NULL);
    pthread_mutex_unlock(&g_access_table_mutex);

    if (!has_access) {
        send_error_response_to_client(session->client_sock, "Access Denied: Read access required to execute.");
        return;
    }

    // 2. Find the actual owner of the file
    // FIX: Prioritize the requesting user's own file
    char* file_owner = NULL;
    FileMapNode* my_file = file_map_table_search(g_file_map_table, session->username, payload.filename);
    
    if (my_file) {
        file_owner = strdup(session->username);
    } else {
        file_owner = file_map_table_find_owner(g_file_map_table, payload.filename);
    }

    if (!file_owner) {
        send_error_response_to_client(session->client_sock, "File not found or SS is offline.");
        return;
    }
    
    // 3. Find SS using the actual owner
    StorageServer* ss = find_ss_for_file(file_owner, payload.filename);
    free(file_owner); // Done with owner string
    
    if (!ss) {
        send_error_response_to_client(session->client_sock, "File not found or SS is offline.");
        return;
    }
    
    // 3. Get file content from SS by connecting and requesting it
    int ss_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock < 0) {
        send_error_response_to_client(session->client_sock, "Failed to create socket for SS connection.");
        return;
    }
    
    struct sockaddr_in ss_addr;
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss->client_port);
    inet_pton(AF_INET, ss->ip, &ss_addr.sin_addr);
    
    if (connect(ss_sock, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        close(ss_sock);
        send_error_response_to_client(session->client_sock, "Failed to connect to Storage Server for EXEC.");
        return;
    }
    
    // Send GET_CONTENT request to SS (reusing MSG_N2S_EXEC_GET_CONTENT message type)
    strncpy(payload.username, session->username, MAX_USERNAME);
    send_response(ss_sock, MSG_N2S_EXEC_GET_CONTENT, &payload, sizeof(payload));
    
    // Receive content from SS
    MsgHeader content_header;
    if (recv_header(ss_sock, &content_header) <= 0) {
        close(ss_sock);
        send_error_response_to_client(session->client_sock, "Failed to receive content from Storage Server.");
        return;
    }
    
    if (content_header.type == MSG_S2N_ACK_FAIL) {
        Res_Error err_payload;
        recv_payload(ss_sock, &err_payload, content_header.payload_len);
        close(ss_sock);
        send_error_response_to_client(session->client_sock, err_payload.msg);
        return;
    }
    
    if (content_header.type != MSG_S2N_EXEC_CONTENT) {
        close(ss_sock);
        send_error_response_to_client(session->client_sock, "Unexpected response from Storage Server.");
        return;
    }
    
    Res_Exec file_content;
    recv_payload(ss_sock, &file_content, content_header.payload_len);
    close(ss_sock);
    
    printf("DEBUG EXEC: Received content from SS: '%s'\n", file_content.output);
    
    // 4. Execute the file content as a bash script on NS
    // Write content to a temporary file, then execute with bash
    char buffer[1024];
    Res_Exec response;
    memset(response.output, 0, MAX_PAYLOAD);
    
    // Create temporary script file
    char temp_script[256];
    snprintf(temp_script, sizeof(temp_script), "/tmp/nfs_exec_%d_%ld.sh", 
             getpid(), time(NULL));
    
    FILE* script_file = fopen(temp_script, "w");
    if (!script_file) {
        send_error_response_to_client(session->client_sock, "Failed to create temporary script file.");
        return;
    }
    
    // Parse content and replace literal "\n" with actual newlines
    char* ptr = file_content.output;
    while (*ptr) {
        if (ptr[0] == '\\' && ptr[1] == 'n') {
            fputc('\n', script_file);
            ptr += 2;
        } else {
            fputc(*ptr, script_file);
            ptr++;
        }
    }
    fclose(script_file);
    
    // Make script executable and run it
    chmod(temp_script, 0700);
    
    char exec_cmd[512];
    snprintf(exec_cmd, sizeof(exec_cmd), "/bin/bash %s 2>&1", temp_script);
    
    printf("DEBUG EXEC: Executing bash script: %s\n", temp_script);
    FILE* pipe = popen(exec_cmd, "r");
    if (!pipe) {
        unlink(temp_script);
        send_error_response_to_client(session->client_sock, "Failed to execute bash script.");
        return;
    }
    
    // Collect output from command execution
    while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
        if (strlen(response.output) + strlen(buffer) < MAX_PAYLOAD - 1) {
            strcat(response.output, buffer);
        }
    }
    int pclose_status = pclose(pipe);
    
    // Clean up temporary file
    unlink(temp_script);
    
    printf("DEBUG EXEC: Command exit status: %d, output length: %zu\n", pclose_status, strlen(response.output));
    printf("DEBUG EXEC: Output: '%s'\n", response.output);
    
    // 5. Send execution output to client
    send_response(session->client_sock, MSG_N2C_EXEC_RES, &response, sizeof(response));
}

static void handle_folder_cmd(UserSession* session, MsgHeader* header) {
    Req_Folder payload;
    recv_payload(session->client_sock, &payload, header->payload_len);
    
    const char* err_msg = NULL;
    char* view_result = NULL;
    Node* new_dir = NULL;
    
    if (strcmp(payload.command, "CREATEFOLDER") == 0) {
        err_msg = createTreeFolder(session->current_directory, payload.arg1);
    } else if (strcmp(payload.command, "VIEWFOLDER") == 0) {
        // VIEWFOLDER shows the current directory or specified path
        Node* target_dir = session->current_directory;
        if (strlen(payload.arg1) > 0) {
            target_dir = resolvePath(session->folder_hierarchy_root, session->current_directory, payload.arg1);
            if (!target_dir) {
                err_msg = "Invalid path.";
            }
        }
        
        if (!err_msg) {
            view_result = viewTreeFolder(target_dir);
            if (!view_result) err_msg = "Error generating folder view.";
        }
    } else if (strcmp(payload.command, "MOVE") == 0) {
        err_msg = moveTreeFile(session->current_directory, payload.arg1, payload.arg2);
    } else if (strcmp(payload.command, "UPMOVE") == 0) {
        err_msg = upMoveTreeFile(session->current_directory, payload.arg1);
    } else if (strcmp(payload.command, "OPEN") == 0) {
        bool create = (strstr(payload.flags, "c") != NULL);
        
        // First check if name collision exists when -c is specified
        if (create) {
            Node* existing = findChildByName(session->current_directory, payload.arg1);
            if (existing) {
                if (existing->type == NODE_FILE) {
                    err_msg = "Cannot create folder: A file with this name already exists.";
                } else {
                    err_msg = "Cannot create folder: A folder with this name already exists.";
                }
            } else {
                new_dir = openTreeFolder(session->current_directory, payload.arg1, create);
                if (new_dir) session->current_directory = new_dir;
                else err_msg = "Failed to create folder.";
            }
        } else {
            new_dir = openTreeFolder(session->current_directory, payload.arg1, create);
            if (new_dir) session->current_directory = new_dir;
            else err_msg = "Folder not found. Use -c flag to create it.";
        }
    } else if (strcmp(payload.command, "OPENPARENT") == 0) {
        new_dir = openTreeParentDirectory(session->current_directory);
        if (new_dir) session->current_directory = new_dir;
        else err_msg = "No parent folder to open (parent is ROOT or you are in ROOT).";
    } else {
        err_msg = "Unknown folder command.";
    }
    
    // Send response
    if (err_msg) {
        send_error_response_to_client(session->client_sock, err_msg);
    } else if (view_result) {
        Res_View res; // <--- This is now a known type
        strncpy(res.data, view_result, MAX_PAYLOAD);
        res.data[MAX_PAYLOAD - 1] = '\0';
        send_response(session->client_sock, MSG_N2C_VIEW_RES, &res, sizeof(res));
        free(view_result);
    } else {
        send_success_response_to_client(session->client_sock, "Folder command successful.");
    }
}

static void handle_ss_redirect(UserSession* session, MsgHeader* header) {
    printf("DEBUG: handle_ss_redirect called, type=%d\n", header->type);
    Req_FileOp payload;
    if (header->type == MSG_C2N_CHECKPOINT_REQ) {
        // For CHECKPOINT routing, the client sends Req_FileOp to NS to resolve SS location.
        // We only need the filename here (tag/command are sent later directly to SS).
        // Previously, we incorrectly read Req_Checkpoint here, corrupting the filename
        // and causing Access Denied for legitimate owners. Read Req_FileOp instead.
        printf("DEBUG: Receiving CHECKPOINT routing payload (Req_FileOp) of size %u\n", header->payload_len);
        recv_payload(session->client_sock, &payload, header->payload_len);
        payload.filename[MAX_FILENAME - 1] = '\0';
    } else {
        printf("DEBUG: Receiving payload of size %u\n", header->payload_len);
        recv_payload(session->client_sock, &payload, header->payload_len);
        printf("DEBUG: Got filename: '%s'\n", payload.filename);
    }
    
    // 1. Check Access Control
    bool has_access = false;
    printf("DEBUG: About to lock g_access_table_mutex\n");
    pthread_mutex_lock(&g_access_table_mutex);
    printf("DEBUG: Mutex locked, checking access for user '%s' on file '%s'\n", session->username, payload.filename);
    printf("DEBUG: g_access_table = %p\n", (void*)g_access_table);
    char* perms = user_ht_get_permission(g_access_table, session->username, payload.filename);
    printf("DEBUG: Got perms = '%s'\n", perms ? perms : "NULL");
    
    if (header->type == MSG_C2N_READ_REQ || header->type == MSG_C2N_STREAM_REQ) {
        printf("DEBUG: Checking READ/STREAM access\n");
        if (perms && strchr(perms, 'r')) {
            printf("DEBUG: Found 'r' in perms\n");
            has_access = true;
        }
    } else if (header->type == MSG_C2N_WRITE_REQ || header->type == MSG_C2N_UNDO_REQ) {
        printf("DEBUG: Checking WRITE/UNDO access\n");
        if (perms && strchr(perms, 'w')) has_access = true;
    } else if (header->type == MSG_C2N_CHECKPOINT_REQ) {
        printf("DEBUG: Checking CHECKPOINT access (needs read permission)\n");
        // Checkpoint only needs read permission (it's like taking a snapshot)
        if (perms && strchr(perms, 'r')) has_access = true;
    }
    // Check ownership inline to avoid deadlock (we already hold the mutex)
    if (perms && strchr(perms, 'o')) {
        printf("DEBUG: Found 'o' in perms, granting access\n");
        has_access = true; // Owner has all access
    }
    pthread_mutex_unlock(&g_access_table_mutex);
    
    printf("DEBUG: has_access=%d\n", has_access);
    if (!has_access) {
        printf("DEBUG: Access denied, sending error\n");
        send_error_response_to_client(session->client_sock, "Access Denied.");
        return;
    }
    
    // 2. Find the actual owner of the file
    printf("DEBUG: Finding owner for file '%s'\n", payload.filename);
    
    // FIX: Prioritize the requesting user's own file
    char* file_owner = NULL;
    FileMapNode* my_file = file_map_table_search(g_file_map_table, session->username, payload.filename);
    
    if (my_file) {
        // I own this file, so use my username
        file_owner = strdup(session->username);
        printf("DEBUG: Found file owned by requester '%s'\n", session->username);
    } else {
        // I don't own it, so search for other owners (e.g. shared files)
        file_owner = file_map_table_find_owner(g_file_map_table, payload.filename);
        printf("DEBUG: Found file owned by '%s' (shared/other)\n", file_owner ? file_owner : "NULL");
    }

    if (!file_owner) {
        printf("DEBUG: File not found in file map\n");
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    // 3. Find Storage Server using the actual file owner
    // For checkpoint operations, use special routing that tries primary then backup
    StorageServer* ss = NULL;
    if (header->type == MSG_C2N_CHECKPOINT_REQ) {
        printf("DEBUG: Finding SS for CHECKPOINT on file '%s' (owner: %s)\n", payload.filename, file_owner);
        ss = find_ss_for_checkpoint(file_owner, payload.filename);
    } else {
        printf("DEBUG: Finding SS for file '%s' (owner: %s)\n", payload.filename, file_owner);
        ss = find_ss_for_file(file_owner, payload.filename);
    }
    free(file_owner); // Free the owner string we got from file_map_table_find_owner
    
    if (!ss) {
        printf("DEBUG: SS not found, sending error\n");
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    printf("DEBUG: Found SS at %s:%d\n", ss->ip, ss->client_port);
    // 3. Send SS location back to client
    Res_SSLocation loc;
    strncpy(loc.ip, ss->ip, 16);
    loc.port = ss->client_port;
    printf("DEBUG: Sending SS location to client\n");
    send_response(session->client_sock, MSG_N2C_SS_LOC, &loc, sizeof(loc));
    printf("DEBUG: SS location sent successfully\n");
}

static void handle_req_access(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);

    pthread_mutex_lock(&g_access_req_mutex);
    AccessRequest* new_req = (AccessRequest*)malloc(sizeof(AccessRequest));
    strncpy(new_req->requester, session->username, MAX_USERNAME - 1);
    new_req->requester[MAX_USERNAME - 1] = '\0';
    strncpy(new_req->filename, payload.filename, MAX_FILENAME - 1);
    new_req->filename[MAX_FILENAME - 1] = '\0';
    new_req->next = g_access_requests_head;
    g_access_requests_head = new_req;
    pthread_mutex_unlock(&g_access_req_mutex);
    
    send_success_response_to_client(session->client_sock, "Access request submitted.");
}

static void handle_view_req_access(UserSession* session, MsgHeader* header) {
    size_t buf_size = 1024;
    char* buffer = (char*)malloc(buf_size);
    buffer[0] = '\0';
    
    pthread_mutex_lock(&g_access_req_mutex);
    AccessRequest* curr = g_access_requests_head;
    while(curr) {
        // Check if current user is owner of the requested file
        if (is_owner(session->username, curr->filename)) {
            char line[MAX_PATH + MAX_USERNAME + 30];
            snprintf(line, sizeof(line), "[%s] requests access to [%s]\n",
                     curr->requester, curr->filename);
            
            if (strlen(buffer) + strlen(line) + 1 > buf_size) {
                buf_size *= 2;
                buffer = (char*)realloc(buffer, buf_size);
            }
            strcat(buffer, line);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&g_access_req_mutex);
    
    Res_View res; // <--- This is now a known type
    strncpy(res.data, buffer, MAX_PAYLOAD);
    res.data[MAX_PAYLOAD - 1] = '\0';
    free(buffer);
    
    send_response(session->client_sock, MSG_N2C_VIEW_REQ_ACCESS_RES, &res, sizeof(res));
}
