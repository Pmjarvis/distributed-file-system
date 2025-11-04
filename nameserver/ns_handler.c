#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
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
static void handle_grant_req_access(UserSession* session, MsgHeader* header);

// For READ/WRITE/STREAM/UNDO/CHECKPOINT
static void handle_ss_redirect(UserSession* session, MsgHeader* header);

// Helper to check ownership
static bool is_owner(const char* username, const char* filename) {
    pthread_mutex_lock(&g_access_table_mutex);
    char* perms = user_ht_get_permission(g_access_table, username, filename);
    bool owner = (perms && strstr(perms, "owner"));
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
            case MSG_C2N_GRANT_REQ_ACCESS:
                handle_grant_req_access(session, &header);
                break;

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
    
    StorageServer* ss = get_ss_for_new_file(payload.filename);
    if (!ss) {
        send_error_response_to_client(session->client_sock, "No available Storage Servers to create file.");
        return;
    }
    
    // 1. Tell SS to create the (empty) file
    strncpy(payload.username, session->username, MAX_USERNAME);
    int res = ss_request_response(ss, MSG_N2S_CREATE_FILE, &payload, sizeof(payload));
    
    if (res != 0) {
        send_error_response_to_client(session->client_sock, "Storage Server failed to create file.");
        return;
    }

    // 2. Update Access Control (set owner)
    pthread_mutex_lock(&g_access_table_mutex);
    user_ht_add_permission(g_access_table, session->username, payload.filename, "read-write-owner");
    // --- FIX ---
    user_ht_save(g_access_table, DB_PATH);
    pthread_mutex_unlock(&g_access_table_mutex);
    
    // 3. Update NS's internal list of files
    FileMetadata meta;
    strncpy(meta.filename, payload.filename, MAX_PATH);
    strncpy(meta.owner, session->username, MAX_USERNAME);
    meta.size_bytes = 0;
    meta.word_count = 0;
    meta.char_count = 0;
    meta.last_access_time = time(NULL);
    
    pthread_mutex_lock(&g_ss_list_mutex);
    add_file_to_ss_list(ss, &meta);
    pthread_mutex_unlock(&g_ss_list_mutex);

    // 4. Add to user's folder hierarchy
    createTreeFile(session->current_directory, payload.filename);
    
    // --- FIX ---
    send_success_response_to_client(session->client_sock, "File created successfully.");
}

static void handle_delete(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);
    
    // 1. Check permissions (must be owner)
    if (!is_owner(session->username, payload.filename)) {
        send_error_response_to_client(session->client_sock, "Access Denied: Only the owner can delete a file.");
        return;
    }
    
    StorageServer* ss = find_ss_for_file(payload.filename);
    if (!ss) {
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    // 2. Tell SS to delete file
    int res = ss_request_response(ss, MSG_N2S_DELETE_FILE, &payload, sizeof(payload));
    if (res != 0) {
        send_error_response_to_client(session->client_sock, "Storage Server failed to delete file.");
        return;
    }
    
    // 3. Remove from NS list
    pthread_mutex_lock(&g_ss_list_mutex);
    remove_file_from_ss_list(ss, payload.filename);
    pthread_mutex_unlock(&g_ss_list_mutex);
    
    // 4. Remove from cache
    pthread_mutex_lock(&g_cache_mutex);
    lru_cache_remove(g_file_cache, payload.filename);
    pthread_mutex_unlock(&g_cache_mutex);
    
    // 5. Remove from Access Control
    // TODO: Need to iterate all users and remove permissions
    pthread_mutex_lock(&g_access_table_mutex);
    user_ht_revoke_permission(g_access_table, session->username, payload.filename);
    // --- FIX ---
    user_ht_save(g_access_table, DB_PATH);
    pthread_mutex_unlock(&g_access_table_mutex);
    
    // 6. Remove from folder hierarchy (if it exists)
    Node* file_node = findChild(session->current_directory, payload.filename, NODE_FILE);
    if(file_node) {
        removeChild(session->current_directory, file_node);
        freeTree(file_node);
    }
    
    send_success_response_to_client(session->client_sock, "File deleted successfully.");
}

static void handle_info(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);

    // 1. Check permissions (must have WRITE access for INFO)
    pthread_mutex_lock(&g_access_table_mutex);
    char* perms = user_ht_get_permission(g_access_table, session->username, payload.filename);
    bool has_access = (perms && strstr(perms, "write"));
    pthread_mutex_unlock(&g_access_table_mutex);
    
    if (!has_access && !is_owner(session->username, payload.filename)) {
        send_error_response_to_client(session->client_sock, "Access Denied: Write access required for INFO.");
        return;
    }

    // 2. Find file
    StorageServer* ss = find_ss_for_file(payload.filename);
    if (!ss) {
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    // 3. Get info from SS
    // --- FIX: Removed unused variable ---
    // int ss_sock = socket(AF_INET, SOCK_STREAM, 0); 
    
    pthread_mutex_lock(&g_ss_list_mutex);
    // --- FIX: This function is now visible ---
    SSFileNode* file_node = find_file_in_ss_list(ss, payload.filename);
    if (!file_node) {
        pthread_mutex_unlock(&g_ss_list_mutex);
        send_error_response_to_client(session->client_sock, "File metadata not found on NS.");
        return;
    }
    
    Res_Info res;
    char time_str[64];
    struct tm local_tm;
    localtime_r(&file_node->meta.last_access_time, &local_tm);
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M", &local_tm);
    
    snprintf(res.data, MAX_PAYLOAD,
             "File: %s\n"
             "Owner: %s\n"
             "Size: %llu bytes\n"
             "Words: %u\n"
             "Chars: %u\n"
             "Last Access: %s\n",
             file_node->meta.filename,
             file_node->meta.owner,
             (unsigned long long)file_node->meta.size_bytes,
             file_node->meta.word_count,
             file_node->meta.char_count,
             time_str);
             
    pthread_mutex_unlock(&g_ss_list_mutex);
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
    bool has_access = (perms && strstr(perms, "read"));
    pthread_mutex_unlock(&g_access_table_mutex);

    if (!has_access && !is_owner(session->username, payload.filename)) {
        send_error_response_to_client(session->client_sock, "Access Denied: Read access required to execute.");
        return;
    }

    // 2. Find SS
    StorageServer* ss = find_ss_for_file(payload.filename);
    if (!ss) {
        send_error_response_to_client(session->client_sock, "File not found or SS is offline.");
        return;
    }
    
    // 3. Get content from SS
    // --- FIX: Removed unused variable ---
    // int ss_sock = socket(AF_INET, SOCK_STREAM, 0);
    
    // Placeholder for content
    Res_Exec exec_content;
    snprintf(exec_content.output, MAX_PAYLOAD, "echo 'Hello from %s' \n ls -l", payload.filename);

    // 4. Execute content
    char buffer[1024];
    Res_Exec response;
    memset(response.output, 0, MAX_PAYLOAD);
    
    FILE* pipe = popen(exec_content.output, "r");
    if (!pipe) {
        send_error_response_to_client(session->client_sock, "Failed to execute command on server.");
        return;
    }
    
    while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
        if (strlen(response.output) + strlen(buffer) < MAX_PAYLOAD) {
            strcat(response.output, buffer);
        }
    }
    pclose(pipe);
    
    // 4. Send output to client
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
        view_result = viewTreeFolder(session->folder_hierarchy_root, session->current_directory, payload.arg1);
        if (!view_result) err_msg = "Error generating folder view.";
    } else if (strcmp(payload.command, "MOVE") == 0) {
        err_msg = moveTreeFile(session->current_directory, payload.arg1, payload.arg2);
    } else if (strcmp(payload.command, "UPMOVE") == 0) {
        err_msg = upMoveTreeFile(session->current_directory, payload.arg1);
    } else if (strcmp(payload.command, "OPEN") == 0) {
        bool create = (strstr(payload.flags, "c") != NULL);
        new_dir = openTreeFolder(session->current_directory, payload.arg1, create);
        if (new_dir) session->current_directory = new_dir;
        else err_msg = "Folder not found and -c flag not specified.";
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
    Req_FileOp payload;
    if (header->type == MSG_C2N_CHECKPOINT_REQ) {
        Req_Checkpoint chk_payload;
        recv_payload(session->client_sock, &chk_payload, header->payload_len);
        strncpy(payload.filename, chk_payload.filename, MAX_PATH); // Copy filename for access check
    } else {
        recv_payload(session->client_sock, &payload, header->payload_len);
    }
    
    // 1. Check Access Control
    bool has_access = false;
    pthread_mutex_lock(&g_access_table_mutex);
    char* perms = user_ht_get_permission(g_access_table, session->username, payload.filename);
    
    if (header->type == MSG_C2N_READ_REQ || header->type == MSG_C2N_STREAM_REQ) {
        if (perms && strstr(perms, "read")) has_access = true;
    } else if (header->type == MSG_C2N_WRITE_REQ || header->type == MSG_C2N_UNDO_REQ || header->type == MSG_C2N_CHECKPOINT_REQ) {
        if (perms && strstr(perms, "write")) has_access = true;
    }
    if (is_owner(session->username, payload.filename)) {
        has_access = true; // Owner has all access
    }
    pthread_mutex_unlock(&g_access_table_mutex);
    
    if (!has_access) {
        send_error_response_to_client(session->client_sock, "Access Denied.");
        return;
    }
    
    // 2. Find Storage Server
    StorageServer* ss = find_ss_for_file(payload.filename);
    if (!ss) {
        send_error_response_to_client(session->client_sock, "File not found or Storage Server is offline.");
        return;
    }
    
    // 3. Send SS location back to client
    Res_SSLocation loc;
    strncpy(loc.ip, ss->ip, 16);
    loc.port = ss->client_port;
    send_response(session->client_sock, MSG_N2C_SS_LOC, &loc, sizeof(loc));
}

static void handle_req_access(UserSession* session, MsgHeader* header) {
    Req_FileOp payload;
    recv_payload(session->client_sock, &payload, header->payload_len);

    pthread_mutex_lock(&g_access_req_mutex);
    AccessRequest* new_req = (AccessRequest*)malloc(sizeof(AccessRequest));
    strncpy(new_req->requester, session->username, MAX_USERNAME);
    strncpy(new_req->filename, payload.filename, MAX_PATH);
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

static void handle_grant_req_access(UserSession* session, MsgHeader* header) {
    Req_Access payload; // Uses same payload as ADDACCESS
    recv_payload(session->client_sock, &payload, header->payload_len);

    // 1. Check if user is owner
    if (!is_owner(session->username, payload.filename)) {
        send_error_response_to_client(session->client_sock, "Access Denied: Only owner can grant access.");
        return;
    }

    // 2. Grant permission
    pthread_mutex_lock(&g_access_table_mutex);
    const char* perm_str = (payload.perm_flag == 'W') ? "read-write" : "read";
    user_ht_add_permission(g_access_table, payload.target_user, payload.filename, perm_str);
    // --- FIX ---
    user_ht_save(g_access_table, DB_PATH);
    pthread_mutex_unlock(&g_access_table_mutex);
    
    // 3. Remove request from list
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
    
    send_success_response_to_client(session->client_sock, "Access granted.");
}