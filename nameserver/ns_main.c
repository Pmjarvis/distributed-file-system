#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "../common/protocol.h"
#include "../common/net_utils.h"
#include "ns_globals.h"
#include "ns_handler.h"
#include "ns_ss_manager.h"
#include "ns_user_manager.h"

// --- Global Variable Definitions ---
StorageServer* g_ss_list_head = NULL;
int g_ss_count = 0;
int g_ss_id_counter = 0;
pthread_mutex_t g_ss_list_mutex = PTHREAD_MUTEX_INITIALIZER;

UserList* g_user_list = NULL;
pthread_mutex_t g_user_list_mutex = PTHREAD_MUTEX_INITIALIZER;

UserHashTable* g_access_table = NULL;
pthread_mutex_t g_access_table_mutex = PTHREAD_MUTEX_INITIALIZER;

LRUCache* g_file_cache = NULL;
pthread_mutex_t g_cache_mutex = PTHREAD_MUTEX_INITIALIZER;

AccessRequest* g_access_requests_head = NULL;
pthread_mutex_t g_access_req_mutex = PTHREAD_MUTEX_INITIALIZER;
// ---

void* client_listener_thread(void* arg);
void* ss_listener_thread(void* arg);
void* ss_monitor_thread(void* arg);

void init_server_state() {
    printf("Initializing Name Server...\n");

    // Load persistent data
    pthread_mutex_lock(&g_user_list_mutex);
    g_user_list = load_user_list();
    printf("Loaded %d users.\n", g_user_list->count);
    pthread_mutex_unlock(&g_user_list_mutex);

    pthread_mutex_lock(&g_access_table_mutex);
    g_access_table = user_ht_load(DB_PATH);
    printf("Loaded access control table.\n");
    pthread_mutex_unlock(&g_access_table_mutex);
    
    // Init non-persistent data
    pthread_mutex_lock(&g_cache_mutex);
    g_file_cache = lru_cache_create(128);
    printf("Cache initialized.\n");
    pthread_mutex_unlock(&g_cache_mutex);
    
    g_ss_list_head = NULL;
    g_ss_count = 0;
    g_ss_id_counter = 0;
    
    g_access_requests_head = NULL;
}

void cleanup_server_state() {
    printf("Shutting down...\n");
    
    pthread_mutex_lock(&g_access_table_mutex);
    user_ht_save(g_access_table, DB_PATH);
    user_ht_free_system(g_access_table);
    pthread_mutex_unlock(&g_access_table_mutex);
    
    pthread_mutex_lock(&g_user_list_mutex);
    save_user_list(g_user_list);
    free_user_list(g_user_list);
    pthread_mutex_unlock(&g_user_list_mutex);
    
    pthread_mutex_lock(&g_cache_mutex);
    lru_cache_free(g_file_cache, NULL);
    pthread_mutex_unlock(&g_cache_mutex);
    
    // TODO: Free g_ss_list_head, g_access_requests_head
}

int main() {
    init_server_state();

    pthread_t client_tid, ss_tid, monitor_tid;
    
    if (pthread_create(&client_tid, NULL, client_listener_thread, NULL) != 0) {
        perror("Failed to create client listener thread");
        exit(EXIT_FAILURE);
    }
    
    if (pthread_create(&ss_tid, NULL, ss_listener_thread, NULL) != 0) {
        perror("Failed to create SS listener thread");
        exit(EXIT_FAILURE);
    }

    if (pthread_create(&monitor_tid, NULL, ss_monitor_thread, NULL) != 0) {
        perror("Failed to create SS monitor thread");
        exit(EXIT_FAILURE);
    }
    
    printf("Name Server is running on ports %d (Client) and %d (SS).\n", NS_PORT, NS_SS_PORT);
    
    pthread_join(client_tid, NULL);
    pthread_join(ss_tid, NULL);
    pthread_join(monitor_tid, NULL);

    cleanup_server_state();
    return 0;
}


void* client_listener_thread(void* arg) {
    int server_fd = setup_listener_socket(NS_PORT);
    
    while(1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_sock = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
        
        if (client_sock < 0) {
            perror("Accept failed");
            continue;
        }
        
        UserSession* session = (UserSession*)malloc(sizeof(UserSession));
        session->client_sock = client_sock;
        memset(session->username, 0, MAX_USERNAME);
        session->folder_hierarchy_root = NULL;
        session->current_directory = NULL;
        
        pthread_t client_thread_id;
        if (pthread_create(&client_thread_id, NULL, handle_client_request, (void*)session) != 0) {
            perror("Failed to create client thread");
            close(client_sock);
            free(session);
        }
        pthread_detach(client_thread_id);
    }
    close(server_fd);
    return NULL;
}

void* ss_listener_thread(void* arg) {
    int server_fd = setup_listener_socket(NS_SS_PORT);
    
    while(1) {
        SSThreadArg* ss_arg = (SSThreadArg*)malloc(sizeof(SSThreadArg));
        socklen_t addr_len = sizeof(ss_arg->ss_addr);
        
        ss_arg->sock_fd = accept(server_fd, (struct sockaddr*)&ss_arg->ss_addr, &addr_len);
        
        if (ss_arg->sock_fd < 0) {
            perror("SS Accept failed");
            free(ss_arg);
            continue;
        }
        
        pthread_t ss_thread_id;
        if (pthread_create(&ss_thread_id, NULL, ss_handler_thread, (void*)ss_arg) != 0) {
            perror("Failed to create SS handler thread");
            close(ss_arg->sock_fd);
            free(ss_arg);
        }
        pthread_detach(ss_thread_id);
    }
    close(server_fd);
    return NULL;
}

void* ss_monitor_thread(void* arg) {
    while(1) {
        sleep(HEARTBEAT_TIMEOUT); // Check at the same interval as the timeout
        check_ss_heartbeats();
    }
    return NULL;
}