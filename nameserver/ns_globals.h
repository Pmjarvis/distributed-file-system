#ifndef NS_GLOBALS_H
#define NS_GLOBALS_H

#include <pthread.h>
#include <time.h>
#include "../common/protocol.h" // <--- MOVED TO TOP

#include "ns_access.h"   // Your access control header
#include "ns_folders.h"  // Your folder hierarchy header
#include "ns_cache.h"    // Cache header
#include "ns_user_manager.h" // User list header
#include "ns_file_map.h" // File mapping hash table


// --- Storage Server Management ---
typedef struct StorageServer {
    int ss_id;
    int sock_fd;
    char ip[16];
    int client_port;
    bool is_online;
    time_t last_heartbeat;
    
    int backup_ss_id;       // ID of the SS it backs up TO
    int backup_of_ss_id;    // ID of the SS it IS a backup FOR
    
    struct StorageServer* next;
} StorageServer;

// List of all connected storage servers
extern StorageServer* g_ss_list_head;
extern int g_ss_count;
extern int g_ss_id_counter;
extern pthread_mutex_t g_ss_list_mutex;


// --- User Management ---
extern UserList* g_user_list;
extern pthread_mutex_t g_user_list_mutex;


// --- Access Control ---
extern UserHashTable* g_access_table;
extern pthread_mutex_t g_access_table_mutex;


// --- Caching ---
extern LRUCache* g_file_cache;
extern pthread_mutex_t g_cache_mutex;

// --- File Mapping Hash Table (now has internal locking) ---
extern FileMapHashTable* g_file_map_table;


// --- Per-Client Session ---
typedef struct {
    int client_sock;
    char username[MAX_USERNAME];
    Node* folder_hierarchy_root; // This user's personal folder tree
    Node* current_directory;     // This user's CWD in their tree
} UserSession;

// --- Access Request List ---
typedef struct AccessRequest {
    char requester[MAX_USERNAME];
    char filename[MAX_FILENAME];
    struct AccessRequest* next;
} AccessRequest;

extern AccessRequest* g_access_requests_head;
extern pthread_mutex_t g_access_req_mutex;


#endif