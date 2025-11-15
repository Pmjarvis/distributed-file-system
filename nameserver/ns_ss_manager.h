#ifndef NS_SS_MANAGER_H
#define NS_SS_MANAGER_H

#include "ns_globals.h"
#include <arpa/inet.h>

// Arg for the SS handler thread
typedef struct {
    int sock_fd;
    struct sockaddr_in ss_addr;
} SSThreadArg;

// Thread function to handle a single SS connection (registration, heartbeats)
void* ss_handler_thread(void* arg);

// Called by monitor thread to mark offline SSs
void check_ss_heartbeats();

// Finds SS by ID (must hold g_ss_list_mutex)
StorageServer* get_ss_by_id(int id);

// Finds the *online* SS (primary or backup) responsible for a file
StorageServer* find_ss_for_file(const char* owner, const char* filename);

// Finds the primary SS where a new file should be created
StorageServer* get_ss_for_new_file(const char* filename);

// Requests fresh metadata from SS and updates hash table
int refresh_file_metadata_from_ss(const char* owner, const char* filename);

// Gets metadata from SS without caching (returns 0 on success, -1 on failure)
int get_file_metadata_from_ss(const char* owner, const char* filename, FileMetadata* out_meta);

// Formats file lists for VIEW command
char* format_file_list(UserHashTable* access_table, const char* username, const char* flags);

#endif // NS_SS_MANAGER_H