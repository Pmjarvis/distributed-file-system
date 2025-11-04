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

// Finds the *online* SS (primary or backup) responsible for a file
StorageServer* find_ss_for_file(const char* filename);

// Finds the primary SS where a new file should be created
StorageServer* get_ss_for_new_file(const char* filename);

// Adds a file to an SS's internal list (after creation)
void add_file_to_ss_list(StorageServer* ss, FileMetadata* meta);

// Removes a file from an SS's internal list (after deletion)
void remove_file_from_ss_list(StorageServer* ss, const char* filename);

// Finds a file in a single SS's list (helper function)
SSFileNode* find_file_in_ss_list(StorageServer* ss, const char* filename); // <--- ADDED THIS

// Formats file lists for VIEW command
char* format_file_list(UserHashTable* access_table, const char* username, const char* flags);

#endif // NS_SS_MANAGER_H