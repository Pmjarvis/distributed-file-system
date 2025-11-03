#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>   // For uint64_t
#include <dirent.h>   // For directory handling
#include <sys/stat.h> // For mkdir
#include <errno.h>    // For checking errno

// ----------------------------------------------------------------------------
//  Configuration
// ----------------------------------------------------------------------------

// Initial prime size for each server's file hash table.
#define INITIAL_FILE_TABLE_SIZE 17

// The directory to store the persistent data
#define DB_PATH "./storage_db"

// ----------------------------------------------------------------------------
//  Data Structures
// ----------------------------------------------------------------------------

/**
 * @brief Node for a single file on a server.
 * The filename is the key, metadata is the value.
 */
typedef struct StorageNode {
    char* filename; // The key
    char* metadata; // The value (e.g., "size:12KB", "last_mod:...")
} StorageNode;

/**
 * @brief The hash table for a single storage server.
 * Maps filename -> metadata.
 */
typedef struct StorageHashTable {
    StorageNode** nodes; // Array of pointers to nodes
    size_t size;         // Size of the array
    size_t count;        // Number of items stored
} StorageHashTable;

/**
 * @brief The main system, representing an array of storage servers.
 */
typedef struct StorageSystem {
    StorageHashTable** servers; // The array of hash tables
    size_t num_servers;         // The number of servers in the array
} StorageSystem;

// A global "tombstone" marker for open addressing (to mark deleted slots).
static StorageNode g_storage_tombstone = {0};

// ----------------------------------------------------------------------------
//  Hash Functions (FNV-1a and djb2)
// ----------------------------------------------------------------------------

#define FNV_PRIME_64 1099511628211ULL
#define FNV_OFFSET_BASIS_64 14695981039346656037ULL

/**
 * @brief Hash function 1 (Primary): FNV-1a
 */
uint64_t fnv1a_hash(const char *str) {
    uint64_t hash = FNV_OFFSET_BASIS_64;
    unsigned char *s = (unsigned char *)str;
    while (*s) {
        hash ^= (uint64_t)(*s++);
        hash *= FNV_PRIME_64;
    }
    return hash;
}

/**
 * @brief Hash function 2 (Secondary for Double Hashing): djb2
 */
uint64_t djb2_hash(const char *str) {
    uint64_t hash = 5381;
    int c;
    unsigned char *s = (unsigned char *)str;
    while ((c = *s++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash;
}

// ----------------------------------------------------------------------------
//  Storage Hash Table (Per-Server) Functions
// ----------------------------------------------------------------------------

/**
 * @brief Creates a new, empty file hash table for a server.
 */
StorageHashTable* storage_ht_create(size_t size) {
    StorageHashTable* table = malloc(sizeof(StorageHashTable));
    if (!table) return NULL;

    table->size = size;
    table->count = 0;
    table->nodes = calloc(table->size, sizeof(StorageNode*)); 
    if (!table->nodes) {
        free(table);
        return NULL;
    }
    return table;
}

/**
 * @brief Frees a server's hash table and all file nodes within it.
 */
void storage_ht_free(StorageHashTable* table) {
    if (!table) return;
    for (size_t i = 0; i < table->size; i++) {
        StorageNode* node = table->nodes[i];
        if (node != NULL && node != &g_storage_tombstone) {
            free(node->filename); 
            free(node->metadata);
            free(node);
        }
    }
    free(table->nodes);
    free(table);
}

/**
 * @brief Core double-hashing probe logic for the file table.
 */
static size_t storage_ht_find_slot(StorageHashTable* table, const char* filename, int* p_found) {
    uint64_t hash1 = fnv1a_hash(filename);
    uint64_t hash2 = djb2_hash(filename);
    
    size_t index = hash1 % table->size;
    size_t step = (hash2 % (table->size - 1)) + 1;
    
    size_t first_tombstone = (size_t)-1;
    *p_found = 0;

    for (size_t i = 0; i < table->size; i++) {
        StorageNode* node = table->nodes[index];

        if (node == NULL) { 
            return (first_tombstone != (size_t)-1) ? first_tombstone : index;
        } 
        if (node == &g_storage_tombstone) { 
            if (first_tombstone == (size_t)-1) {
                first_tombstone = index;
            }
        } else if (strcmp(node->filename, filename) == 0) { 
            *p_found = 1;
            return index;
        }
        index = (index + step) % table->size;
    }
    return first_tombstone;
}

/**
 * @brief Inserts or updates a file/metadata pair in a server's table.
 */
int storage_ht_insert(StorageHashTable* table, const char* filename, const char* metadata) {
    if (table->count >= table->size / 2) {
        fprintf(stderr, "Storage table is too full. Insertion failed.\n");
        return 0; 
    }

    int found = 0;
    size_t index = storage_ht_find_slot(table, filename, &found);

    if (index == (size_t)-1) { return 0; }

    if (found) {
        free(table->nodes[index]->metadata);
        table->nodes[index]->metadata = strdup(metadata);
        if (!table->nodes[index]->metadata) return 0;
    } else {
        StorageNode* node = malloc(sizeof(StorageNode));
        if (!node) return 0;
        
        node->filename = strdup(filename);
        if (!node->filename) { free(node); return 0; }
        
        node->metadata = strdup(metadata);
        if (!node->metadata) { free(node->filename); free(node); return 0; }

        table->nodes[index] = node;
        table->count++;
    }
    return 1;
}

/**
 * @brief Searches for a file's metadata in a server's table.
 */
char* storage_ht_search(StorageHashTable* table, const char* filename) {
    int found = 0;
    size_t index = storage_ht_find_slot(table, filename, &found);

    if (found) {
        return table->nodes[index]->metadata;
    }
    return NULL;
}

/**
 * @brief Deletes a file from a server's table.
 */
int storage_ht_delete(StorageHashTable* table, const char* filename) {
    int found = 0;
    size_t index = storage_ht_find_slot(table, filename, &found);

    if (found) {
        StorageNode* node = table->nodes[index];
        free(node->filename);
        free(node->metadata);
        free(node);
        table->nodes[index] = &g_storage_tombstone; 
        table->count--;
        return 1;
    }
    return 0; // Not found
}


// ----------------------------------------------------------------------------
//  Storage System (Array of Servers) Functions
// ----------------------------------------------------------------------------

/**
 * @brief Creates the entire storage system (array of hash tables).
 * NOTE: This is now an internal function, call storage_system_load() instead.
 */
StorageSystem* storage_system_create(size_t num_servers) {
    if (num_servers == 0) return NULL;

    StorageSystem* system = malloc(sizeof(StorageSystem));
    if (!system) return NULL;
    
    system->num_servers = num_servers;
    system->servers = malloc(num_servers * sizeof(StorageHashTable*));
    if (!system->servers) {
        free(system);
        return NULL;
    }

    for (size_t i = 0; i < num_servers; i++) {
        system->servers[i] = storage_ht_create(INITIAL_FILE_TABLE_SIZE);
        if (system->servers[i] == NULL) {
            for (size_t j = 0; j < i; j++) {
                storage_ht_free(system->servers[j]);
            }
            free(system->servers);
            free(system);
            return NULL;
        }
    }
    return system;
}

/**
 * @brief Frees the entire storage system, including all server tables.
 */
void storage_system_free(StorageSystem* system) {
    if (!system) return;
    
    for (size_t i = 0; i < system->num_servers; i++) {
        storage_ht_free(system->servers[i]);
    }
    free(system->servers);
    free(system);
}

/**
 * @brief Inserts/updates a file on a specific server.
 */
int storage_system_insert_file(StorageSystem* system, size_t server_index, const char* filename, const char* metadata) {
    if (!system || server_index >= system->num_servers) {
        return 0; // Invalid arguments
    }
    
    StorageHashTable* table = system->servers[server_index];
    return storage_ht_insert(table, filename, metadata);
}

/**
 * @brief Searches for a file on a specific server.
 */
char* storage_system_search_file(StorageSystem* system, size_t server_index, const char* filename) {
    if (!system || server_index >= system->num_servers) {
        return NULL; // Invalid arguments
    }

    StorageHashTable* table = system->servers[server_index];
    return storage_ht_search(table, filename);
}

/**
 * @brief Deletes a file from a specific server.
 */
int storage_system_delete_file(StorageSystem* system, size_t server_index, const char* filename) {
    if (!system || server_index >= system->num_servers) {
        return 0; // Invalid arguments
    }

    StorageHashTable* table = system->servers[server_index];
    return storage_ht_delete(table, filename);
}

// ----------------------------------------------------------------------------
//  Persistence Functions (NEW)
// ----------------------------------------------------------------------------

/**
 * @brief Saves the entire storage system to disk.
 * Creates the directory if it doesn't exist.
 */
int storage_system_save(StorageSystem* system, const char* db_path) {
    // Create the directory, ignore error if it already exists
    if (mkdir(db_path, 0755) != 0 && errno != EEXIST) {
        perror("Failed to create database directory");
        return 0;
    }

    // Loop through each server in the array
    for (size_t i = 0; i < system->num_servers; i++) {
        // Construct file path: db_path/server_i.db
        char file_path[1024];
        snprintf(file_path, sizeof(file_path), "%s/server_%zu.db", db_path, i);

        FILE* fp = fopen(file_path, "w");
        if (!fp) {
            perror("Failed to open server file for writing");
            continue; // Skip this server, try the next one
        }

        // Iterate this server's inner file table
        StorageHashTable* table = system->servers[i];
        for (size_t j = 0; j < table->size; j++) {
            StorageNode* node = table->nodes[j];
            if (node != NULL && node != &g_storage_tombstone) {
                // Write "filename|metadata\n"
                fprintf(fp, "%s|%s\n", node->filename, node->metadata);
            }
        }
        fclose(fp);
    }
    return 1;
}

/**
 * @brief Loads the storage system from disk, rebuilding the in-memory structure.
 * @param db_path The directory to load from.
 * @param num_servers The *expected* number of servers (must match the array setup).
 */
StorageSystem* storage_system_load(const char* db_path, size_t num_servers) {
    // 1. Create a new, empty system structure
    StorageSystem* system = storage_system_create(num_servers);
    if (!system) {
        fprintf(stderr, "Failed to create empty system during load.\n");
        return NULL;
    }

    // 2. Check if the database directory exists
    DIR* dir = opendir(db_path);
    if (!dir) {
        if (errno == ENOENT) {
            // Directory doesn't exist, this is a first run.
            // Return the new empty system.
            return system; 
        }
        perror("Failed to open database directory for loading");
        return system; // Return empty system on other errors
    }
    closedir(dir); // We only needed to check if it exists

    // 3. Loop through each expected server and try to load its file
    for (size_t i = 0; i < num_servers; i++) {
        // Construct file path: db_path/server_i.db
        char file_path[1024];
        snprintf(file_path, sizeof(file_path), "%s/server_%zu.db", db_path, i);

        FILE* fp = fopen(file_path, "r");
        if (!fp) {
            // This is not an error. It just means this server had no data.
            continue; 
        }

        char* line = NULL;
        size_t len = 0;
        // Read each line (e.g., "filename|metadata")
        while (getline(&line, &len, fp) != -1) {
            char* pipe = strchr(line, '|');
            if (!pipe) continue; // Malformed line

            *pipe = '\0'; // Split the string at the pipe
            char* filename = line;
            char* metadata = pipe + 1;

            // Remove trailing newline from metadata
            metadata[strcspn(metadata, "\n")] = '\0';

            // Add the data to the correct server's hash table
            storage_ht_insert(system->servers[i], filename, metadata);
        }
        
        fclose(fp);
        free(line); // getline allocates, so we must free
    }

    return system;
}


// ----------------------------------------------------------------------------
//  Main (Example Usage)
// ----------------------------------------------------------------------------

int main() {
    const size_t NUM_SERVERS = 3;
    printf("Loading storage system from '%s'...\n", DB_PATH);
    
    // Call load instead of create
    StorageSystem* system = storage_system_load(DB_PATH, NUM_SERVERS);
    if (!system) {
        fprintf(stderr, "Failed to load storage system.\n");
        return 1;
    }

    printf("...Load complete.\n");
    printf("\n--- Inserting/Updating Files ---\n");
    storage_system_insert_file(system, 0, "/var/log/syslog", "Server 0, 1.2MB");
    storage_system_insert_file(system, 0, "/etc/passwd", "Server 0, 3KB");
    storage_system_insert_file(system, 1, "/home/user/video.mp4", "Server 1, 850MB");
    storage_system_insert_file(system, 2, "/var/log/syslog", "Server 2, 900KB (Backup)");

    // Test helper
    #define CHECK(server_idx, file) \
        printf("  File [%s] on Server [%d]: %s\n", \
        file, server_idx, storage_system_search_file(system, server_idx, file) ? \
        storage_system_search_file(system, server_idx, file) : "NULL")

    printf("\n--- Checking Files ---\n");
    CHECK(0, "/var/log/syslog");
    CHECK(2, "/var/log/syslog"); // Same file, different server
    CHECK(1, "/home/user/video.mp4");
    CHECK(0, "/home/user/video.mp4"); // Should be NULL
    CHECK(1, "/etc/passwd"); // Should be NULL

    printf("\n--- Deleting File ---\n");
    printf("Deleting [/etc/passwd] from Server [0]...\n");
    storage_system_delete_file(system, 0, "/etc/passwd");
    CHECK(0, "/etc/passwd"); // Should now be NULL

    printf("\n--- Updating File ---\n");
    printf("Updating [/var/log/syslog] on Server [0]...\n");
    storage_system_insert_file(system, 0, "/var/log/syslog", "Server 0, 1.3MB (Updated)");
    CHECK(0, "/var/log/syslog"); // Should be updated
    CHECK(2, "/var/log/syslog"); // Should be unchanged

    // Save the final state back to disk
    printf("\nSaving storage system to '%s'...\n", DB_PATH);
    storage_system_save(system, DB_PATH);

    printf("\nFreeing storage system...\n");
    storage_system_free(system);
    printf("Done.\n");

    return 0;
}