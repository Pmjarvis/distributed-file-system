#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>   // For uint64_t
#include <dirent.h>   // For directory handling
#include <sys/stat.h> // For mkdir
#include <errno.h>    // For checking errno

// ----------------------------------------------------------------------------
//  Configuration & Forward Declarations
// ----------------------------------------------------------------------------

#define INITIAL_USER_TABLE_SIZE 17
#define INITIAL_FILE_TABLE_SIZE 11

// The directory to store the persistent data
#define DB_PATH "./permission_db"

// Forward declaration for the inner hash table struct
typedef struct FileHashTable FileHashTable;

// ----------------------------------------------------------------------------
//  Data Structures
// ----------------------------------------------------------------------------

/**
 * @brief Inner table node. Stores a file and its permissions.
 */
typedef struct FileNode {
    char* filename; // The key
    char* perms;    // The value
} FileNode;

/**
 * @brief The inner hash table (maps filename -> perms).
 */
typedef struct FileHashTable {
    FileNode** nodes; // Array of pointers to nodes
    size_t size;      // Size of the array
    size_t count;     // Number of items stored
} FileHashTable;

/**
 * @brief Outer table node. Stores a user and their file table.
 */
typedef struct UserNode {
    char* username;             // The key
    FileHashTable* file_table;  // The value (pointer to inner table)
} UserNode;

/**
 * @brief The outer hash table (maps username -> FileHashTable).
 * This is the main "UserHashTable" object.
 */
typedef struct UserHashTable {
    UserNode** nodes; // Array of pointers to nodes
    size_t size;
    size_t count;
} UserHashTable;

// Global "tombstone" markers for open addressing.
static UserNode g_user_tombstone = {0};
static FileNode g_file_tombstone = {0};


// ----------------------------------------------------------------------------
//  Hash Functions (FNV-1a and djb2)
// ----------------------------------------------------------------------------

#define FNV_PRIME_64 1099511628211ULL
#define FNV_OFFSET_BASIS_64 14695981039346656037ULL

/**
 * @brief Hash function 1: FNV-1a
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
 * @brief Hash function 2: djb2
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
//  Inner Hash Table (File -> Perms)
// ----------------------------------------------------------------------------

/**
 * @brief Creates a new, empty file hash table.
 */
FileHashTable* file_ht_create(size_t size) {
    FileHashTable* table = malloc(sizeof(FileHashTable));
    if (!table) return NULL;

    table->size = size;
    table->count = 0;
    table->nodes = calloc(table->size, sizeof(FileNode*)); 
    if (!table->nodes) {
        free(table);
        return NULL;
    }
    return table;
}

/**
 * @brief Frees the file hash table and all nodes within it.
 */
void file_ht_free(FileHashTable* table) {
    if (!table) return;
    for (size_t i = 0; i < table->size; i++) {
        FileNode* node = table->nodes[i];
        if (node != NULL && node != &g_file_tombstone) {
            free(node->filename); 
            free(node->perms);
            free(node);
        }
    }
    free(table->nodes);
    free(table);
}

/**
 * @brief Core probing logic for the file table.
 */
static size_t file_ht_find_slot(FileHashTable* table, const char* filename, int* p_found) {
    uint64_t hash1 = fnv1a_hash(filename);
    uint64_t hash2 = djb2_hash(filename);
    
    size_t index = hash1 % table->size;
    size_t step = (hash2 % (table->size - 1)) + 1;
    
    size_t first_tombstone = (size_t)-1;
    *p_found = 0;

    for (size_t i = 0; i < table->size; i++) {
        FileNode* node = table->nodes[index];

        if (node == NULL) { 
            return (first_tombstone != (size_t)-1) ? first_tombstone : index;
        } 
        if (node == &g_file_tombstone) { 
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
 * @brief Inserts or updates a file/permission pair in the file table.
 */
int file_ht_insert(FileHashTable* table, const char* filename, const char* perms) {
    if (table->count >= table->size / 2) {
        // A real implementation would resize here.
        fprintf(stderr, "File table is too full. Insertion failed.\n");
        return 0; 
    }

    int found = 0;
    size_t index = file_ht_find_slot(table, filename, &found);

    if (index == (size_t)-1) { return 0; }

    if (found) {
        free(table->nodes[index]->perms);
        table->nodes[index]->perms = strdup(perms);
        if (!table->nodes[index]->perms) return 0;
    } else {
        FileNode* node = malloc(sizeof(FileNode));
        if (!node) return 0;
        
        node->filename = strdup(filename);
        if (!node->filename) { free(node); return 0; }
        
        node->perms = strdup(perms);
        if (!node->perms) { free(node->filename); free(node); return 0; }

        table->nodes[index] = node;
        table->count++;
    }
    return 1;
}

/**
 * @brief Searches for a file's permissions in the file table.
 */
char* file_ht_search(FileHashTable* table, const char* filename) {
    int found = 0;
    size_t index = file_ht_find_slot(table, filename, &found);
    if (found) {
        return table->nodes[index]->perms;
    }
    return NULL;
}

/**
 * @brief Deletes a file from the file table.
 */
int file_ht_delete(FileHashTable* table, const char* filename) {
    int found = 0;
    size_t index = file_ht_find_slot(table, filename, &found);

    if (found) {
        FileNode* node = table->nodes[index];
        free(node->filename);
        free(node->perms);
        free(node);
        table->nodes[index] = &g_file_tombstone; 
        table->count--;
        return 1;
    }
    return 0; // Not found
}

// ----------------------------------------------------------------------------
//  Outer Hash Table (User -> FileTable)
// ----------------------------------------------------------------------------

/**
 * @brief Creates a new, empty user hash table.
 */
UserHashTable* user_ht_create(size_t size) {
    UserHashTable* table = malloc(sizeof(UserHashTable));
    if (!table) return NULL;

    table->size = size;
    table->count = 0;
    table->nodes = calloc(table->size, sizeof(UserNode*));
    if (!table->nodes) {
        free(table);
        return NULL;
    }
    return table;
}

/**
 * @brief Frees the entire user hash table, including all inner tables.
 */
void user_ht_free_system(UserHashTable* table) {
    if (!table) return;
    for (size_t i = 0; i < table->size; i++) {
        UserNode* node = table->nodes[i];
        if (node != NULL && node != &g_user_tombstone) {
            free(node->username);
            file_ht_free(node->file_table); // Recursively free inner table
            free(node);
        }
    }
    free(table->nodes);
    free(table);
}

/**
 * @brief Core probing logic for the user table.
 */
static size_t user_ht_find_slot(UserHashTable* table, const char* username, int* p_found) {
    uint64_t hash1 = fnv1a_hash(username);
    uint64_t hash2 = djb2_hash(username);
    
    size_t index = hash1 % table->size;
    size_t step = (hash2 % (table->size - 1)) + 1;
    
    size_t first_tombstone = (size_t)-1;
    *p_found = 0;

    for (size_t i = 0; i < table->size; i++) {
        UserNode* node = table->nodes[index];

        if (node == NULL) {
            return (first_tombstone != (size_t)-1) ? first_tombstone : index;
        }
        if (node == &g_user_tombstone) {
            if (first_tombstone == (size_t)-1) {
                first_tombstone = index;
            }
        } else if (strcmp(node->username, username) == 0) {
            *p_found = 1;
            return index;
        }
        index = (index + step) % table->size;
    }
    return first_tombstone;
}

/**
 * @brief Inserts a new user. This creates their empty file table.
 */
static int user_ht_insert(UserHashTable* table, const char* username) {
    if (table->count >= table->size / 2) {
        fprintf(stderr, "User table is too full. Insertion failed.\n");
        return 0;
    }

    int found = 0;
    size_t index = user_ht_find_slot(table, username, &found);

    if (found) {
        return 1; // User already exists
    }
    if (index == (size_t)-1) {
        return 0; // Table full
    }

    UserNode* node = malloc(sizeof(UserNode));
    if (!node) return 0;

    node->username = strdup(username);
    if (!node->username) { free(node); return 0; }

    node->file_table = file_ht_create(INITIAL_FILE_TABLE_SIZE);
    if (!node->file_table) {
        free(node->username);
        free(node);
        return 0;
    }

    table->nodes[index] = node;
    table->count++;
    return 1;
}

/**
 * @brief Searches for a user's file table.
 */
static FileHashTable* user_ht_search(UserHashTable* table, const char* username) {
    int found = 0;
    size_t index = user_ht_find_slot(table, username, &found);

    if (found) {
        return table->nodes[index]->file_table;
    }
    return NULL;
}

// ----------------------------------------------------------------------------
//  Public API & Persistence
// ----------------------------------------------------------------------------

/**
 * @brief Adds or updates a permission for a user/file pair.
 */
void user_ht_add_permission(UserHashTable* table, const char* username, const char* filename, const char* perms) {
    FileHashTable* file_table = user_ht_search(table, username);

    if (file_table == NULL) {
        // User doesn't exist, create them first
        if (!user_ht_insert(table, username)) {
            fprintf(stderr, "Failed to add user '%s'\n", username);
            return;
        }
        file_table = user_ht_search(table, username);
        if (file_table == NULL) {
            fprintf(stderr, "Critical error: user table not found after insert.\n");
            return;
        }
    }

    if (!file_ht_insert(file_table, filename, perms)) {
        fprintf(stderr, "Failed to add file '%s' for user '%s'\n", filename, username);
    }
}

/**
 * @brief Gets the permission string for a user/file pair.
 * @return The permission string, or NULL if not found.
 */
char* user_ht_get_permission(UserHashTable* table, const char* username, const char* filename) {
    FileHashTable* file_table = user_ht_search(table, username);
    if (file_table == NULL) {
        return NULL; // User not found
    }
    return file_ht_search(file_table, filename);
}

/**
 * @brief Revokes a permission.
 */
void user_ht_revoke_permission(UserHashTable* table, const char* username, const char* filename) {
    FileHashTable* file_table = user_ht_search(table, username);
    if (file_table != NULL) {
        file_ht_delete(file_table, filename);
    }
}

/**
 * @brief Saves the entire hash table structure to disk.
 */
int user_ht_save(UserHashTable* table, const char* db_path) {
    // Create the directory, ignore error if it already exists
    if (mkdir(db_path, 0755) != 0 && errno != EEXIST) {
        perror("Failed to create database directory");
        return 0;
    }

    for (size_t i = 0; i < table->size; i++) {
        UserNode* user_node = table->nodes[i];
        if (user_node == NULL || user_node == &g_user_tombstone) {
            continue;
        }

        // Construct file path: db_path/username
        char file_path[1024];
        snprintf(file_path, sizeof(file_path), "%s/%s", db_path, user_node->username);

        FILE* fp = fopen(file_path, "w");
        if (!fp) {
            perror("Failed to open user file for writing");
            continue; // Skip this user
        }

        // Iterate this user's inner file table
        FileHashTable* file_table = user_node->file_table;
        for (size_t j = 0; j < file_table->size; j++) {
            FileNode* file_node = file_table->nodes[j];
            if (file_node != NULL && file_node != &g_file_tombstone) {
                // Write "filename|perms\n"
                fprintf(fp, "%s|%s\n", file_node->filename, file_node->perms);
            }
        }
        fclose(fp);
    }
    return 1;
}

/**
 * @brief Loads the hash table from disk, rebuilding the in-memory structure.
 */
UserHashTable* user_ht_load(const char* db_path) {
    UserHashTable* table = user_ht_create(INITIAL_USER_TABLE_SIZE);
    if (!table) return NULL;

    DIR* dir = opendir(db_path);
    if (!dir) {
        if (errno == ENOENT) {
            // Directory doesn't exist, this is a first run.
            return table; // Return new empty table
        }
        perror("Failed to open database directory");
        return table; // Return new empty table
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        // Skip "." and ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char* username = entry->d_name;
        
        // Construct file path: db_path/username
        char file_path[1024];
        snprintf(file_path, sizeof(file_path), "%s/%s", db_path, username);

        FILE* fp = fopen(file_path, "r");
        if (!fp) {
            perror("Failed to open user file for reading");
            continue; // Skip this file
        }

        char* line = NULL;
        size_t len = 0;
        while (getline(&line, &len, fp) != -1) {
            char* pipe = strchr(line, '|');
            if (!pipe) continue; // Malformed line

            *pipe = '\0'; // Split the string at the pipe
            char* filename = line;
            char* perms = pipe + 1;

            // Remove trailing newline from perms
            perms[strcspn(perms, "\n")] = '\0';

            // Add the permission (this also creates the user)
            user_ht_add_permission(table, username, filename, perms);
        }
        
        fclose(fp);
        free(line); // getline allocates, so we must free
    }

    closedir(dir);
    return table;
}

// ----------------------------------------------------------------------------
//  Main (Example Usage)
// ----------------------------------------------------------------------------

int main() {
    printf("Loading permissions from '%s'...\n", DB_PATH);
    UserHashTable* user_table = user_ht_load(DB_PATH);
    if (!user_table) {
        fprintf(stderr, "Failed to load permission system.\n");
        return 1;
    }

    printf("...Load complete.\n\nAdding new/updated permissions...\n");
    user_ht_add_permission(user_table, "alice", "/var/log/syslog", "read");
    user_ht_add_permission(user_table, "alice", "/home/alice/report.pdf", "read-write");
    user_ht_add_permission(user_table, "bob", "/home/alice/report.pdf", "read");
    user_ht_add_permission(user_table, "admin", "/var/log/syslog", "read-write-exec");
    user_ht_add_permission(user_table, "alice", "/tmp/notes.txt", "read");

    printf("\n--- Checking Permissions ---\n");
    
    // Test helper
    #define CHECK(user, file) \
        printf("Perms for [%s] on [%s]: %s\n", \
        user, file, user_ht_get_permission(user_table, user, file) ? \
        user_ht_get_permission(user_table, user, file) : "NULL")

    CHECK("alice", "/home/alice/report.pdf");
    CHECK("bob", "/home/alice/report.pdf");
    CHECK("alice", "/var/log/syslog");
    CHECK("admin", "/var/log/syslog");
    CHECK("bob", "/var/log/syslog");   // Should be NULL
    CHECK("charlie", "report.pdf"); // Should be NULL

    printf("\n--- Revoking Permission ---\n");
    user_ht_revoke_permission(user_table, "alice", "/var/log/syslog");
    CHECK("alice", "/var/log/syslog"); // Should now be NULL

    printf("\n--- Updating Permission ---\n");
    user_ht_add_permission(user_table, "alice", "/home/alice/report.pdf", "NONE");
    CHECK("alice", "/home/alice/report.pdf"); // Should be "NONE"

    printf("\nSaving permissions to '%s'...\n", DB_PATH);
    user_ht_save(user_table, DB_PATH);

    printf("Freeing system...\n");
    user_ht_free_system(user_table);
    printf("Done.\n");

    return 0;
}