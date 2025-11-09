#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>   // For uint64_t
#include <dirent.h>   // For directory handling
#include <sys/stat.h> // For mkdir
#include <errno.h>    // For checking errno

#include "ns_access.h" // Include the header

// ----------------------------------------------------------------------------
//  Configuration & Forward Declarations
// ----------------------------------------------------------------------------

#define INITIAL_TABLE_SIZE 197  // Prime number for better hash distribution
#define INITIAL_USER_TABLE_SIZE 101
#define INITIAL_FILE_TABLE_SIZE 11
//#define DB_PATH "./permission_db"

// ----------------------------------------------------------------------------
//  Data Structures
// ----------------------------------------------------------------------------

struct FileNode {
    char* filename;
    char* perms;
};

struct FileHashTable {
    FileNode** nodes;
    size_t size;
    size_t count;
};

struct UserNode {
    char* username;
    FileHashTable* file_table;
};

struct UserHashTable {
    UserNode** nodes;
    size_t size;
    size_t count;
};

static UserNode g_user_tombstone = {0};
static FileNode g_file_tombstone = {0};

// ----------------------------------------------------------------------------
//  Hash Functions (FNV-1a and djb2)
// ----------------------------------------------------------------------------

#define FNV_PRIME_64 1099511628211ULL
#define FNV_OFFSET_BASIS_64 14695981039346656037ULL

uint64_t fnv1a_hash(const char *str) {
    uint64_t hash = FNV_OFFSET_BASIS_64;
    unsigned char *s = (unsigned char *)str;
    while (*s) {
        hash ^= (uint64_t)(*s++);
        hash *= FNV_PRIME_64;
    }
    return hash;
}

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

char* file_ht_search(FileHashTable* table, const char* filename) {
    int found = 0;
    size_t index = file_ht_find_slot(table, filename, &found);
    if (found) {
        return table->nodes[index]->perms;
    }
    return NULL;
}

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

char* user_ht_get_permission(UserHashTable* table, const char* username, const char* filename) {
    FileHashTable* file_table = user_ht_search(table, username);
    if (file_table == NULL) {
        return NULL; // User not found
    }
    return file_ht_search(file_table, filename);
}

void user_ht_revoke_permission(UserHashTable* table, const char* username, const char* filename) {
    FileHashTable* file_table = user_ht_search(table, username);
    if (file_table != NULL) {
        file_ht_delete(file_table, filename);
    }
}

void user_ht_revoke_file_from_all(UserHashTable* table, const char* filename) {
    // Iterate through all users in the hash table and remove the file from each
    for (size_t i = 0; i < table->size; i++) {
        UserNode* user_node = table->nodes[i];
        if (user_node != NULL && user_node != &g_user_tombstone) {
            FileHashTable* file_table = user_node->file_table;
            if (file_table != NULL) {
                file_ht_delete(file_table, filename);
            }
        }
    }
}

int user_ht_save(UserHashTable* table, const char* db_path) {
    if (mkdir(db_path, 0755) != 0 && errno != EEXIST) {
        perror("Failed to create database directory");
        return 0;
    }

    for (size_t i = 0; i < table->size; i++) {
        UserNode* user_node = table->nodes[i];
        if (user_node == NULL || user_node == &g_user_tombstone) {
            continue;
        }

        char file_path[1024];
        snprintf(file_path, sizeof(file_path), "%s/%s", db_path, user_node->username);

        FILE* fp = fopen(file_path, "w");
        if (!fp) {
            perror("Failed to open user file for writing");
            continue;
        }

        FileHashTable* file_table = user_node->file_table;
        for (size_t j = 0; j < file_table->size; j++) {
            FileNode* file_node = file_table->nodes[j];
            if (file_node != NULL && file_node != &g_file_tombstone) {
                fprintf(fp, "%s|%s\n", file_node->filename, file_node->perms);
            }
        }
        fclose(fp);
    }
    return 1;
}

UserHashTable* user_ht_load(const char* db_path) {
    UserHashTable* table = user_ht_create(INITIAL_USER_TABLE_SIZE);
    if (!table) return NULL;

    DIR* dir = opendir(db_path);
    if (!dir) {
        if (errno == ENOENT) {
            return table; // Return new empty table
        }
        perror("Failed to open database directory");
        return table; // Return new empty table
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char* username = entry->d_name;
        
        char file_path[1024];
        snprintf(file_path, sizeof(file_path), "%s/%s", db_path, username);

        FILE* fp = fopen(file_path, "r");
        if (!fp) {
            perror("Failed to open user file for reading");
            continue;
        }

        char* line = NULL;
        size_t len = 0;
        while (getline(&line, &len, fp) != -1) {
            char* pipe = strchr(line, '|');
            if (!pipe) continue;

            *pipe = '\0';
            char* filename = line;
            char* perms = pipe + 1;

            perms[strcspn(perms, "\n")] = '\0';
            user_ht_add_permission(table, username, filename, perms);
        }
        
        fclose(fp);
        free(line); 
    }

    closedir(dir);
    return table;
}