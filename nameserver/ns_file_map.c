#include "ns_file_map.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

// A global tombstone marker for deleted slots
static FileMapNode g_file_map_tombstone = {0};

// ----------------------------------------------------------------------------
// Hash Functions (FNV-1a and djb2)
// ----------------------------------------------------------------------------

#define FNV_PRIME_64 1099511628211ULL
#define FNV_OFFSET_BASIS_64 14695981039346656037ULL

/**
 * @brief Hash function 1 (Primary): FNV-1a
 */
static uint64_t fnv1a_hash(const char *str) {
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
static uint64_t djb2_hash(const char *str) {
    uint64_t hash = 5381;
    int c;
    unsigned char *s = (unsigned char *)str;
    while ((c = *s++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash;
}

/**
 * @brief Get the bucket lock index for a filename
 * Uses primary hash to determine which lock protects this filename
 */
static size_t get_lock_index(FileMapHashTable* table, const char* filename) {
    uint64_t hash = fnv1a_hash(filename);
    return hash % table->num_locks;
}

// ----------------------------------------------------------------------------
// Core Hash Table Functions
// ----------------------------------------------------------------------------

/**
 * @brief Core double-hashing probe logic
 */
static size_t file_map_find_slot(FileMapHashTable* table, const char* filename, int* p_found) {
    uint64_t hash1 = fnv1a_hash(filename);
    uint64_t hash2 = djb2_hash(filename);
    
    size_t index = hash1 % table->size;
    size_t step = (hash2 % (table->size - 1)) + 1;
    
    size_t first_tombstone = (size_t)-1;
    *p_found = 0;

    for (size_t i = 0; i < table->size; i++) {
        FileMapNode* node = table->nodes[index];

        if (node == NULL) {
            // Empty slot found
            return (first_tombstone != (size_t)-1) ? first_tombstone : index;
        }
        
        if (node == &g_file_map_tombstone) {
            // Tombstone - remember first one
            if (first_tombstone == (size_t)-1) {
                first_tombstone = index;
            }
        } else if (strcmp(node->filename, filename) == 0) {
            // Found the key
            *p_found = 1;
            return index;
        }
        
        index = (index + step) % table->size;
    }
    
    // Table is full
    return first_tombstone;
}

FileMapHashTable* file_map_table_create(size_t size) {
    FileMapHashTable* table = malloc(sizeof(FileMapHashTable));
    if (!table) return NULL;

    table->size = size;
    table->count = 0;
    table->num_locks = FILE_MAP_NUM_LOCKS;
    
    table->nodes = calloc(table->size, sizeof(FileMapNode*)); 
    if (!table->nodes) {
        free(table);
        return NULL;
    }
    
    // Allocate bucket locks
    table->bucket_locks = malloc(table->num_locks * sizeof(pthread_mutex_t));
    if (!table->bucket_locks) {
        free(table->nodes);
        free(table);
        return NULL;
    }
    
    // Initialize bucket locks
    for (size_t i = 0; i < table->num_locks; i++) {
        if (pthread_mutex_init(&table->bucket_locks[i], NULL) != 0) {
            // Cleanup already initialized locks
            for (size_t j = 0; j < i; j++) {
                pthread_mutex_destroy(&table->bucket_locks[j]);
            }
            free(table->bucket_locks);
            free(table->nodes);
            free(table);
            return NULL;
        }
    }
    
    // Initialize count lock
    if (pthread_mutex_init(&table->count_lock, NULL) != 0) {
        for (size_t i = 0; i < table->num_locks; i++) {
            pthread_mutex_destroy(&table->bucket_locks[i]);
        }
        free(table->bucket_locks);
        free(table->nodes);
        free(table);
        return NULL;
    }
    
    return table;
}

void file_map_table_free(FileMapHashTable* table) {
    if (!table) return;
    
    // Free all nodes
    for (size_t i = 0; i < table->size; i++) {
        FileMapNode* node = table->nodes[i];
        if (node != NULL && node != &g_file_map_tombstone) {
            free(node);
        }
    }
    
    free(table->nodes);
    
    // Destroy all bucket locks
    for (size_t i = 0; i < table->num_locks; i++) {
        pthread_mutex_destroy(&table->bucket_locks[i]);
    }
    free(table->bucket_locks);
    
    // Destroy count lock
    pthread_mutex_destroy(&table->count_lock);
    
    free(table);
}

int file_map_table_insert(FileMapHashTable* table, const char* filename,
                          int primary_ss_id, int backup_ss_id,
                          const char* owner) {
    if (!table || !filename) return 0;
    
    // Get the bucket lock for this filename
    size_t lock_idx = get_lock_index(table, filename);
    pthread_mutex_lock(&table->bucket_locks[lock_idx]);
    
    if (table->count >= table->size / 2) {
        fprintf(stderr, "File map table is too full. Insertion failed.\n");
        pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
        return 0;
    }

    int found = 0;
    size_t index = file_map_find_slot(table, filename, &found);

    if (index == (size_t)-1) {
        pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
        return 0;
    }

    if (found) {
        // Update existing entry
        table->nodes[index]->primary_ss_id = primary_ss_id;
        table->nodes[index]->backup_ss_id = backup_ss_id;
        if (owner) {
            strncpy(table->nodes[index]->owner, owner, MAX_USERNAME - 1);
            table->nodes[index]->owner[MAX_USERNAME - 1] = '\0';
        }
        pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
    } else {
        // Insert new entry
        FileMapNode* node = malloc(sizeof(FileMapNode));
        if (!node) {
            pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
            return 0;
        }
        
        strncpy(node->filename, filename, MAX_FILENAME - 1);
        node->filename[MAX_FILENAME - 1] = '\0';
        node->primary_ss_id = primary_ss_id;
        node->backup_ss_id = backup_ss_id;
        
        if (owner) {
            strncpy(node->owner, owner, MAX_USERNAME - 1);
            node->owner[MAX_USERNAME - 1] = '\0';
        } else {
            node->owner[0] = '\0';
        }

        table->nodes[index] = node;
        
        // Update global count (needs separate lock)
        pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
        pthread_mutex_lock(&table->count_lock);
        table->count++;
        pthread_mutex_unlock(&table->count_lock);
    }
    
    return 1;
}

FileMapNode* file_map_table_search(FileMapHashTable* table, const char* filename) {
    if (!table || !filename) return NULL;
    
    // Get the bucket lock for this filename
    size_t lock_idx = get_lock_index(table, filename);
    pthread_mutex_lock(&table->bucket_locks[lock_idx]);
    
    int found = 0;
    size_t index = file_map_find_slot(table, filename, &found);

    FileMapNode* result = NULL;
    if (found) {
        result = table->nodes[index];
    }
    
    pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
    return result;
}

int file_map_table_delete(FileMapHashTable* table, const char* filename) {
    if (!table || !filename) return 0;
    
    // Get the bucket lock for this filename
    size_t lock_idx = get_lock_index(table, filename);
    pthread_mutex_lock(&table->bucket_locks[lock_idx]);
    
    int found = 0;
    size_t index = file_map_find_slot(table, filename, &found);

    if (found) {
        FileMapNode* node = table->nodes[index];
        free(node);
        table->nodes[index] = &g_file_map_tombstone;
        
        // Update global count
        pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
        pthread_mutex_lock(&table->count_lock);
        table->count--;
        pthread_mutex_unlock(&table->count_lock);
        return 1;
    }
    
    pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
    return 0;
}

int file_map_table_save(FileMapHashTable* table, const char* filepath) {
    if (!table || !filepath) return 0;
    
    // Lock all buckets for full table iteration
    for (size_t i = 0; i < table->num_locks; i++) {
        pthread_mutex_lock(&table->bucket_locks[i]);
    }
    
    FILE* fp = fopen(filepath, "wb");
    if (!fp) {
        fprintf(stderr, "Failed to open %s for writing: %s\n", filepath, strerror(errno));
        for (size_t i = 0; i < table->num_locks; i++) {
            pthread_mutex_unlock(&table->bucket_locks[i]);
        }
        return 0;
    }
    
    // Write count
    uint32_t count = (uint32_t)table->count;
    if (fwrite(&count, sizeof(uint32_t), 1, fp) != 1) {
        fprintf(stderr, "Failed to write count to %s\n", filepath);
        fclose(fp);
        for (size_t i = 0; i < table->num_locks; i++) {
            pthread_mutex_unlock(&table->bucket_locks[i]);
        }
        return 0;
    }
    
    // Write each entry
    for (size_t i = 0; i < table->size; i++) {
        FileMapNode* node = table->nodes[i];
        if (node != NULL && node != &g_file_map_tombstone) {
            // Write filename length
            uint32_t name_len = strlen(node->filename) + 1; // Include null terminator
            if (fwrite(&name_len, sizeof(uint32_t), 1, fp) != 1) {
                fprintf(stderr, "Failed to write filename length\n");
                fclose(fp);
                for (size_t i = 0; i < table->num_locks; i++) {
                    pthread_mutex_unlock(&table->bucket_locks[i]);
                }
                return 0;
            }
            
            // Write filename
            if (fwrite(node->filename, 1, name_len, fp) != name_len) {
                fprintf(stderr, "Failed to write filename\n");
                fclose(fp);
                for (size_t i = 0; i < table->num_locks; i++) {
                    pthread_mutex_unlock(&table->bucket_locks[i]);
                }
                return 0;
            }
            
            // Write SS IDs
            if (fwrite(&node->primary_ss_id, sizeof(int), 1, fp) != 1 ||
                fwrite(&node->backup_ss_id, sizeof(int), 1, fp) != 1) {
                fprintf(stderr, "Failed to write SS IDs\n");
                fclose(fp);
                for (size_t i = 0; i < table->num_locks; i++) {
                    pthread_mutex_unlock(&table->bucket_locks[i]);
                }
                return 0;
            }
            
            // Write owner length
            uint32_t owner_len = strlen(node->owner) + 1;
            if (fwrite(&owner_len, sizeof(uint32_t), 1, fp) != 1) {
                fprintf(stderr, "Failed to write owner length\n");
                fclose(fp);
                for (size_t i = 0; i < table->num_locks; i++) {
                    pthread_mutex_unlock(&table->bucket_locks[i]);
                }
                return 0;
            }
            
            // Write owner
            if (fwrite(node->owner, 1, owner_len, fp) != owner_len) {
                fprintf(stderr, "Failed to write owner\n");
                fclose(fp);
                for (size_t i = 0; i < table->num_locks; i++) {
                    pthread_mutex_unlock(&table->bucket_locks[i]);
                }
                return 0;
            }
        }
    }
    
    fclose(fp);
    
    // Unlock all buckets
    for (size_t i = 0; i < table->num_locks; i++) {
        pthread_mutex_unlock(&table->bucket_locks[i]);
    }
    
    return 1;
}

FileMapHashTable* file_map_table_load(const char* filepath, size_t size) {
    if (!filepath) return NULL;
    
    FILE* fp = fopen(filepath, "rb");
    if (!fp) {
        fprintf(stderr, "No existing file map at %s (starting fresh)\n", filepath);
        return file_map_table_create(size);
    }
    
    // Read expected count
    uint32_t expected_count;
    if (fread(&expected_count, sizeof(uint32_t), 1, fp) != 1) {
        fprintf(stderr, "Failed to read file count\n");
        fclose(fp);
        return file_map_table_create(size);
    }
    
    // Create new table
    FileMapHashTable* table = file_map_table_create(size);
    if (!table) {
        fclose(fp);
        return NULL;
    }
    
    // Read entries
    for (uint32_t i = 0; i < expected_count; i++) {
        // Read filename length
        uint32_t name_len;
        if (fread(&name_len, sizeof(uint32_t), 1, fp) != 1) break;
        
        if (name_len == 0 || name_len > MAX_FILENAME) break;
        
        // Read filename
        char filename[MAX_FILENAME];
        if (fread(filename, 1, name_len, fp) != name_len) break;
        filename[MAX_FILENAME - 1] = '\0';
        
        // Read SS IDs
        int primary_ss_id, backup_ss_id;
        if (fread(&primary_ss_id, sizeof(int), 1, fp) != 1 ||
            fread(&backup_ss_id, sizeof(int), 1, fp) != 1) break;
        
        // Read owner length
        uint32_t owner_len;
        if (fread(&owner_len, sizeof(uint32_t), 1, fp) != 1) break;
        
        if (owner_len == 0 || owner_len > MAX_USERNAME) break;
        
        // Read owner
        char owner[MAX_USERNAME];
        if (fread(owner, 1, owner_len, fp) != owner_len) break;
        owner[MAX_USERNAME - 1] = '\0';
        
        // Insert into table
        file_map_table_insert(table, filename, primary_ss_id, backup_ss_id, owner);
    }
    
    fclose(fp);
    return table;
}

void file_map_table_iterate(FileMapHashTable* table, FileMapIteratorCallback callback, void* user_data) {
    if (!table || !callback) return;
    
    // Lock all buckets for full table iteration
    for (size_t i = 0; i < table->num_locks; i++) {
        pthread_mutex_lock(&table->bucket_locks[i]);
    }
    
    for (size_t i = 0; i < table->size; i++) {
        FileMapNode* node = table->nodes[i];
        if (node != NULL && node != &g_file_map_tombstone) {
            callback(node, user_data);
        }
    }
    
    // Unlock all buckets
    for (size_t i = 0; i < table->num_locks; i++) {
        pthread_mutex_unlock(&table->bucket_locks[i]);
    }
}

int file_map_table_update_primary(FileMapHashTable* table, const char* filename, int new_primary_ss_id) {
    if (!table || !filename) return 0;
    
    // Get the bucket lock for this filename
    size_t lock_idx = get_lock_index(table, filename);
    pthread_mutex_lock(&table->bucket_locks[lock_idx]);
    
    int found = 0;
    size_t index = file_map_find_slot(table, filename, &found);

    if (found) {
        table->nodes[index]->primary_ss_id = new_primary_ss_id;
        pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
        return 1;
    }
    
    pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
    return 0;
}

int file_map_table_update_backup(FileMapHashTable* table, const char* filename, int new_backup_ss_id) {
    if (!table || !filename) return 0;
    
    // Get the bucket lock for this filename
    size_t lock_idx = get_lock_index(table, filename);
    pthread_mutex_lock(&table->bucket_locks[lock_idx]);
    
    int found = 0;
    size_t index = file_map_find_slot(table, filename, &found);

    if (found) {
        table->nodes[index]->backup_ss_id = new_backup_ss_id;
        pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
        return 1;
    }
    
    pthread_mutex_unlock(&table->bucket_locks[lock_idx]);
    return 0;
}
