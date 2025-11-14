#include "ss_metadata.h"
#include "ss_logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * THREAD-SAFE NESTED HASH TABLE IMPLEMENTATION
 * 
 * This file implements a fully thread-safe nested hash table for file metadata.
 * 
 * ARCHITECTURE:
 * - Two-level hashing with different hash functions (djb2 + FNV-1a)
 * - Outer table: 1024 buckets, each can contain an inner table
 * - Inner table: 64 buckets, each with a linked list of nodes
 * - Total potential buckets: 1024 × 64 = 65,536
 * 
 * THREAD SAFETY MECHANISMS:
 * 1. Per-Inner-Table Locking: Each inner table has its own mutex
 *    - Reduces contention: operations on different inner tables don't block
 *    - Protects all operations within that inner table
 * 
 * 2. Global Count Lock: Separate mutex for total file count
 *    - Prevents races when incrementing/decrementing global count
 *    - Independent from inner table locks
 * 
 * 3. Copy-on-Return: metadata_table_get() returns allocated copies
 *    - Prevents use-after-free when node is removed by another thread
 *    - Caller owns the memory and must free() it
 * 
 * 4. Single-Lock Pattern: Update operations lock once, find & modify, unlock once
 *    - No gaps where pointers are unlocked
 *    - Atomic operation from caller's perspective
 * 
 * 5. Atomic Lazy Initialization: Inner tables created with check-and-install
 *    - Uses count_lock to protect outer bucket array
 *    - Prevents race where multiple threads create same inner table
 *    - Discards duplicate attempts, keeping first one
 * 
 * CONCURRENCY SCENARIOS:
 * 
 * Scenario 1: Concurrent inserts to different outer buckets
 *   Thread A: insert("file1.txt") → hash to outer[5]
 *   Thread B: insert("file2.txt") → hash to outer[10]
 *   Result: Both proceed concurrently, different inner table locks
 * 
 * Scenario 2: Concurrent read while another thread removes
 *   Thread A: get("file1.txt") → returns COPY, then unlocks
 *   Thread B: remove("file1.txt") → locks, removes, frees original
 *   Result: Thread A has its own copy, safe to use
 * 
 * Scenario 3: Concurrent updates to same file
 *   Thread A: update_size("file1.txt", 1024)
 *   Thread B: update_counts("file1.txt", 100, 500)
 *   Result: Serialized by inner table lock, both succeed atomically
 * 
 * PERFORMANCE:
 * - Average case: O(1) for all operations
 * - Worst case: O(n/65536) where n = total files (very short chains)
 * - Lock contention: Low due to 1024+ independent locks
 * 
 * MEMORY SAFETY:
 * - All returned pointers from get() must be freed by caller
 * - Inner tables freed recursively in metadata_table_free()
 * - No memory leaks in lazy initialization (discarded tables are freed)
 */

// ============ HASH FUNCTIONS ============

// Primary hash function for outer table
uint32_t metadata_hash_primary(const char* filename) {
    // djb2 hash algorithm
    uint32_t hash = 5381;
    int c;
    
    while ((c = *filename++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    
    return hash;
}

// Secondary hash function for inner table
uint32_t metadata_hash_secondary(const char* filename) {
    // FNV-1a hash algorithm (different from primary)
    uint32_t hash = 2166136261u;
    int c;
    
    while ((c = *filename++)) {
        hash ^= (unsigned char)c;
        hash *= 16777619u;
    }
    
    return hash;
}

// Public hash function (uses primary)
uint32_t metadata_hash(const char* filename) {
    return metadata_hash_primary(filename);
}

// ============ INNER HASH TABLE OPERATIONS ============

InnerHashTable* inner_table_init(uint32_t size) {
    InnerHashTable* table = (InnerHashTable*)malloc(sizeof(InnerHashTable));
    if (!table) return NULL;
    
    table->size = size;
    table->count = 0;
    table->buckets = (FileMetadataNode**)calloc(size, sizeof(FileMetadataNode*));
    
    if (!table->buckets) {
        free(table);
        return NULL;
    }
    
    pthread_mutex_init(&table->lock, NULL);
    return table;
}

void inner_table_free(InnerHashTable* table) {
    if (!table) return;
    
    for (uint32_t i = 0; i < table->size; i++) {
        FileMetadataNode* curr = table->buckets[i];
        while (curr) {
            FileMetadataNode* next = curr->next;
            free(curr);
            curr = next;
        }
    }
    
    pthread_mutex_destroy(&table->lock);
    free(table->buckets);
    free(table);
}

int inner_table_insert(InnerHashTable* table, const char* filename,
                      const char* owner, uint64_t file_size,
                      uint64_t word_count, uint64_t char_count,
                      time_t last_access, time_t last_modified) {
    if (!table || !filename) return -1;
    
    pthread_mutex_lock(&table->lock);
    
    uint32_t hash = metadata_hash_secondary(filename);
    uint32_t index = hash % table->size;
    
    // Check if entry already exists (update case)
    FileMetadataNode* curr = table->buckets[index];
    while (curr) {
        if (strcmp(curr->filename, filename) == 0) {
            // Update existing entry
            if (owner) {
                strncpy(curr->owner, owner, MAX_USERNAME - 1);
                curr->owner[MAX_USERNAME - 1] = '\0';
            }
            curr->file_size = file_size;
            curr->word_count = word_count;
            curr->char_count = char_count;
            curr->last_access = last_access;
            curr->last_modified = last_modified;
            
            pthread_mutex_unlock(&table->lock);
            return 1; // Updated
        }
        curr = curr->next;
    }
    
    // Create new node
    FileMetadataNode* new_node = (FileMetadataNode*)malloc(sizeof(FileMetadataNode));
    if (!new_node) {
        pthread_mutex_unlock(&table->lock);
        return -1;
    }
    
    strncpy(new_node->filename, filename, MAX_FILENAME - 1);
    new_node->filename[MAX_FILENAME - 1] = '\0';
    
    if (owner) {
        strncpy(new_node->owner, owner, MAX_USERNAME - 1);
        new_node->owner[MAX_USERNAME - 1] = '\0';
    } else {
        strcpy(new_node->owner, "unknown");
    }
    
    new_node->file_size = file_size;
    new_node->word_count = word_count;
    new_node->char_count = char_count;
    new_node->last_access = last_access;
    new_node->last_modified = last_modified;
    
    // Insert at head of inner bucket
    new_node->next = table->buckets[index];
    table->buckets[index] = new_node;
    table->count++;
    
    pthread_mutex_unlock(&table->lock);
    return 0; // Inserted
}

FileMetadataNode* inner_table_get(InnerHashTable* table, const char* filename) {
    if (!table || !filename) return NULL;
    
    pthread_mutex_lock(&table->lock);
    
    uint32_t hash = metadata_hash_secondary(filename);
    uint32_t index = hash % table->size;
    
    FileMetadataNode* curr = table->buckets[index];
    FileMetadataNode* result = NULL;
    
    while (curr) {
        if (strcmp(curr->filename, filename) == 0) {
            // Allocate and return a COPY (thread-safe)
            result = (FileMetadataNode*)malloc(sizeof(FileMetadataNode));
            if (result) {
                memcpy(result, curr, sizeof(FileMetadataNode));
                result->next = NULL;  // Don't expose internal structure
            }
            break;
        }
        curr = curr->next;
    }
    
    pthread_mutex_unlock(&table->lock);
    return result;  // Caller must free() this
}

int inner_table_remove(InnerHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    pthread_mutex_lock(&table->lock);
    
    uint32_t hash = metadata_hash_secondary(filename);
    uint32_t index = hash % table->size;
    
    FileMetadataNode* curr = table->buckets[index];
    FileMetadataNode* prev = NULL;
    
    while (curr) {
        if (strcmp(curr->filename, filename) == 0) {
            // Found node to remove
            if (prev) {
                prev->next = curr->next;
            } else {
                table->buckets[index] = curr->next;
            }
            
            free(curr);
            table->count--;
            pthread_mutex_unlock(&table->lock);
            return 0;
        }
        prev = curr;
        curr = curr->next;
    }
    
    pthread_mutex_unlock(&table->lock);
    return -1; // Not found
}

// ============ OUTER HASH TABLE OPERATIONS ============

MetadataHashTable* metadata_table_init(uint32_t outer_size) {
    MetadataHashTable* table = (MetadataHashTable*)malloc(sizeof(MetadataHashTable));
    if (!table) {
        ss_log("ERROR: Failed to allocate metadata hash table");
        return NULL;
    }
    
    table->size = outer_size;
    table->count = 0;
    table->buckets = (InnerHashTable**)calloc(outer_size, sizeof(InnerHashTable*));
    
    if (!table->buckets) {
        ss_log("ERROR: Failed to allocate outer buckets");
        free(table);
        return NULL;
    }
    
    // Initialize count lock
    pthread_mutex_init(&table->count_lock, NULL);
    
    // Note: Inner hash tables are created lazily on first insert
    ss_log("Nested metadata hash table initialized: %u outer buckets × %u inner buckets",
           outer_size, INNER_TABLE_SIZE);
    return table;
}

void metadata_table_free(MetadataHashTable* table) {
    if (!table) return;
    
    for (uint32_t i = 0; i < table->size; i++) {
        if (table->buckets[i]) {
            inner_table_free(table->buckets[i]);
        }
    }
    
    pthread_mutex_destroy(&table->count_lock);
    free(table->buckets);
    free(table);
    ss_log("Nested metadata hash table freed");
}

int metadata_table_insert(MetadataHashTable* table, const char* filename,
                         const char* owner, uint64_t file_size,
                         uint64_t word_count, uint64_t char_count,
                         time_t last_access, time_t last_modified) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // Atomic lazy initialization of inner table (thread-safe)
    if (!table->buckets[outer_index]) {
        // Create inner table outside any lock
        InnerHashTable* new_inner = inner_table_init(INNER_TABLE_SIZE);
        if (!new_inner) {
            ss_log("ERROR: Failed to create inner table for bucket %u", outer_index);
            return -1;
        }
        
        // Use count_lock to protect outer bucket array access
        pthread_mutex_lock(&table->count_lock);
        
        if (!table->buckets[outer_index]) {
            // We're the first, install our table
            table->buckets[outer_index] = new_inner;
            pthread_mutex_unlock(&table->count_lock);
        } else {
            // Someone else created it first, discard ours
            pthread_mutex_unlock(&table->count_lock);
            inner_table_free(new_inner);
        }
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
    int result = inner_table_insert(inner, filename, owner, file_size,
                                   word_count, char_count, last_access, last_modified);
    
    if (result == 0) {
        // New insertion (not update)
        pthread_mutex_lock(&table->count_lock);
        table->count++;
        pthread_mutex_unlock(&table->count_lock);
        
        ss_log("Inserted metadata for: %s (owner: %s, size: %lu, words: %lu, chars: %lu)",
               filename, owner ? owner : "unknown", file_size, word_count, char_count);
    } else if (result == 1) {
        ss_log("Updated metadata for: %s", filename);
    }
    
    return result;
}

FileMetadataNode* metadata_table_get(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return NULL;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    if (!table->buckets[outer_index]) {
        return NULL; // Inner table doesn't exist, file not found
    }
    
    return inner_table_get(table->buckets[outer_index], filename);
}

int metadata_table_exists(MetadataHashTable* table, const char* filename) {
    FileMetadataNode* node = metadata_table_get(table, filename);
    if (node) {
        free(node);  // Free the copy returned by get()
        return 1;
    }
    return 0;
}

// ============ UPDATE SPECIFIC FIELDS ============

int metadata_table_update_size(MetadataHashTable* table, const char* filename, uint64_t new_size) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    if (!table->buckets[outer_index]) {
        return -1; // Inner table doesn't exist
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
    
    pthread_mutex_lock(&inner->lock);  // LOCK ONCE at start
    
    uint32_t inner_hash = metadata_hash_secondary(filename);
    uint32_t inner_index = inner_hash % inner->size;
    
    FileMetadataNode* curr = inner->buckets[inner_index];
    int found = 0;
    
    while (curr) {
        if (strcmp(curr->filename, filename) == 0) {
            curr->file_size = new_size;  // Update while locked
            found = 1;
            ss_log("Updated size for %s: %lu bytes", filename, new_size);
            break;
        }
        curr = curr->next;
    }
    
    pthread_mutex_unlock(&inner->lock);  // UNLOCK ONCE at end
    
    return found ? 0 : -1;
}

int metadata_table_update_counts(MetadataHashTable* table, const char* filename,
                                 uint64_t word_count, uint64_t char_count) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    if (!table->buckets[outer_index]) {
        return -1;
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
    
    pthread_mutex_lock(&inner->lock);
    
    uint32_t inner_hash = metadata_hash_secondary(filename);
    uint32_t inner_index = inner_hash % inner->size;
    
    FileMetadataNode* curr = inner->buckets[inner_index];
    int found = 0;
    
    while (curr) {
        if (strcmp(curr->filename, filename) == 0) {
            curr->word_count = word_count;
            curr->char_count = char_count;
            found = 1;
            ss_log("Updated counts for %s: %lu words, %lu chars", filename, word_count, char_count);
            break;
        }
        curr = curr->next;
    }
    
    pthread_mutex_unlock(&inner->lock);
    
    return found ? 0 : -1;
}

int metadata_table_update_access_time(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    if (!table->buckets[outer_index]) {
        return -1;
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
    
    pthread_mutex_lock(&inner->lock);
    
    uint32_t inner_hash = metadata_hash_secondary(filename);
    uint32_t inner_index = inner_hash % inner->size;
    
    FileMetadataNode* curr = inner->buckets[inner_index];
    int found = 0;
    
    while (curr) {
        if (strcmp(curr->filename, filename) == 0) {
            curr->last_access = time(NULL);
            found = 1;
            break;
        }
        curr = curr->next;
    }
    
    pthread_mutex_unlock(&inner->lock);
    
    return found ? 0 : -1;
}

int metadata_table_update_modified_time(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    if (!table->buckets[outer_index]) {
        return -1;
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
    
    pthread_mutex_lock(&inner->lock);
    
    uint32_t inner_hash = metadata_hash_secondary(filename);
    uint32_t inner_index = inner_hash % inner->size;
    
    FileMetadataNode* curr = inner->buckets[inner_index];
    int found = 0;
    
    while (curr) {
        if (strcmp(curr->filename, filename) == 0) {
            curr->last_modified = time(NULL);
            found = 1;
            break;
        }
        curr = curr->next;
    }
    
    pthread_mutex_unlock(&inner->lock);
    
    return found ? 0 : -1;
}

// ============ REMOVE ============

int metadata_table_remove(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    if (!table->buckets[outer_index]) {
        return -1; // Inner table doesn't exist, file not found
    }
    
    int result = inner_table_remove(table->buckets[outer_index], filename);
    
    if (result == 0) {
        pthread_mutex_lock(&table->count_lock);
        table->count--;
        pthread_mutex_unlock(&table->count_lock);
        
        ss_log("Removed metadata for: %s", filename);
    } else {
        ss_log("WARNING: Metadata not found for removal: %s", filename);
    }
    
    return result;
}

// ============ PERSISTENCE ============

int metadata_table_save(MetadataHashTable* table, const char* filepath) {
    if (!table || !filepath) return -1;
    
    FILE* fp = fopen(filepath, "wb");
    if (!fp) {
        perror("Failed to open metadata file for writing");
        return -1;
    }
    
    // Write table count
    pthread_mutex_lock(&table->count_lock);
    uint32_t count = table->count;
    pthread_mutex_unlock(&table->count_lock);
    
    fwrite(&count, sizeof(uint32_t), 1, fp);
    
    // Iterate through outer table
    for (uint32_t i = 0; i < table->size; i++) {
        InnerHashTable* inner = table->buckets[i];
        if (!inner) continue;
        
        pthread_mutex_lock(&inner->lock);
        
        // Iterate through inner table
        for (uint32_t j = 0; j < inner->size; j++) {
            FileMetadataNode* node = inner->buckets[j];
            
            // Iterate through linked list
            while (node) {
                // Write filename length
                uint32_t name_len = strlen(node->filename) + 1;
                fwrite(&name_len, sizeof(uint32_t), 1, fp);
                
                // Write filename
                fwrite(node->filename, 1, name_len, fp);
                
                // Write owner length and owner
                uint32_t owner_len = strlen(node->owner) + 1;
                fwrite(&owner_len, sizeof(uint32_t), 1, fp);
                fwrite(node->owner, 1, owner_len, fp);
                
                // Write metadata fields
                fwrite(&node->file_size, sizeof(uint64_t), 1, fp);
                fwrite(&node->word_count, sizeof(uint64_t), 1, fp);
                fwrite(&node->char_count, sizeof(uint64_t), 1, fp);
                fwrite(&node->last_modified, sizeof(time_t), 1, fp);
                fwrite(&node->last_access, sizeof(time_t), 1, fp);
                
                node = node->next;
            }
        }
        
        pthread_mutex_unlock(&inner->lock);
    }
    
    fclose(fp);
    ss_log("Saved %u metadata entries to %s", count, filepath);
    return 0;
}

MetadataHashTable* metadata_table_load(const char* filepath) {
    if (!filepath) return NULL;
    
    FILE* fp = fopen(filepath, "rb");
    if (!fp) {
        ss_log("No existing metadata file found at %s (starting fresh)", filepath);
        return NULL; // Not an error - first run
    }
    
    // Read expected count
    uint32_t expected_count;
    if (fread(&expected_count, sizeof(uint32_t), 1, fp) != 1) {
        ss_log("WARNING: Failed to read metadata count");
        fclose(fp);
        return NULL;
    }
    
    // Create new table
    MetadataHashTable* table = metadata_table_init(OUTER_TABLE_SIZE);
    if (!table) {
        fclose(fp);
        return NULL;
    }
    
    uint32_t loaded = 0;
    
    // Read entries
    for (uint32_t i = 0; i < expected_count; i++) {
        // Read filename length
        uint32_t name_len;
        if (fread(&name_len, sizeof(uint32_t), 1, fp) != 1) break;
        
        // Allocate and read filename
        char* filename = malloc(name_len);
        if (!filename) break;
        
        if (fread(filename, 1, name_len, fp) != name_len) {
            free(filename);
            break;
        }
        
        // Read owner length
        uint32_t owner_len;
        if (fread(&owner_len, sizeof(uint32_t), 1, fp) != 1) {
            free(filename);
            break;
        }
        
        // Allocate and read owner
        char* owner = malloc(owner_len);
        if (!owner) {
            free(filename);
            break;
        }
        
        if (fread(owner, 1, owner_len, fp) != owner_len) {
            free(filename);
            free(owner);
            break;
        }
        
        // Read metadata fields
        uint64_t file_size, word_count, char_count;
        time_t last_modified, last_access;
        
        if (fread(&file_size, sizeof(uint64_t), 1, fp) != 1 ||
            fread(&word_count, sizeof(uint64_t), 1, fp) != 1 ||
            fread(&char_count, sizeof(uint64_t), 1, fp) != 1 ||
            fread(&last_modified, sizeof(time_t), 1, fp) != 1 ||
            fread(&last_access, sizeof(time_t), 1, fp) != 1) {
            free(filename);
            free(owner);
            break;
        }
        
        // Insert using public API (handles locking)
        if (metadata_table_insert(table, filename, owner, file_size, word_count, char_count, 
                                 last_access, last_modified) == 0) {
            loaded++;
        }
        
        free(filename);
        free(owner);
    }
    
    fclose(fp);
    ss_log("Loaded %u metadata entries from %s (expected %u)", loaded, filepath, expected_count);
    
    if (loaded != expected_count) {
        ss_log("WARNING: Loaded count mismatch");
    }
    
    return table;
}

// ============ UTILITY ============

void metadata_table_print(MetadataHashTable* table) {
    if (!table) return;
    
    pthread_mutex_lock(&table->count_lock);
    uint32_t total_count = table->count;
    pthread_mutex_unlock(&table->count_lock);
    
    printf("\n========== METADATA TABLE (NESTED HASH) ==========\n");
    printf("Outer Table Size: %u, Total Files: %u\n", table->size, total_count);
    printf("==================================================\n");
    
    for (uint32_t i = 0; i < table->size; i++) {
        InnerHashTable* inner = table->buckets[i];
        if (!inner) continue;
        
        pthread_mutex_lock(&inner->lock);
        
        if (inner->count > 0) {
            printf("Outer Bucket %u (Inner size: %u, count: %u):\n", i, inner->size, inner->count);
            
            for (uint32_t j = 0; j < inner->size; j++) {
                FileMetadataNode* node = inner->buckets[j];
                
                if (node) {
                    printf("  Inner Bucket %u:\n", j);
                    while (node) {
                        printf("    File: %s\n", node->filename);
                        printf("      Owner: %s\n", node->owner);
                        printf("      Size: %lu bytes\n", node->file_size);
                        printf("      Words: %lu, Chars: %lu\n", node->word_count, node->char_count);
                        printf("      Last Access: %ld\n", node->last_access);
                        printf("      Last Modified: %ld\n", node->last_modified);
                        node = node->next;
                    }
                }
            }
        }
        
        pthread_mutex_unlock(&inner->lock);
    }
    printf("==================================================\n\n");
}

uint32_t metadata_table_get_count(MetadataHashTable* table) {
    if (!table) return 0;
    
    pthread_mutex_lock(&table->count_lock);
    uint32_t count = table->count;
    pthread_mutex_unlock(&table->count_lock);
    
    return count;
}

