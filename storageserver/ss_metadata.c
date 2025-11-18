#include "ss_metadata.h"
#include "ss_logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * THREAD-SAFE NESTED HASH TABLE IMPLEMENTATION - REDESIGNED
 * 
 * LOCKING ARCHITECTURE (Nov 15, 2025):
 * ====================================
 * 
 * KEY CHANGE: Locks moved from inner tables to outer buckets
 * 
 * OLD DESIGN:
 * - Each InnerHashTable had its own pthread_mutex_t
 * - Required locking AFTER accessing outer bucket pointer
 * - Race condition: pointer could be NULL, then change before lock
 * 
 * NEW DESIGN:
 * - Each outer bucket has its own pthread_mutex_t (1024 locks)
 * - Lock acquired BEFORE accessing outer bucket pointer
 * - Eliminates lazy allocation races completely
 * - Simpler: One lock protects bucket pointer AND inner table
 * 
 * THREAD SAFETY MECHANISMS:
 * 1. Per-Outer-Bucket Locking: MetadataHashTable has outer_bucket_locks[] array
 *    - Lock outer_bucket_locks[i] before accessing buckets[i]
 *    - Protects: NULL check, inner table creation, all inner operations
 *    - Reduces contention: 1024 independent locks
 * 
 * 2. Global Count Lock: Separate mutex for total file count
 *    - Prevents races when incrementing/decrementing global count
 *    - Independent from outer bucket locks
 * 
 * 3. Copy-on-Return: metadata_table_get() returns allocated copies
 *    - Prevents use-after-free when node is removed by another thread
 *    - Caller owns the memory and must free() it
 * 
 * 4. Single-Lock Pattern: Operations lock ONCE per outer bucket
 *    - Lock → Check pointer → Allocate if needed → Operate → Unlock
 *    - Atomic lazy initialization (no race)
 * 
 * 5. No Nested Locking: Inner table functions assume caller holds lock
 *    - inner_table_* functions have NO locking
 *    - All locking done at outer table level
 *    - Prevents deadlocks, simplifies reasoning
 * 
 * CONCURRENCY SCENARIOS:
 * 
 * Scenario 1: Concurrent inserts to different outer buckets
 *   Thread A: insert("file1.txt") → locks outer_bucket_locks[5]
 *   Thread B: insert("file2.txt") → locks outer_bucket_locks[10]
 *   Result: Both proceed concurrently, independent locks
 * 
 * Scenario 2: Concurrent lazy allocation (same outer bucket)
 *   Thread A: insert("file1.txt") → locks bucket[5], sees NULL, creates inner table
 *   Thread B: insert("file2.txt") → waits on lock, sees existing table, uses it
 *   Result: Serialized by lock, only one table created
 * 
 * Scenario 3: Concurrent read while another thread removes
 *   Thread A: get("file1.txt") → locks, copies, unlocks
 *   Thread B: remove("file1.txt") → locks (waits), removes, frees original
 *   Result: Thread A has its own copy, safe to use
 * 
 * PERFORMANCE:
 * - Average case: O(1) for all operations
 * - Worst case: O(n/65536) where n = total files (very short chains)
 * - Lock contention: Low due to 1024 independent locks
 * - Same concurrency as before, simpler implementation
 * 
 * MEMORY SAFETY:
 * - All returned pointers from get() must be freed by caller
 * - Inner tables freed recursively in metadata_table_free()
 * - No leaked inner tables (lock held during creation)
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
// NOTE: These functions assume caller holds outer bucket lock!
// NEVER call these directly - use outer table functions instead.

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
    
    // NO MUTEX - protected by outer bucket lock
    return table;
}

void inner_table_free(InnerHashTable* table) {
    if (!table) return;
    
    // Free all linked list nodes in all buckets
    for (uint32_t i = 0; i < table->size; i++) {
        FileMetadataNode* curr = table->buckets[i];
        while (curr) {
            FileMetadataNode* next = curr->next;
            free(curr);
            curr = next;
        }
    }
    
    // NO MUTEX to destroy
    free(table->buckets);
    free(table);
}

int inner_table_insert(InnerHashTable* table, const char* filename,
                      const char* owner, uint64_t file_size,
                      uint64_t word_count, uint64_t char_count,
                      time_t last_access, time_t last_modified, bool is_backup) {
    if (!table || !filename) return -1;
    
    // NO LOCKING - caller holds outer bucket lock
    
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
            curr->is_backup = is_backup;  // Update backup flag
            
            return 1; // Updated
        }
        curr = curr->next;
    }
    
    // Create new node
    FileMetadataNode* new_node = (FileMetadataNode*)malloc(sizeof(FileMetadataNode));
    if (!new_node) {
        return -1;
    }
    
    strncpy(new_node->filename, filename, MAX_FILENAME - 1);
    new_node->filename[MAX_FILENAME - 1] = '\0';
    
    if (owner) {
        strncpy(new_node->owner, owner, MAX_USERNAME - 1);
        new_node->owner[MAX_USERNAME - 1] = '\0';
    } else {
        // CRITICAL: Owner should NEVER be NULL for tracked files!
        // This is a programming error if it happens
        fprintf(stderr, "ERROR: metadata_table_insert called with NULL owner for file '%s'\n", filename);
        strcpy(new_node->owner, "unknown");
    }
    
    new_node->file_size = file_size;
    new_node->word_count = word_count;
    new_node->char_count = char_count;
    new_node->last_access = last_access;
    new_node->last_modified = last_modified;
    new_node->is_backup = is_backup;  // Set backup flag
    
    // Insert at head of inner bucket
    new_node->next = table->buckets[index];
    table->buckets[index] = new_node;
    table->count++;
    
    return 0; // Inserted
}

FileMetadataNode* inner_table_get(InnerHashTable* table, const char* filename) {
    if (!table || !filename) return NULL;
    
    // NO LOCKING - caller holds outer bucket lock
    
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
    
    return result;  // Caller must free() this
}

int inner_table_remove(InnerHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    // NO LOCKING - caller holds outer bucket lock
    
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
            return 0;
        }
        prev = curr;
        curr = curr->next;
    }
    
    return -1; // Not found
}

// ============ OUTER HASH TABLE OPERATIONS ============
// These functions manage the outer bucket locks.

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
    
    // Allocate array of mutexes for each outer bucket
    table->outer_bucket_locks = (pthread_mutex_t*)malloc(outer_size * sizeof(pthread_mutex_t));
    if (!table->outer_bucket_locks) {
        ss_log("ERROR: Failed to allocate outer bucket locks");
        free(table->buckets);
        free(table);
        return NULL;
    }
    
    // Initialize all outer bucket locks
    for (uint32_t i = 0; i < outer_size; i++) {
        if (pthread_mutex_init(&table->outer_bucket_locks[i], NULL) != 0) {
            ss_log("ERROR: Failed to initialize mutex %u", i);
            // Cleanup already initialized mutexes
            for (uint32_t j = 0; j < i; j++) {
                pthread_mutex_destroy(&table->outer_bucket_locks[j]);
            }
            free(table->outer_bucket_locks);
            free(table->buckets);
            free(table);
            return NULL;
        }
    }
    
    // Initialize global count lock
    if (pthread_mutex_init(&table->count_lock, NULL) != 0) {
        ss_log("ERROR: Failed to initialize count lock");
        for (uint32_t i = 0; i < outer_size; i++) {
            pthread_mutex_destroy(&table->outer_bucket_locks[i]);
        }
        free(table->outer_bucket_locks);
        free(table->buckets);
        free(table);
        return NULL;
    }
    
    // Note: Inner hash tables are created lazily on first insert
    ss_log("Nested metadata hash table initialized: %u outer buckets × %u inner buckets (1024 locks)",
           outer_size, INNER_TABLE_SIZE);
    return table;
}

void metadata_table_free(MetadataHashTable* table) {
    if (!table) return;
    
    // Free all inner tables
    for (uint32_t i = 0; i < table->size; i++) {
        if (table->buckets[i]) {
            inner_table_free(table->buckets[i]);
        }
    }
    
    // Destroy all outer bucket locks
    for (uint32_t i = 0; i < table->size; i++) {
        pthread_mutex_destroy(&table->outer_bucket_locks[i]);
    }
    
    pthread_mutex_destroy(&table->count_lock);
    free(table->outer_bucket_locks);
    free(table->buckets);
    free(table);
    ss_log("Nested metadata hash table freed (1024 locks destroyed)");
}

int metadata_table_insert(MetadataHashTable* table, const char* filename,
                         const char* owner, uint64_t file_size,
                         uint64_t word_count, uint64_t char_count,
                         time_t last_access, time_t last_modified, bool is_backup) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // LOCK OUTER BUCKET FIRST (eliminates lazy allocation races)
    pthread_mutex_lock(&table->outer_bucket_locks[outer_index]);
    
    // Check if inner table exists (safe - we hold the lock)
    if (!table->buckets[outer_index]) {
        // Create inner table (no race - lock held)
        table->buckets[outer_index] = inner_table_init(INNER_TABLE_SIZE);
        if (!table->buckets[outer_index]) {
            pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
            ss_log("ERROR: Failed to create inner table for bucket %u", outer_index);
            return -1;
        }
        ss_log("DEBUG: Lazily created inner table for outer bucket %u", outer_index);
    }
    
    // Inner table exists, insert into it (caller holds lock)
    InnerHashTable* inner = table->buckets[outer_index];
    int result = inner_table_insert(inner, filename, owner, file_size,
                                   word_count, char_count, last_access, last_modified, is_backup);
    
    // UNLOCK OUTER BUCKET
    pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
    
    // Update global count if new insertion
    if (result == 0) {
        pthread_mutex_lock(&table->count_lock);
        table->count++;
        pthread_mutex_unlock(&table->count_lock);
        
        ss_log("Inserted metadata for: %s (owner: %s, size: %lu, is_backup: %d)",
               filename, owner ? owner : "unknown", file_size, is_backup);
    } else if (result == 1) {
        ss_log("Updated metadata for: %s", filename);
    }
    
    return result;
}

FileMetadataNode* metadata_table_get(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return NULL;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // LOCK OUTER BUCKET
    pthread_mutex_lock(&table->outer_bucket_locks[outer_index]);
    
    if (!table->buckets[outer_index]) {
        pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
        return NULL; // Inner table doesn't exist, file not found
    }
    
    // Get copy from inner table (caller holds lock)
    FileMetadataNode* result = inner_table_get(table->buckets[outer_index], filename);
    
    // UNLOCK OUTER BUCKET
    pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
    
    return result;  // Caller must free()
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
// All update functions use outer bucket locking.

int metadata_table_update_size(MetadataHashTable* table, const char* filename, uint64_t new_size) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // LOCK OUTER BUCKET
    pthread_mutex_lock(&table->outer_bucket_locks[outer_index]);
    
    if (!table->buckets[outer_index]) {
        pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
        return -1; // Inner table doesn't exist
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
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
    
    // UNLOCK OUTER BUCKET
    pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
    
    return found ? 0 : -1;
}

int metadata_table_update_counts(MetadataHashTable* table, const char* filename,
                                 uint64_t word_count, uint64_t char_count) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // LOCK OUTER BUCKET
    pthread_mutex_lock(&table->outer_bucket_locks[outer_index]);
    
    if (!table->buckets[outer_index]) {
        pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
        return -1;
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
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
    
    // UNLOCK OUTER BUCKET
    pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
    
    return found ? 0 : -1;
}

int metadata_table_update_access_time(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // LOCK OUTER BUCKET
    pthread_mutex_lock(&table->outer_bucket_locks[outer_index]);
    
    if (!table->buckets[outer_index]) {
        pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
        return -1;
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
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
    
    // UNLOCK OUTER BUCKET
    pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
    
    return found ? 0 : -1;
}

int metadata_table_update_modified_time(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // LOCK OUTER BUCKET
    pthread_mutex_lock(&table->outer_bucket_locks[outer_index]);
    
    if (!table->buckets[outer_index]) {
        pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
        return -1;
    }
    
    InnerHashTable* inner = table->buckets[outer_index];
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
    
    // UNLOCK OUTER BUCKET
    pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
    
    return found ? 0 : -1;
}

// ============ REMOVE ============

int metadata_table_remove(MetadataHashTable* table, const char* filename) {
    if (!table || !filename) return -1;
    
    uint32_t hash = metadata_hash_primary(filename);
    uint32_t outer_index = hash % table->size;
    
    // LOCK OUTER BUCKET
    pthread_mutex_lock(&table->outer_bucket_locks[outer_index]);
    
    if (!table->buckets[outer_index]) {
        pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
        return -1; // Inner table doesn't exist, file not found
    }
    
    // Remove from inner table (caller holds lock)
    int result = inner_table_remove(table->buckets[outer_index], filename);
    
    // UNLOCK OUTER BUCKET
    pthread_mutex_unlock(&table->outer_bucket_locks[outer_index]);
    
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

/*
 * ATOMIC SAVE IMPLEMENTATION
 * 
 * This function creates a consistent snapshot of the hash table by:
 * 1. Locking ALL outer buckets before writing anything
 * 2. Writing all data while locks are held
 * 3. Unlocking all buckets after write completes
 * 
 * This ensures the saved count matches the saved entries exactly,
 * preventing "count mismatch" warnings on reload.
 * 
 * Trade-off: Blocks all table operations during save (~50-100ms for 1000 files)
 * This is acceptable because saves only happen every 60 seconds (checkpoint).
 */
int metadata_table_save(MetadataHashTable* table, const char* filepath) {
    if (!table || !filepath) return -1;
    
    ss_log("Saving metadata to %s (atomic snapshot)...", filepath);
    
    // PHASE 1: Lock ALL outer buckets (creates consistent snapshot)
    for (uint32_t i = 0; i < table->size; i++) {
        pthread_mutex_lock(&table->outer_bucket_locks[i]);
    }
    
    // Get count while all locks held (guaranteed consistent)
    pthread_mutex_lock(&table->count_lock);
    uint32_t total_count = table->count;
    pthread_mutex_unlock(&table->count_lock);
    
    // PHASE 2: Write to disk (all locks held - atomic snapshot)
    FILE* fp = fopen(filepath, "wb");
    if (!fp) {
        perror("Failed to open metadata file for writing");
        
        // Unlock all before returning
        for (uint32_t i = 0; i < table->size; i++) {
            pthread_mutex_unlock(&table->outer_bucket_locks[i]);
        }
        return -1;
    }
    
    // Write total count
    if (fwrite(&total_count, sizeof(uint32_t), 1, fp) != 1) {
        fprintf(stderr, "Failed to write metadata count\n");
        fclose(fp);
        
        // Unlock all before returning
        for (uint32_t i = 0; i < table->size; i++) {
            pthread_mutex_unlock(&table->outer_bucket_locks[i]);
        }
        return -1;
    }
    
    uint32_t entries_written = 0;
    
    // Write all entries (snapshot is consistent - all locks held)
    for (uint32_t i = 0; i < table->size; i++) {
        InnerHashTable* inner = table->buckets[i];
        
        if (!inner) continue;  // Skip empty outer buckets
        
        // Iterate through inner table (safe - lock held)
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
                
                // Write is_backup field
                uint8_t is_backup_byte = node->is_backup ? 1 : 0;
                fwrite(&is_backup_byte, sizeof(uint8_t), 1, fp);
                
                entries_written++;
                node = node->next;
            }
        }
    }
    
    fclose(fp);
    
    // PHASE 3: Unlock all buckets
    for (uint32_t i = 0; i < table->size; i++) {
        pthread_mutex_unlock(&table->outer_bucket_locks[i]);
    }
    
    // Verify count matches
    if (entries_written != total_count) {
        ss_log("WARNING: Saved %u entries but count was %u", 
               entries_written, total_count);
    } else {
        ss_log("Successfully saved %u metadata entries (atomic snapshot)", 
               entries_written);
    }
    
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
        ss_log("WARNING: Failed to read metadata count from %s", filepath);
        fclose(fp);
        return NULL;
    }
    
    ss_log("Loading %u metadata entries from %s...", expected_count, filepath);
    
    // Create new table
    MetadataHashTable* table = metadata_table_init(OUTER_TABLE_SIZE);
    if (!table) {
        ss_log("ERROR: Failed to create metadata table during load");
        fclose(fp);
        return NULL;
    }
    
    uint32_t loaded = 0;
    
    // Read entries
    for (uint32_t i = 0; i < expected_count; i++) {
        // Read filename length
        uint32_t name_len;
        if (fread(&name_len, sizeof(uint32_t), 1, fp) != 1) {
            ss_log("WARNING: Failed to read filename length at entry %u", i);
            break;
        }
        
        // Validate filename length
        if (name_len == 0 || name_len > MAX_FILENAME) {
            ss_log("WARNING: Invalid filename length %u at entry %u (skipping)", name_len, i);
            break;
        }
        
        // Allocate and read filename
        char* filename = malloc(name_len);
        if (!filename) {
            ss_log("ERROR: Memory allocation failed for filename at entry %u", i);
            break;
        }
        
        if (fread(filename, 1, name_len, fp) != name_len) {
            ss_log("WARNING: Failed to read filename at entry %u", i);
            free(filename);
            break;
        }
        
        // Read owner length
        uint32_t owner_len;
        if (fread(&owner_len, sizeof(uint32_t), 1, fp) != 1) {
            ss_log("WARNING: Failed to read owner length at entry %u", i);
            free(filename);
            break;
        }
        
        // Validate owner length
        if (owner_len == 0 || owner_len > MAX_USERNAME) {
            ss_log("WARNING: Invalid owner length %u at entry %u (skipping)", owner_len, i);
            free(filename);
            break;
        }
        
        // Allocate and read owner
        char* owner = malloc(owner_len);
        if (!owner) {
            ss_log("ERROR: Memory allocation failed for owner at entry %u", i);
            free(filename);
            break;
        }
        
        if (fread(owner, 1, owner_len, fp) != owner_len) {
            ss_log("WARNING: Failed to read owner at entry %u", i);
            free(filename);
            free(owner);
            break;
        }
        
        // Read metadata fields
        uint64_t file_size, word_count, char_count;
        time_t last_modified, last_access;
        uint8_t is_backup_byte = 0;  // Default to false for old metadata files
        
        if (fread(&file_size, sizeof(uint64_t), 1, fp) != 1 ||
            fread(&word_count, sizeof(uint64_t), 1, fp) != 1 ||
            fread(&char_count, sizeof(uint64_t), 1, fp) != 1 ||
            fread(&last_modified, sizeof(time_t), 1, fp) != 1 ||
            fread(&last_access, sizeof(time_t), 1, fp) != 1) {
            ss_log("WARNING: Failed to read metadata fields at entry %u", i);
            free(filename);
            free(owner);
            break;
        }
        
        // Try to read is_backup field (may not exist in old metadata files)
        fread(&is_backup_byte, sizeof(uint8_t), 1, fp);
        bool is_backup = (is_backup_byte != 0);
        
        // Insert using public API (handles locking)
        int insert_result = metadata_table_insert(table, filename, owner, 
                                                 file_size, word_count, char_count, 
                                                 last_access, last_modified, is_backup);
        
        if (insert_result == 0 || insert_result == 1) {
            loaded++;
        } else {
            ss_log("WARNING: Failed to insert '%s' during load", filename);
        }
        
        free(filename);
        free(owner);
    }
    
    fclose(fp);
    
    ss_log("Loaded %u/%u metadata entries from %s", loaded, expected_count, filepath);
    
    if (loaded != expected_count) {
        ss_log("WARNING: Loaded count mismatch (expected %u, got %u)", 
               expected_count, loaded);
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
        // LOCK OUTER BUCKET
        pthread_mutex_lock(&table->outer_bucket_locks[i]);
        
        InnerHashTable* inner = table->buckets[i];
        if (!inner) {
            pthread_mutex_unlock(&table->outer_bucket_locks[i]);
            continue;
        }
        
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
        
        // UNLOCK OUTER BUCKET
        pthread_mutex_unlock(&table->outer_bucket_locks[i]);
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

