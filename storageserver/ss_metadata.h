#ifndef SS_METADATA_H
#define SS_METADATA_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <pthread.h>

/*
 * THREAD-SAFE NESTED HASH TABLE FOR FILE METADATA
 * 
 * REDESIGNED LOCKING STRATEGY (Nov 15, 2025):
 * ============================================
 * 
 * CONCURRENCY MODEL:
 * - Two-level hashing: Outer table (1024 buckets) -> Inner tables (64 buckets each)
 * - ONE MUTEX PER OUTER BUCKET (not per inner table!)
 * - Protects: outer bucket pointer + entire inner table (if allocated)
 * - Eliminates lazy allocation races completely
 * 
 * WHY OUTER BUCKET LOCKS?
 * 1. Simpler: Lock protects bucket pointer AND inner table atomically
 * 2. Safer: No race between checking pointer and using inner table
 * 3. Cleaner: No separate lock inside InnerHashTable structure
 * 4. Same concurrency: Still 1024 independent locks
 * 
 * THREAD SAFETY GUARANTEES:
 * - metadata_table_get() returns a COPY of the node (caller must free())
 * - All operations acquire ONE outer bucket lock
 * - Lock held from pointer check through all inner table operations
 * - No use-after-free: returned pointers are owned by caller
 * - No lazy allocation races: pointer check + creation atomic
 * 
 * LOCKING RULES:
 * 1. Always lock outer_bucket_locks[outer_index] before accessing buckets[outer_index]
 * 2. Never access inner table pointer without holding corresponding outer lock
 * 3. Lock is held for entire operation (check, allocate, modify, return)
 * 4. No nested locking (one outer lock at a time)
 * 
 * USAGE NOTES:
 * - Always free() the result of metadata_table_get() when done
 * - Update operations are atomic and thread-safe
 * - Multiple threads can operate on different outer buckets concurrently
 * - Operations on same outer bucket are serialized (thread-safe)
 */

#define MAX_FILENAME 256
#define MAX_USERNAME 64
#define OUTER_TABLE_SIZE 1024     // Outer hash table size
#define INNER_TABLE_SIZE 64       // Inner hash table size (per bucket)
// METADATA_DB_PATH is now defined in ss_globals.h (dynamically set based on SS ID)


// File metadata node (leaf node - stores actual metadata)
typedef struct FileMetadataNode {
    char filename[MAX_FILENAME];
    char owner[MAX_USERNAME];
    
    uint64_t file_size;        // Size in bytes
    uint64_t word_count;       // Total words
    uint64_t char_count;       // Total characters
    time_t last_access;        // Last access timestamp
    time_t last_modified;      // Last modification timestamp
    
    bool is_backup;            // true if this file is a backup copy, false if primary
    
    struct FileMetadataNode* next;  // For chaining within inner hash table bucket
} FileMetadataNode;

// Inner hash table (stored in each outer bucket)
// NOTE: No lock inside - protected by outer bucket lock
typedef struct InnerHashTable {
    FileMetadataNode** buckets;  // Array of inner buckets
    uint32_t size;               // Size of inner table (INNER_TABLE_SIZE)
    uint32_t count;              // Number of entries in this inner table
} InnerHashTable;

// Outer hash table structure (nested hash table)
typedef struct MetadataHashTable {
    InnerHashTable** buckets;             // Array of inner hash tables (may be NULL)
    pthread_mutex_t* outer_bucket_locks;  // ONE LOCK PER OUTER BUCKET (1024 locks)
    uint32_t size;                        // Number of outer buckets (OUTER_TABLE_SIZE)
    uint32_t count;                       // Total number of files (atomic operations)
    pthread_mutex_t count_lock;           // Lock for global count only
} MetadataHashTable;

// Function declarations

// Initialize hash table
MetadataHashTable* metadata_table_init(uint32_t size);

// Free hash table
void metadata_table_free(MetadataHashTable* table);

// Hash function
uint32_t metadata_hash(const char* filename);

// Insert or update metadata
int metadata_table_insert(MetadataHashTable* table, const char* filename, 
                         const char* owner, uint64_t file_size,
                         uint64_t word_count, uint64_t char_count,
                         time_t last_access, time_t last_modified, bool is_backup);

// Get metadata
FileMetadataNode* metadata_table_get(MetadataHashTable* table, const char* filename);

// Check if file exists
int metadata_table_exists(MetadataHashTable* table, const char* filename);

// Update specific fields
int metadata_table_update_size(MetadataHashTable* table, const char* filename, uint64_t new_size);
int metadata_table_update_counts(MetadataHashTable* table, const char* filename, 
                                 uint64_t word_count, uint64_t char_count);
int metadata_table_update_access_time(MetadataHashTable* table, const char* filename);
int metadata_table_update_modified_time(MetadataHashTable* table, const char* filename);

// Remove metadata
int metadata_table_remove(MetadataHashTable* table, const char* filename);

// Persistence functions
int metadata_table_save(MetadataHashTable* table, const char* filepath);
MetadataHashTable* metadata_table_load(const char* filepath);

// Utility functions
void metadata_table_print(MetadataHashTable* table);
uint32_t metadata_table_get_count(MetadataHashTable* table);

#endif // SS_METADATA_H
