#ifndef SS_METADATA_H
#define SS_METADATA_H

#include <stdint.h>
#include <time.h>
#include <pthread.h>

/*
 * THREAD-SAFE NESTED HASH TABLE FOR FILE METADATA
 * 
 * This implementation provides a thread-safe nested hash table structure for
 * storing file metadata with the following guarantees:
 * 
 * CONCURRENCY MODEL:
 * - Two-level hashing: Outer table (1024 buckets) -> Inner tables (64 buckets each)
 * - Each inner table has its own pthread_mutex_t for fine-grained locking
 * - Global count protected by separate count_lock
 * - Atomic lazy initialization prevents race conditions
 * 
 * THREAD SAFETY GUARANTEES:
 * - metadata_table_get() returns a COPY of the node (caller must free())
 * - All update operations acquire lock once and hold during entire operation
 * - No use-after-free: returned pointers are owned by caller
 * - Atomic insert with check-and-install pattern for inner table creation
 * 
 * USAGE NOTES:
 * - Always free() the result of metadata_table_get() when done
 * - Update operations are atomic and thread-safe
 * - Multiple threads can operate on different outer buckets concurrently
 * - Operations on same inner table are serialized (thread-safe)
 */

#define MAX_FILENAME 256
#define MAX_USERNAME 64
#define OUTER_TABLE_SIZE 1024     // Outer hash table size
#define INNER_TABLE_SIZE 64       // Inner hash table size (per bucket)
#define METADATA_DB_PATH "ss_data/metadata.db"

// File metadata node (leaf node - stores actual metadata)
typedef struct FileMetadataNode {
    char filename[MAX_FILENAME];
    char owner[MAX_USERNAME];
    
    uint64_t file_size;        // Size in bytes
    uint64_t word_count;       // Total words
    uint64_t char_count;       // Total characters
    time_t last_access;        // Last access timestamp
    time_t last_modified;      // Last modification timestamp
    
    struct FileMetadataNode* next;  // For chaining within inner hash table bucket
} FileMetadataNode;

// Inner hash table (stored in each outer bucket)
typedef struct InnerHashTable {
    FileMetadataNode** buckets;  // Array of inner buckets
    uint32_t size;               // Size of inner table (INNER_TABLE_SIZE)
    uint32_t count;              // Number of entries in this inner table
    pthread_mutex_t lock;        // Lock for this inner table
} InnerHashTable;

// Outer hash table structure (nested hash table)
typedef struct MetadataHashTable {
    InnerHashTable** buckets;    // Array of inner hash tables
    uint32_t size;               // Number of outer buckets (OUTER_TABLE_SIZE)
    uint32_t count;              // Total number of files across all inner tables
    pthread_mutex_t count_lock;  // Lock for count field
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
                         time_t last_access, time_t last_modified);

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
