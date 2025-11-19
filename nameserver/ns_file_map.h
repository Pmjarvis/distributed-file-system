#ifndef NS_FILE_MAP_H
#define NS_FILE_MAP_H

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include "../common/protocol.h"

// Initial size for file mapping hash table
#define INITIAL_FILE_MAP_SIZE 1024
#define FILE_MAP_NUM_LOCKS 256  // Number of bucket locks (power of 2 for fast modulo)

/**
 * @brief Node representing a file in the hash table
 * Stores the filename, primary SS ID, backup SS ID (NO metadata - query SS for that)
 */
typedef struct FileMapNode {
    char filename[MAX_FILENAME];        // The key
    int primary_ss_id;                  // Primary storage server ID
    int backup_ss_id;                   // Backup storage server ID
    char owner[MAX_USERNAME];           // File owner (for access control)
} FileMapNode;

/**
 * @brief Hash table for file-to-SS mapping
 * Uses open addressing with double hashing
 * 
 * THREAD SAFETY (matching SS metadata table design):
 * - 256 bucket locks for fine-grained concurrency
 * - Each lock protects a range of hash table slots
 * - Operations acquire ONE bucket lock based on filename hash
 * - Multiple threads can operate on different buckets concurrently
 * - Separate count_lock protects global count field
 */
typedef struct FileMapHashTable {
    FileMapNode** nodes;                // Array of pointers to nodes
    size_t size;                        // Size of the array
    size_t count;                       // Number of items stored
    pthread_mutex_t* bucket_locks;      // Array of locks (one per bucket range)
    pthread_mutex_t count_lock;         // Lock for global count
    size_t num_locks;                   // Number of bucket locks
} FileMapHashTable;

/**
 * @brief Creates a new file mapping hash table
 * @param size Initial size of the hash table
 * @return Pointer to the new hash table, or NULL on error
 */
FileMapHashTable* file_map_table_create(size_t size);

/**
 * @brief Frees a file mapping hash table
 * @param table The hash table to free
 */
void file_map_table_free(FileMapHashTable* table);

/**
 * @brief Inserts or updates a file mapping in the hash table
 * @param table The hash table
 * @param filename The filename (key)
 * @param primary_ss_id Primary storage server ID
 * @param backup_ss_id Backup storage server ID
 * @param owner File owner username
 * @return 1 on success, 0 on failure
 */
int file_map_table_insert(FileMapHashTable* table, const char* filename,
                          int primary_ss_id, int backup_ss_id,
                          const char* owner);

/**
 * @brief Searches for a file by owner and filename
 * @param table The hash table
 * @param owner The file owner username
 * @param filename The filename to search for
 * @return Pointer to the FileMapNode if found, NULL otherwise
 * @note The returned pointer should NOT be freed by the caller
 */
FileMapNode* file_map_table_search(FileMapHashTable* table, const char* owner, const char* filename);

/**
 * @brief Searches for a file by SS ID and filename (used during recovery)
 * @param table The hash table
 * @param ss_id The primary SS ID
 * @param filename The filename to search for
 * @return Pointer to the FileMapNode if found, NULL otherwise
 * @note The returned pointer should NOT be freed by the caller
 * @note This searches all owners for a file on a specific SS
 */
FileMapNode* file_map_table_search_by_ss_and_filename(FileMapHashTable* table, int ss_id, const char* filename);

/**
 * @brief Finds the owner of a file by filename
 * @param table The hash table
 * @param filename The filename to search for
 * @return A copy of the owner's username (caller must free), or NULL if not found
 * @note This searches across all owners to find who owns the file
 * @note The returned string MUST be freed by the caller using free()
 */
char* file_map_table_find_owner(FileMapHashTable* table, const char* filename);

/**
 * @brief Deletes a file from the hash table
 * @param table The hash table
 * @param owner The file owner username
 * @param filename The filename to delete
 * @return 1 on success, 0 if not found
 */
int file_map_table_delete(FileMapHashTable* table, const char* owner, const char* filename);

/**
 * @brief Saves the hash table to disk
 * @param table The hash table
 * @param filepath Path to save to
 * @return 1 on success, 0 on failure
 */
int file_map_table_save(FileMapHashTable* table, const char* filepath);

/**
 * @brief Loads the hash table from disk
 * @param filepath Path to load from
 * @param size Initial size for the hash table
 * @return Pointer to the loaded hash table, or NULL on error
 */
FileMapHashTable* file_map_table_load(const char* filepath, size_t size);

/**
 * @brief Iterate over all files in the hash table
 * @param table The hash table
 * @param callback Function to call for each file
 * @param user_data User data to pass to the callback
 */
typedef void (*FileMapIteratorCallback)(const FileMapNode* node, void* user_data);
void file_map_table_iterate(FileMapHashTable* table, FileMapIteratorCallback callback, void* user_data);

/**
 * @brief Updates the primary SS ID for a file
 * @param table The hash table
 * @param owner The file owner username
 * @param filename The filename
 * @param new_primary_ss_id New primary SS ID
 * @return 1 on success, 0 if not found
 */
int file_map_table_update_primary(FileMapHashTable* table, const char* owner, const char* filename, int new_primary_ss_id);

/**
 * @brief Updates the backup SS ID for a file
 * @param table The hash table
 * @param owner The file owner username
 * @param filename The filename
 * @param new_backup_ss_id New backup SS ID
 * @return 1 on success, 0 if not found
 */
int file_map_table_update_backup(FileMapHashTable* table, const char* owner, const char* filename, int new_backup_ss_id);

/**
 * @brief Deletes all entries for a specific storage server
 * @param table The hash table
 * @param ss_id The storage server ID whose entries should be deleted
 * @return Number of entries deleted
 * @note This is used during SS recovery to clean up stale entries before re-adding fresh ones
 */
int file_map_table_delete_all_for_ss(FileMapHashTable* table, int ss_id);

#endif // NS_FILE_MAP_H
