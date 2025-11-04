#ifndef NS_ACCESS_H
#define NS_ACCESS_H

#include <stdlib.h>
#include <stdint.h>
#define DB_PATH "./permission_db"
// --- Data Structures ---

typedef struct FileNode FileNode;
typedef struct FileHashTable FileHashTable;
typedef struct UserNode UserNode;
typedef struct UserHashTable UserHashTable;

// --- Function Prototypes ---

// Hash Functions
uint64_t fnv1a_hash(const char *str);
uint64_t djb2_hash(const char *str);

// Inner Hash Table (File -> Perms)
FileHashTable* file_ht_create(size_t size);
void file_ht_free(FileHashTable* table);
int file_ht_insert(FileHashTable* table, const char* filename, const char* perms);
char* file_ht_search(FileHashTable* table, const char* filename);
int file_ht_delete(FileHashTable* table, const char* filename);

// Outer Hash Table (User -> FileTable)
UserHashTable* user_ht_create(size_t size);
void user_ht_free_system(UserHashTable* table);

// Public API & Persistence
void user_ht_add_permission(UserHashTable* table, const char* username, const char* filename, const char* perms);
char* user_ht_get_permission(UserHashTable* table, const char* username, const char* filename);
void user_ht_revoke_permission(UserHashTable* table, const char* username, const char* filename);
int user_ht_save(UserHashTable* table, const char* db_path);
UserHashTable* user_ht_load(const char* db_path);

#endif // NS_ACCESS_H