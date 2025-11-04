#ifndef SS_DATA_STRUCTS_H
#define SS_DATA_STRUCTS_H

#include <pthread.h>
#include <stdbool.h>
#include "../common/protocol.h"

// --- Thread-Safe Replication Queue ---

typedef struct ReplQueueNode {
    char filename[MAX_FILENAME];
    MsgType operation; // e.g., REPLICATE_FILE, DELETE_FILE
    struct ReplQueueNode* next;
} ReplQueueNode;

typedef struct {
    ReplQueueNode *head, *tail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool stop;
} ReplicationQueue;

void repl_queue_init(ReplicationQueue* q);
void repl_queue_push(ReplicationQueue* q, const char* filename, MsgType operation);
ReplQueueNode* repl_queue_pop(ReplicationQueue* q);
void repl_queue_shutdown(ReplicationQueue* q);
void repl_queue_destroy(ReplicationQueue* q);


// --- File Lock Hash Map ---

typedef struct {
    char filename[MAX_FILENAME];
    pthread_rwlock_t file_lock; // Coarse lock for read, undo, delete, checkpoint
    pthread_mutex_t* sentence_locks; // Fine-grained write locks
    int sentence_capacity;
    pthread_mutex_t metadata_lock; // Lock for resizing sentence_locks
} FileLock;

typedef struct FileLockNode {
    FileLock* lock;
    struct FileLockNode* next;
} FileLockNode;

typedef struct {
    FileLockNode** buckets;
    int num_buckets;
    pthread_mutex_t* bucket_locks; // Mutex per bucket for fine-grained locking
} FileLockMap;

void lock_map_init(FileLockMap* map, int num_buckets);
// Gets a file lock. If create_if_missing is true, it creates one.
FileLock* lock_map_get(FileLockMap* map, const char* filename);
// Gets a specific sentence lock, resizing the array if necessary.
pthread_mutex_t* lock_map_get_sentence_lock(FileLock* file_lock, int sentence_num);
void lock_map_destroy(FileLockMap* map);

#endif // SS_DATA_STRUCTS_H