#include "ss_data_structs.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// --- Replication Queue Implementation ---

void repl_queue_init(ReplicationQueue* q) {
    q->head = NULL;
    q->tail = NULL;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
    q->stop = false;
}

void repl_queue_push(ReplicationQueue* q, const char* filename, MsgType operation) {
    ReplQueueNode* node = (ReplQueueNode*)malloc(sizeof(ReplQueueNode));
    
    // --- FIX: Use MAX_FILENAME ---
    strncpy(node->filename, filename, MAX_FILENAME - 1);
    node->filename[MAX_FILENAME - 1] = '\0';
    
    node->operation = operation;
    node->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail) {
        q->tail->next = node;
    } else {
        q->head = node;
    }
    q->tail = node;
    pthread_cond_signal(&q->cond); // Signal worker thread
    pthread_mutex_unlock(&q->mutex);
}

ReplQueueNode* repl_queue_pop(ReplicationQueue* q) {
    pthread_mutex_lock(&q->mutex);
    
    // Wait while queue is empty AND not stopped
    while (q->head == NULL && !q->stop) {
        pthread_cond_wait(&q->cond, &q->mutex);
    }

    if (q->stop && q->head == NULL) {
        pthread_mutex_unlock(&q->mutex);
        return NULL; // Shutdown signal
    }

    ReplQueueNode* node = q->head;
    q->head = q->head->next;
    if (q->head == NULL) {
        q->tail = NULL;
    }
    
    pthread_mutex_unlock(&q->mutex);
    return node;
}

void repl_queue_shutdown(ReplicationQueue* q) {
    pthread_mutex_lock(&q->mutex);
    q->stop = true;
    pthread_cond_broadcast(&q->cond); // Wake up all waiting workers
    pthread_mutex_unlock(&q->mutex);
}

void repl_queue_destroy(ReplicationQueue* q) {
    ReplQueueNode* curr = q->head;
    while (curr) {
        ReplQueueNode* next = curr->next;
        free(curr);
        curr = next;
    }
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
}


// --- File Lock Map Implementation ---

// djb2 hash function
static unsigned long _lock_map_hash(const char *str, int num_buckets) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    return hash % num_buckets;
}

void lock_map_init(FileLockMap* map, int num_buckets) {
    map->num_buckets = num_buckets;
    map->buckets = (FileLockNode**)calloc(num_buckets, sizeof(FileLockNode*));
    map->bucket_locks = (pthread_mutex_t*)malloc(num_buckets * sizeof(pthread_mutex_t));
    for (int i = 0; i < num_buckets; i++) {
        pthread_mutex_init(&map->bucket_locks[i], NULL);
    }
}

static FileLock* _file_lock_create(const char* filename) {
    FileLock* lock = (FileLock*)malloc(sizeof(FileLock));
    
    // --- FIX: Use MAX_FILENAME ---
    strncpy(lock->filename, filename, MAX_FILENAME - 1);
    lock->filename[MAX_FILENAME - 1] = '\0';
    
    pthread_rwlock_init(&lock->file_lock, NULL);
    pthread_mutex_init(&lock->metadata_lock, NULL);
    lock->sentence_locks = NULL;
    lock->sentence_capacity = 0;
    
    return lock;
}

FileLock* lock_map_get(FileLockMap* map, const char* filename) {
    unsigned long index = _lock_map_hash(filename, map->num_buckets);
    
    pthread_mutex_lock(&map->bucket_locks[index]);
    
    // 1. Search for existing lock
    FileLockNode* curr = map->buckets[index];
    while (curr) {
        if (strcmp(curr->lock->filename, filename) == 0) {
            pthread_mutex_unlock(&map->bucket_locks[index]);
            return curr->lock;
        }
        curr = curr->next;
    }
    
    // 2. Not found, create new one
    FileLock* new_lock = _file_lock_create(filename);
    FileLockNode* new_node = (FileLockNode*)malloc(sizeof(FileLockNode));
    new_node->lock = new_lock;
    new_node->next = map->buckets[index];
    map->buckets[index] = new_node;
    
    pthread_mutex_unlock(&map->bucket_locks[index]);
    return new_lock;
}

pthread_mutex_t* lock_map_get_sentence_lock(FileLock* file_lock, int sentence_num) {
    pthread_mutex_lock(&file_lock->metadata_lock);
    
    if (sentence_num >= file_lock->sentence_capacity) {
        int new_capacity = sentence_num + 10; // Grow by 10
        file_lock->sentence_locks = (pthread_mutex_t*)realloc(file_lock->sentence_locks, new_capacity * sizeof(pthread_mutex_t));
        
        // Initialize *new* mutexes
        for (int i = file_lock->sentence_capacity; i < new_capacity; i++) {
            pthread_mutex_init(&file_lock->sentence_locks[i], NULL);
        }
        file_lock->sentence_capacity = new_capacity;
    }
    
    pthread_mutex_unlock(&file_lock->metadata_lock);
    return &file_lock->sentence_locks[sentence_num];
}

void lock_map_destroy(FileLockMap* map) {
    for (int i = 0; i < map->num_buckets; i++) {
        FileLockNode* curr = map->buckets[i];
        while (curr) {
            FileLockNode* next = curr->next;
            
            // Destroy lock components
            FileLock* lock = curr->lock;
            pthread_rwlock_destroy(&lock->file_lock);
            pthread_mutex_destroy(&lock->metadata_lock);
            for (int j = 0; j < lock->sentence_capacity; j++) {
                pthread_mutex_destroy(&lock->sentence_locks[j]);
            }
            free(lock->sentence_locks);
            free(lock);
            
            free(curr);
            curr = next;
        }
        pthread_mutex_destroy(&map->bucket_locks[i]);
    }
    free(map->buckets);
    free(map->bucket_locks);
}