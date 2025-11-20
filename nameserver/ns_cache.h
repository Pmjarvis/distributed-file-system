#ifndef NS_CACHE_H
#define NS_CACHE_H

#include <stddef.h>

typedef struct CacheNode {
    char* key;
    void* value;
    struct CacheNode* prev; // For LRU list
    struct CacheNode* next; // For LRU list
    struct CacheNode* h_next; // For Hash Table collision chain
} CacheNode;

typedef struct LRUCache {
    int capacity;
    int size;
    CacheNode** buckets;
    int num_buckets;
    CacheNode* head; // MRU
    CacheNode* tail; // LRU
} LRUCache;

LRUCache* lru_cache_create(int capacity);
void lru_cache_put(LRUCache* cache, const char* key, void* value);
void* lru_cache_get(LRUCache* cache, const char* key);
void lru_cache_remove(LRUCache* cache, const char* key);
void lru_cache_free(LRUCache* cache, void (*free_value)(void*));

#endif
