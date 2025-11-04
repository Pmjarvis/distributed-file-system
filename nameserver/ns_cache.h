#ifndef NS_CACHE_H
#define NS_CACHE_H

#include <glib.h> // Requires GLib. Install with `sudo apt-get install libglib2.0-dev`

typedef struct CacheNode {
    char* key;
    void* value;
    struct CacheNode *prev, *next;
} CacheNode;

typedef struct {
    int capacity;
    GHashTable* map; // Hash map for O(1) lookups
    CacheNode *head, *tail; // Doubly linked list for O(1) eviction
} LRUCache;

LRUCache* lru_cache_create(int capacity);
void lru_cache_put(LRUCache* cache, const char* key, void* value);
void* lru_cache_get(LRUCache* cache, const char* key);
void lru_cache_remove(LRUCache* cache, const char* key);
void lru_cache_free(LRUCache* cache, void (*free_value)(void*));

#endif // NS_CACHE_H