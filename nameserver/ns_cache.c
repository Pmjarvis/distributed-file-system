#include "ns_cache.h"
#include <stdlib.h>
#include <string.h>

// --- Internal List Helpers ---

static void _cache_detach_node(LRUCache* cache, CacheNode* node) {
    if (node->prev) {
        node->prev->next = node->next;
    } else {
        cache->head = node->next;
    }
    if (node->next) {
        node->next->prev = node->prev;
    } else {
        cache->tail = node->prev;
    }
}

static void _cache_attach_to_front(LRUCache* cache, CacheNode* node) {
    node->next = cache->head;
    node->prev = NULL;
    if (cache->head) {
        cache->head->prev = node;
    }
    cache->head = node;
    if (cache->tail == NULL) {
        cache->tail = node;
    }
}

static CacheNode* _cache_create_node(const char* key, void* value) {
    CacheNode* node = (CacheNode*)malloc(sizeof(CacheNode));
    node->key = strdup(key);
    node->value = value;
    return node;
}

// --- Public API ---

LRUCache* lru_cache_create(int capacity) {
    LRUCache* cache = (LRUCache*)malloc(sizeof(LRUCache));
    cache->capacity = capacity;
    cache->map = g_hash_table_new_full(g_str_hash, g_str_equal, free, NULL);
    cache->head = NULL;
    cache->tail = NULL;
    return cache;
}

void lru_cache_put(LRUCache* cache, const char* key, void* value) {
    CacheNode* node = (CacheNode*)g_hash_table_lookup(cache->map, key);

    if (node) {
        // Update existing node
        node->value = value; // Value is just replaced, not freed
        _cache_detach_node(cache, node);
        _cache_attach_to_front(cache, node);
    } else {
        // Add new node
        node = _cache_create_node(key, value);
        _cache_attach_to_front(cache, node);
        
        if (g_hash_table_size(cache->map) >= cache->capacity) {
            // Evict least recently used (tail)
            CacheNode* tail = cache->tail;
            _cache_detach_node(cache, tail);
            g_hash_table_remove(cache->map, tail->key); // This frees tail->key
            free(tail); // Free the node struct
        }
        
        // g_hash_table_insert will automatically free the old key if it exists
        // but we've already handled that. It will copy the new key.
        g_hash_table_insert(cache->map, strdup(key), node);
    }
}

void* lru_cache_get(LRUCache* cache, const char* key) {
    CacheNode* node = (CacheNode*)g_hash_table_lookup(cache->map, key);
    if (node) {
        // Move to front
        _cache_detach_node(cache, node);
        _cache_attach_to_front(cache, node);
        return node->value;
    }
    return NULL;
}

void lru_cache_remove(LRUCache* cache, const char* key) {
    CacheNode* node = (CacheNode*)g_hash_table_lookup(cache->map, key);
    if (node) {
        _cache_detach_node(cache, node);
        g_hash_table_remove(cache->map, node->key);
        free(node->key);
        free(node);
    }
}

void lru_cache_free(LRUCache* cache, void (*free_value)(void*)) {
    g_hash_table_destroy(cache->map); // This frees keys, but not nodes/values
    
    CacheNode* node = cache->head;
    while (node) {
        CacheNode* next = node->next;
        if (free_value && node->value) {
            free_value(node->value);
        }
        // node->key was freed by g_hash_table_destroy
        free(node);
        node = next;
    }
    free(cache);
}