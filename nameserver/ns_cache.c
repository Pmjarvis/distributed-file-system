#include "ns_cache.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// --- Hash Function ---
static unsigned long hash_string(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    return hash;
}

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
    if (!node) return NULL;
    node->key = strdup(key);
    node->value = value;
    node->prev = NULL;
    node->next = NULL;
    node->h_next = NULL;
    return node;
}

// --- Public API ---

LRUCache* lru_cache_create(int capacity) {
    LRUCache* cache = (LRUCache*)malloc(sizeof(LRUCache));
    if (!cache) return NULL;
    
    cache->capacity = capacity;
    cache->size = 0;
    // Use a prime number or just capacity * 2 for bucket size to reduce collisions
    cache->num_buckets = capacity * 2 + 1; 
    cache->buckets = (CacheNode**)calloc(cache->num_buckets, sizeof(CacheNode*));
    if (!cache->buckets) {
        free(cache);
        return NULL;
    }
    
    cache->head = NULL;
    cache->tail = NULL;
    return cache;
}

void lru_cache_put(LRUCache* cache, const char* key, void* value) {
    unsigned long hash = hash_string(key);
    int index = hash % cache->num_buckets;
    
    // Search for existing node
    CacheNode* curr = cache->buckets[index];
    while (curr) {
        if (strcmp(curr->key, key) == 0) {
            // Found: update value and move to front
            curr->value = value;
            _cache_detach_node(cache, curr);
            _cache_attach_to_front(cache, curr);
            return;
        }
        curr = curr->h_next;
    }
    
    // Not found: insert new node
    
    // Check capacity
    if (cache->size >= cache->capacity) {
        // Evict tail
        CacheNode* tail = cache->tail;
        if (tail) {
            // Remove from LRU list
            _cache_detach_node(cache, tail);
            
            // Remove from hash table
            unsigned long tail_hash = hash_string(tail->key);
            int tail_index = tail_hash % cache->num_buckets;
            
            CacheNode* t_curr = cache->buckets[tail_index];
            CacheNode* t_prev = NULL;
            while (t_curr) {
                if (t_curr == tail) {
                    if (t_prev) {
                        t_prev->h_next = t_curr->h_next;
                    } else {
                        cache->buckets[tail_index] = t_curr->h_next;
                    }
                    break;
                }
                t_prev = t_curr;
                t_curr = t_curr->h_next;
            }
            
            free(tail->key);
            free(tail);
            cache->size--;
        }
    }
    
    // Create new node
    CacheNode* node = _cache_create_node(key, value);
    if (!node) return;
    
    // Add to hash table
    node->h_next = cache->buckets[index];
    cache->buckets[index] = node;
    
    // Add to LRU list
    _cache_attach_to_front(cache, node);
    cache->size++;
}

void* lru_cache_get(LRUCache* cache, const char* key) {
    unsigned long hash = hash_string(key);
    int index = hash % cache->num_buckets;
    
    CacheNode* curr = cache->buckets[index];
    while (curr) {
        if (strcmp(curr->key, key) == 0) {
            // Found: move to front
            _cache_detach_node(cache, curr);
            _cache_attach_to_front(cache, curr);
            return curr->value;
        }
        curr = curr->h_next;
    }
    return NULL;
}

void lru_cache_remove(LRUCache* cache, const char* key) {
    unsigned long hash = hash_string(key);
    int index = hash % cache->num_buckets;
    
    CacheNode* curr = cache->buckets[index];
    CacheNode* prev = NULL;
    
    while (curr) {
        if (strcmp(curr->key, key) == 0) {
            // Found: remove from hash table
            if (prev) {
                prev->h_next = curr->h_next;
            } else {
                cache->buckets[index] = curr->h_next;
            }
            
            // Remove from LRU list
            _cache_detach_node(cache, curr);
            
            free(curr->key);
            free(curr);
            cache->size--;
            return;
        }
        prev = curr;
        curr = curr->h_next;
    }
}

void lru_cache_free(LRUCache* cache, void (*free_value)(void*)) {
    if (!cache) return;
    
    CacheNode* curr = cache->head;
    while (curr) {
        CacheNode* next = curr->next;
        if (free_value && curr->value) {
            free_value(curr->value);
        }
        free(curr->key);
        free(curr);
        curr = next;
    }
    
    free(cache->buckets);
    free(cache);
}
