#include "ns_user_manager.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

UserList* load_user_list() {
    UserList* list = (UserList*)malloc(sizeof(UserList));
    list->capacity = 16;
    list->count = 0;
    list->users = (UserEntry*)malloc(list->capacity * sizeof(UserEntry));
    
    FILE* fp = fopen(USER_DB_FILE, "a+"); // "a+" creates if not exist, then can read
    if (!fp) {
        perror("Failed to open user DB");
        return list; // Return empty list
    }
    
    fseek(fp, 0, SEEK_SET); // Go to start for reading
    
    char username[MAX_USERNAME];
    int active_status;
    while (fscanf(fp, "%s %d\n", username, &active_status) == 2) {
        if (list->count == list->capacity) {
            list->capacity *= 2;
            list->users = (UserEntry*)realloc(list->users, list->capacity * sizeof(UserEntry));
        }
        strncpy(list->users[list->count].username, username, MAX_USERNAME);
        list->users[list->count].is_active = false; // Always load as inactive
        list->count++;
    }
    
    fclose(fp);
    return list;
}

void save_user_list(UserList* list) {
    FILE* fp = fopen(USER_DB_FILE, "w");
    if (!fp) {
        perror("Failed to open user DB for writing");
        return;
    }
    
    for (int i = 0; i < list->count; i++) {
        fprintf(fp, "%s %d\n", list->users[i].username, list->users[i].is_active);
    }
    
    fclose(fp);
}

// --- REMOVED 'static' from this function ---
UserEntry* find_user(UserList* list, const char* username) {
    for (int i = 0; i < list->count; i++) {
        if (strcmp(list->users[i].username, username) == 0) {
            return &list->users[i];
        }
    }
    return NULL;
}

bool activate_user(UserList* list, const char* username) {
    UserEntry* user = find_user(list, username);
    
    if (user) {
        // User exists
        if (user->is_active) {
            return false; // Already active
        }
        user->is_active = true;
        return true;
    } else {
        // New user
        if (list->count == list->capacity) {
            list->capacity *= 2;
            list->users = (UserEntry*)realloc(list->users, list->capacity * sizeof(UserEntry));
        }
        strncpy(list->users[list->count].username, username, MAX_USERNAME);
        list->users[list->count].is_active = true;
        list->count++;
        return true;
    }
}

void set_user_active(UserList* list, const char* username, bool active) {
    UserEntry* user = find_user(list, username);
    if (user) {
        user->is_active = active;
    }
}

void free_user_list(UserList* list) {
    if (list) {
        free(list->users);
        free(list);
    }
}

char* get_all_users_string(UserList* list) {
    // Estimate buffer size
    size_t buf_size = list->count * (MAX_USERNAME + 12); // " (active)\n"
    if (buf_size < 256) buf_size = 256;
    
    char* buffer = (char*)malloc(buf_size);
    if (!buffer) return NULL;
    
    buffer[0] = '\0';
    size_t current_len = 0;
    
    for (int i = 0; i < list->count; i++) {
        char line[MAX_USERNAME + 16];
        int line_len = snprintf(line, sizeof(line), "-> %s (%s)\n", 
                                list->users[i].username, 
                                list->users[i].is_active ? "active" : "inactive");
        
        if (current_len + line_len + 1 > buf_size) {
            buf_size = (buf_size + line_len) * 2;
            char* new_buffer = (char*)realloc(buffer, buf_size);
            if (!new_buffer) { free(buffer); return NULL; }
            buffer = new_buffer;
        }
        
        strcat(buffer, line);
        current_len += line_len;
    }
    
    return buffer;
}