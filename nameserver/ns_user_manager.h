#ifndef NS_USER_MANAGER_H
#define NS_USER_MANAGER_H

#include <stdbool.h>
#include "../common/protocol.h"

#define USER_DB_FILE "users.db"

typedef struct {
    char username[MAX_USERNAME];
    bool is_active;
} UserEntry;

typedef struct {
    UserEntry* users;
    int count;
    int capacity;
} UserList;

UserList* load_user_list();
void save_user_list(UserList* list);
// Tries to activate a user. Returns true on success.
// Returns false if user is already active.
// Creates user if they don't exist.
bool activate_user(UserList* list, const char* username);
void set_user_active(UserList* list, const char* username, bool active);
UserEntry* find_user(UserList* list, const char* username); // <--- ADD THIS LINE
void free_user_list(UserList* list);
// Returns a malloc'd string of all users. Caller must free.
char* get_all_users_string(UserList* list);

#endif // NS_USER_MANAGER_H