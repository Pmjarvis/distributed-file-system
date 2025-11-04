#ifndef CLIENT_COMMANDS_H
#define CLIENT_COMMANDS_H

#include "../common/protocol.h"
#include <stdbool.h>

/**
 * @brief Attempts to log the user in to the Name Server.
 * @return true on success, false on failure.
 */
bool do_login(int ns_sock, const char* username);

// --- Command Functions ---

void do_view(char* args);
void do_read(char* args);
void do_create(char* args);
void do_write(char* args);
void do_undo(char* args);
void do_info(char* args);
void do_delete(char* args);
void do_stream(char* args);
void do_list_users(char* args);
void do_access(char* args, MsgType type); // For ADDACCESS, REMACCESS
void do_exec(char* args);

// Folder commands
void do_folder_cmd(char* args, const char* command);
void do_open_folder(char* args); // Special case for -c flag

// Checkpoint commands
void do_checkpoint_cmd(char* args, const char* command);

// Access request commands
void do_request_access(char* args);
void do_view_requests(char* args);
void do_grant_access(char* args);


#endif // CLIENT_COMMANDS_H