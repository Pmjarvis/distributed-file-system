#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "client_net.h"
#include "client_commands.h"
#include "../common/protocol.h"

// --- Global Definitions ---
int g_ns_sock = -1;
char g_username[MAX_USERNAME];
// ---

static void print_prompt() {
    printf("[%s@nfs]$ ", g_username);
    fflush(stdout);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <ns_ip> <ns_port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    
    printf("Enter username: ");
    if (!fgets(g_username, MAX_USERNAME, stdin)) {
        exit(EXIT_FAILURE);
    }
    g_username[strcspn(g_username, "\n")] = 0; // Remove newline

    g_ns_sock = connect_to_server(argv[1], atoi(argv[2]));
    if (g_ns_sock < 0) {
        exit(EXIT_FAILURE);
    }
    
    if (!do_login(g_ns_sock, g_username)) {
        fprintf(stderr, "Login failed. Exiting.\n");
        close(g_ns_sock);
        exit(EXIT_FAILURE);
    }
    
    printf("Login successful. Welcome to the NFS. Type 'help' for commands.\n");
    
    char line_buf[MAX_PAYLOAD + 512];
    
    while(1) {
        print_prompt();
        if (!fgets(line_buf, sizeof(line_buf), stdin)) {
            break; // EOF (Ctrl+D)
        }
        
        line_buf[strcspn(line_buf, "\n")] = 0;
        if (strlen(line_buf) == 0) {
            continue;
        }

        // --- Command Parsing ---
        char* saveptr;
        char* command = strtok_r(line_buf, " ", &saveptr);
        char* args = strtok_r(NULL, "", &saveptr);
        if (!args) args = ""; // Ensure args is never NULL
        
        // --- Command Dispatcher ---
        if (strcmp(command, "VIEW") == 0) {
            do_view(args);
        } else if (strcmp(command, "READ") == 0) {
            do_read(args);
        } else if (strcmp(command, "CREATE") == 0) {
            do_create(args);
        } else if (strcmp(command, "WRITE") == 0) {
            do_write(args);
        } else if (strcmp(command, "UNDO") == 0) {
            do_undo(args);
        } else if (strcmp(command, "INFO") == 0) {
            do_info(args);
        } else if (strcmp(command, "DELETE") == 0) {
            do_delete(args);
        } else if (strcmp(command, "STREAM") == 0) {
            do_stream(args);
        } else if (strcmp(command, "LIST") == 0) {
            do_list_users(args);
        } else if (strcmp(command, "ADDACCESS") == 0) {
            do_access(args, MSG_C2N_ACCESS_ADD);
        } else if (strcmp(command, "REMACCESS") == 0) {
            do_access(args, MSG_C2N_ACCESS_REM);
        } else if (strcmp(command, "EXEC") == 0) {
            do_exec(args);
        } else if (strcmp(command, "CREATEFOLDER") == 0) {
            do_folder_cmd(args, "CREATEFOLDER");
        } else if (strcmp(command, "VIEWFOLDER") == 0) {
            do_folder_cmd(args, "VIEWFOLDER");
        } else if (strcmp(command, "MOVE") == 0) {
            do_folder_cmd(args, "MOVE");
        } else if (strcmp(command, "UPMOVE") == 0) {
            do_folder_cmd(args, "UPMOVE");
        } else if (strcmp(command, "OPEN") == 0) {
            do_open_folder(args);
        } else if (strcmp(command, "OPENPARENT") == 0) {
            do_folder_cmd(args, "OPENPARENT");
        } else if (strcmp(command, "CHECKPOINT") == 0) {
            do_checkpoint_cmd(args, "CHECKPOINT");
        } else if (strcmp(command, "VIEWCHECKPOINT") == 0) {
            do_checkpoint_cmd(args, "VIEWCHECKPOINT");
        } else if (strcmp(command, "REVERT") == 0) {
            do_checkpoint_cmd(args, "REVERT");
        } else if (strcmp(command, "LISTCHECKPOINTS") == 0) {
            do_checkpoint_cmd(args, "LISTCHECKPOINTS");
        } else if (strcmp(command, "REQACCESS") == 0) {
            do_request_access(args);
        } else if (strcmp(command, "VIEWREQS") == 0) {
            do_view_requests(args);
        } else if (strcmp(command, "GRANTACCESS") == 0) {
            do_grant_access(args);
        }
        else if (strcmp(command, "help") == 0) {
            printf("Available Commands:\n");
            printf("  VIEW [-a|-l|-al]\n");
            printf("  READ <file>          STREAM <file>\n");
            printf("  CREATE <file>        DELETE <file>        INFO <file>\n");
            printf("  WRITE <file> <sent_#>  UNDO <file>          EXEC <file>\n");
            printf("  LIST\n");
            printf("  ADDACCESS -R|-W <file> <user>    REMACCESS <file> <user>\n");
            printf("  REQACCESS <file>    VIEWREQS    GRANTACCESS -R|-W <file> <user>\n");
            printf("  CREATEFOLDER <dir>  VIEWFOLDER\n");
            printf("  OPEN [-c] <dir>     OPENPARENT\n");
            printf("  MOVE <file> <dir>   UPMOVE <file>\n");
            printf("  CHECKPOINT <file> <tag>    REVERT <file> <tag>\n");
            printf("  VIEWCHECKPOINT <file> <tag>  LISTCHECKPOINTS <file>\n");
            printf("  exit\n");
        }
        else if (strcmp(command, "exit") == 0) {
            break;
        } else {
            fprintf(stderr, "Unknown command: %s. Type 'help' for a list.\n", command);
        }
    }
    
    close(g_ns_sock);
    printf("Goodbye.\n");
    return 0;
}