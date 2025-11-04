#ifndef SS_HANDLER_H
#define SS_HANDLER_H

// Thread arg for new connections
typedef struct {
    int sock_fd;
    char ip_addr[16];
} ConnectionArg;

// Main dispatcher thread for new connections
void* handle_connection(void* arg);

// Session loop for a Client
void handle_client_session(int client_sock, const char* client_ip);

// Session loop for the Name Server
void handle_ns_session(int ns_sock, const char* ns_ip);

#endif // SS_HANDLER_H