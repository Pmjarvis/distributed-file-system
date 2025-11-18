#ifndef SS_HANDLER_H
#define SS_HANDLER_H

#include "../common/protocol.h"

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

// Recovery sync handlers (called from main handler)
void ss_handle_sync_from_backup(int sock, Req_SyncFromBackup* req);
void ss_handle_sync_to_primary(int sock, Req_SyncToPrimary* req);
void ss_handle_re_replicate_all(int sock, Req_ReReplicate* req);
void ss_handle_recovery_connection(int sock, Req_StartRecovery* req);
void ss_handle_update_backup(int sock, Req_UpdateBackup* req);

#endif // SS_HANDLER_H