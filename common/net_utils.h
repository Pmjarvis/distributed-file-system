#ifndef NET_UTILS_H
#define NET_UTILS_H

#include "protocol.h"
#include <sys/socket.h>

// Sends a complete message (header + payload)
int send_response(int sock, MsgType type, const void* payload, uint32_t len);

// --- Client-facing responses (for NS to use) ---
int send_error_response_to_client(int sock, const char* msg);
int send_success_response_to_client(int sock, const char* msg);
int send_lock_error_to_client(int sock, const char* msg); // <-- FIX: ADDED THIS
int send_file_not_found_to_client(int sock, const char* msg);

// --- NS-facing responses (for SS to use) ---
int send_error_response_to_ns(int sock, const char* msg);
int send_success_response_to_ns(int sock, const char* msg);

// --- Receivers ---
int recv_header(int sock, MsgHeader* header);
int recv_payload(int sock, void* payload, uint32_t len);

// --- Socket setup ---
int setup_listener_socket(int port);  // Deprecated - binds to INADDR_ANY
int setup_listener_socket_on_ip(const char* ip, int port);  // Bind to specific IP
int connect_to_server(const char* ip, int port); // <-- FIX: ADDED THIS

#endif // NET_UTILS_H