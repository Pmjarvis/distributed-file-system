#ifndef CLIENT_NET_H
#define CLIENT_NET_H

#include "../common/protocol.h"
#include <stdbool.h>

// Globals defined in client_main.c
extern int g_ns_sock;
extern char g_username[MAX_USERNAME];

/**
 * @brief Connects to a server.
 * @return Socket file descriptor, or -1 on failure.
 */
int connect_to_server(const char* ip, int port);

/**
 * @brief Sends a request (header + payload) to a socket.
 * @return 0 on success, -1 on failure.
 */
int send_request(int sock, MsgType type, const void* payload, uint32_t len);

/**
 * @brief Receives a message header.
 * @return 1 on success, 0 on disconnect, -1 on error.
 */
int recv_header(int sock, MsgHeader* header);

/**
 * @brief Receives a payload of a specific length.
 * @return 1 on success, 0 on disconnect, -1 on error.
 */
int recv_payload(int sock, void* payload, uint32_t len);

/**
 * @brief Handles a generic OK/FAIL response from a server.
 * If header is NULL, it will read one first.
 * @return 0 on OK, -1 on FAIL/error.
 */
int handle_generic_response(int sock, MsgHeader* header);

/**
 * @brief Helper function to get a connection to an SS for a file operation.
 * Contacts NS, gets location, and connects to SS.
 * @param filename The file to operate on.
 * @param req_type The *Name Server* request type (e.g., MSG_C2N_READ_REQ).
 * @return A new socket_fd to the SS, or -1 on failure.
 */
int get_ss_connection(const char* filename, MsgType req_type);

#endif // CLIENT_NET_H