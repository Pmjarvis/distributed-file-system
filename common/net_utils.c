#include "net_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h> // <-- FIX: Added this include

int send_response(int sock, MsgType type, const void* payload, uint32_t len) {
    MsgHeader header;
    header.type = type;
    header.payload_len = len;

    // 1. Send header
    if (send(sock, &header, sizeof(MsgHeader), 0) != sizeof(MsgHeader)) {
        perror("send header failed");
        return -1;
    }

    // 2. Send payload (if any)
    if (len > 0 && payload != NULL) {
        if (send(sock, payload, len, 0) != len) {
            perror("send payload failed");
            return -1;
        }
    }
    return 0;
}

// --- Client-facing ---
int send_error_response_to_client(int sock, const char* msg) {
    Res_Error err;
    strncpy(err.msg, msg, MAX_PAYLOAD - 1);
    err.msg[MAX_PAYLOAD - 1] = '\0';
    // This was MSG_N2C_GENERIC_FAIL, but SS should send SS codes
    return send_response(sock, MSG_S2C_GENERIC_FAIL, &err, sizeof(err));
}

int send_success_response_to_client(int sock, const char* msg) {
    Res_Success ok;
    strncpy(ok.msg, msg, MAX_PAYLOAD - 1);
    ok.msg[MAX_PAYLOAD - 1] = '\0';
    // This was MSG_N2C_GENERIC_OK, but SS should send SS codes
    return send_response(sock, MSG_S2C_GENERIC_OK, &ok, sizeof(ok));
}

// --- FIX: ADDED THIS FUNCTION ---
int send_lock_error_to_client(int sock, const char* msg) {
    Res_Error err;
    strncpy(err.msg, msg, MAX_PAYLOAD - 1);
    err.msg[MAX_PAYLOAD - 1] = '\0';
    return send_response(sock, MSG_S2C_WRITE_LOCKED, &err, sizeof(err));
}

// --- NS-facing ---
int send_error_response_to_ns(int sock, const char* msg) {
    Res_Error err;
    strncpy(err.msg, msg, MAX_PAYLOAD - 1);
    err.msg[MAX_PAYLOAD - 1] = '\0';
    return send_response(sock, MSG_S2N_ACK_FAIL, &err, sizeof(err));
}

int send_success_response_to_ns(int sock, const char* msg) {
    Res_Success ok;
    strncpy(ok.msg, msg, MAX_PAYLOAD - 1);
    ok.msg[MAX_PAYLOAD - 1] = '\0';
    return send_response(sock, MSG_S2N_ACK_OK, &ok, sizeof(ok));
}


static int recv_all(int sock, void* buffer, size_t len) {
    char* buf = (char*)buffer;
    size_t bytes_received = 0;
    while (bytes_received < len) {
        ssize_t res = recv(sock, buf + bytes_received, len - bytes_received, 0);
        if (res < 0) {
            if (errno == EINTR) continue;
            perror("recv failed");
            return -1;
        }
        if (res == 0) {
            fprintf(stderr, "Connection closed by peer\n");
            return 0; // Connection closed
        }
        bytes_received += res;
    }
    return 1; // Success
}

int recv_header(int sock, MsgHeader* header) {
    return recv_all(sock, header, sizeof(MsgHeader));
}

int recv_payload(int sock, void* payload, uint32_t len) {
    if (len == 0) return 1;
    return recv_all(sock, payload, len);
}

int setup_listener_socket(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }
    
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }
    
    if (listen(server_fd, 10) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }
    
    return server_fd;
}

// --- FIX: ADDED THIS FUNCTION ---
int connect_to_server(const char* ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Failed to create socket");
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid server IP address\n");
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        // This print is more user-friendly than perror
        fprintf(stderr, "Failed to connect to %s:%d: %s\n", ip, port, strerror(errno));
        close(sock);
        return -1;
    }

    return sock;
}