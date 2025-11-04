#include "client_net.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>

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
        fprintf(stderr, "Failed to connect to %s:%d: %s\n", ip, port, strerror(errno));
        close(sock);
        return -1;
    }

    return sock;
}

int send_request(int sock, MsgType type, const void* payload, uint32_t len) {
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

static int recv_all(int sock, void* buffer, size_t len) {
    char* buf = (char*)buffer;
    size_t bytes_received = 0;
    while (bytes_received < len) {
        ssize_t res = recv(sock, buf + bytes_received, len - bytes_received, 0);
        if (res < 0) {
            if (errno == EINTR) continue;
            perror("recv_all failed");
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

int handle_generic_response(int sock, MsgHeader* in_header) {
    MsgHeader header;
    int res_code;
    
    if (in_header) {
        header = *in_header; // Use pre-read header
    } else {
        res_code = recv_header(sock, &header);
        if (res_code <= 0) return -1; // Disconnect or error
    }

    if (header.type == MSG_N2C_GENERIC_OK || header.type == MSG_S2C_GENERIC_OK || header.type == MSG_S2C_WRITE_OK) {
        Res_Success res;
        if (header.payload_len > 0) {
            recv_payload(sock, &res, header.payload_len);
            printf("SUCCESS: %s\n", res.msg);
        } else {
            printf("SUCCESS\n");
        }
        return 0;
    } 
    else if (header.type == MSG_N2C_GENERIC_FAIL || header.type == MSG_S2C_GENERIC_FAIL || header.type == MSG_S2C_WRITE_LOCKED) {
        Res_Error res;
        if (header.payload_len > 0) {
            recv_payload(sock, &res, header.payload_len);
            fprintf(stderr, "ERROR: %s\n", res.msg);
        } else {
            fprintf(stderr, "ERROR: Received unspecified error from server.\n");
        }
        return -1;
    }
    else {
        fprintf(stderr, "ERROR: Received unexpected response type %d\n", header.type);
        // Clear the unexpected payload
        if (header.payload_len > 0) {
            char* junk = (char*)malloc(header.payload_len);
            if(junk) {
                recv_payload(sock, junk, header.payload_len);
                free(junk);
            }
        }
        return -1;
    }
}

int get_ss_connection(const char* filename, MsgType req_type) {
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, filename, MAX_PATH - 1);
    // NS will get username from its session, but we send it for
    // stateless check (though our NS is stateful)
    strncpy(req.username, g_username, MAX_USERNAME - 1);

    if (send_request(g_ns_sock, req_type, &req, sizeof(req)) < 0) {
        fprintf(stderr, "Failed to send request to Name Server\n");
        return -1;
    }

    MsgHeader header;
    if (recv_header(g_ns_sock, &header) <= 0) {
        fprintf(stderr, "Lost connection to Name Server\n");
        return -1;
    }

    if (header.type == MSG_N2C_SS_LOC) {
        Res_SSLocation loc;
        if (recv_payload(g_ns_sock, &loc, header.payload_len) <= 0) {
            fprintf(stderr, "Failed to get SS location from NS\n");
            return -1;
        }
        
        // Connect to the SS
        return connect_to_server(loc.ip, loc.port);
    } else {
        // It's an error (e.g., Access Denied, File Not Found)
        handle_generic_response(g_ns_sock, &header);
        return -1;
    }
}