#include "client_commands.h"
#include "client_net.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

// --- Login ---

bool do_login(int ns_sock, const char* username) {
    Req_Login req;
    memset(&req, 0, sizeof(req));
    strncpy(req.username, username, MAX_USERNAME - 1);
    
    if (send_request(ns_sock, MSG_C2N_LOGIN, &req, sizeof(req)) < 0) {
        return false;
    }
    
    MsgHeader header;
    if (recv_header(ns_sock, &header) <= 0) {
        return false;
    }
    
    if (header.type == MSG_N2C_LOGIN_OK) {
        return true;
    }
    
    if (header.type == MSG_N2C_LOGIN_FAIL) {
        Res_Error res;
        recv_payload(ns_sock, &res, header.payload_len);
        fprintf(stderr, "Login Failed: %s\n", res.msg);
        return false;
    }
    
    return false;
}

// --- NS-Only Commands ---

void do_view(char* args) {
    Req_View req;
    memset(&req, 0, sizeof(req));
    if (args) {
        strncpy(req.flags, args, 3);
    }
    
    send_request(g_ns_sock, MSG_C2N_VIEW, &req, sizeof(req));
    
    MsgHeader header;
    if (recv_header(g_ns_sock, &header) <= 0) return;
    
    if (header.type == MSG_N2C_VIEW_RES) {
        Res_View res;
        memset(&res, 0, sizeof(res));
        recv_payload(g_ns_sock, &res, header.payload_len);
        printf("%s", res.data);
    } else {
        handle_generic_response(g_ns_sock, &header);
    }
}

void do_create(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: CREATE <filename>\n");
        return;
    }
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(g_ns_sock, MSG_C2N_CREATE, &req, sizeof(req));
    handle_generic_response(g_ns_sock, NULL);
}

void do_delete(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: DELETE <filename>\n");
        return;
    }
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(g_ns_sock, MSG_C2N_DELETE, &req, sizeof(req));
    handle_generic_response(g_ns_sock, NULL);
}

void do_info(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: INFO <filename>\n");
        return;
    }
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(g_ns_sock, MSG_C2N_INFO, &req, sizeof(req));
    
    MsgHeader header;
    if (recv_header(g_ns_sock, &header) <= 0) return;
    
    if (header.type == MSG_N2C_INFO_RES) {
        Res_Info res;
        memset(&res, 0, sizeof(res));
        recv_payload(g_ns_sock, &res, header.payload_len);
        printf("%s", res.data);
    } else {
        handle_generic_response(g_ns_sock, &header);
    }
}

void do_list_users(char* args) {
    send_request(g_ns_sock, MSG_C2N_LIST_USERS, NULL, 0);
    
    MsgHeader header;
    if (recv_header(g_ns_sock, &header) <= 0) return;
    
    if (header.type == MSG_N2C_LIST_USERS_RES) {
        Res_ListUsers res;
        memset(&res, 0, sizeof(res));
        recv_payload(g_ns_sock, &res, header.payload_len);
        printf("%s", res.data);
    } else {
        handle_generic_response(g_ns_sock, &header);
    }
}

void do_access(char* args, MsgType type) {
    Req_Access req;
    memset(&req, 0, sizeof(req));
    
    char* saveptr;
    char* flag = strtok_r(args, " \t\n", &saveptr);
    char* filename = strtok_r(NULL, " \t\n", &saveptr);
    char* username = strtok_r(NULL, " \t\n", &saveptr);
    
    if (!flag || !filename || !username) {
        if (type == MSG_C2N_ACCESS_ADD)
            fprintf(stderr, "Usage: ADDACCESS -R|-W <filename> <username>\n");
        else
            fprintf(stderr, "Usage: REMACCESS <filename> <username>\n");
        return;
    }
    
    if (type == MSG_C2N_ACCESS_ADD) {
        if (strcmp(flag, "-R") == 0) req.perm_flag = 'R';
        else if (strcmp(flag, "-W") == 0) req.perm_flag = 'W';
        else {
             fprintf(stderr, "Usage: ADDACCESS -R|-W <filename> <username>\n");
             return;
        }
    } else {
        // For REMACCESS, the 'flag' is actually the filename
        username = filename;
        filename = flag;
    }

    strncpy(req.filename, filename, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    strncpy(req.target_user, username, MAX_USERNAME - 1);
    
    send_request(g_ns_sock, type, &req, sizeof(req));
    handle_generic_response(g_ns_sock, NULL);
}

void do_exec(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: EXEC <filename>\n");
        return;
    }
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(g_ns_sock, MSG_C2N_EXEC_REQ, &req, sizeof(req));
    
    MsgHeader header;
    if (recv_header(g_ns_sock, &header) <= 0) return;
    
    if (header.type == MSG_N2C_EXEC_RES) {
        Res_Exec res;
        memset(&res, 0, sizeof(res));
        recv_payload(g_ns_sock, &res, header.payload_len);
        printf("--- Executing %s ---\n%s\n--- End of Exec ---\n", args, res.output);
    } else {
        handle_generic_response(g_ns_sock, &header);
    }
}

// --- Folder Commands (NS-Only) ---

void do_folder_cmd(char* args, const char* command) {
    Req_Folder req;
    memset(&req, 0, sizeof(req));
    strncpy(req.command, command, 31);
    
    char* saveptr;
    char* arg1 = strtok_r(args, " ", &saveptr);
    char* arg2 = strtok_r(NULL, "", &saveptr);
    
    if (arg1) strncpy(req.arg1, arg1, MAX_PATH - 1);
    if (arg2) strncpy(req.arg2, arg2, MAX_PATH - 1);
    
    // VIEWFOLDER and OPENPARENT don't require arguments
    if (!arg1 && (strcmp(command, "OPENPARENT") != 0) && (strcmp(command, "VIEWFOLDER") != 0)) {
        fprintf(stderr, "Usage: %s <arg1> [arg2]\n", command);
        return;
    }
    
    send_request(g_ns_sock, MSG_C2N_FOLDER_CMD, &req, sizeof(req));
    
    // VIEWFOLDER returns MSG_N2C_VIEW_RES, not generic response
    if (strcmp(command, "VIEWFOLDER") == 0) {
        MsgHeader header;
        if (recv_header(g_ns_sock, &header) <= 0) {
            fprintf(stderr, "ERROR: Connection lost.\n");
            return;
        }
        
        if (header.type == MSG_N2C_VIEW_RES) {
            Res_View res;
            recv_payload(g_ns_sock, &res, header.payload_len);
            printf("%s", res.data);
        } else if (header.type == MSG_N2C_GENERIC_FAIL) {
            Res_Error res;
            recv_payload(g_ns_sock, &res, header.payload_len);
            fprintf(stderr, "ERROR: %s\n", res.msg);
        } else {
            fprintf(stderr, "ERROR: Unexpected response type %d\n", header.type);
        }
    } else {
        handle_generic_response(g_ns_sock, NULL);
    }
}

void do_open_folder(char* args) {
    Req_Folder req;
    memset(&req, 0, sizeof(req));
    strncpy(req.command, "OPEN", 31);

    char* saveptr;
    char* arg1 = strtok_r(args, " ", &saveptr);
    char* arg2 = strtok_r(NULL, "", &saveptr);
    
    if (arg1 && strcmp(arg1, "-c") == 0) {
        strncpy(req.flags, "-c", 3);
        if (arg2) strncpy(req.arg1, arg2, MAX_PATH - 1);
    } else if (arg1) {
        strncpy(req.arg1, arg1, MAX_PATH - 1);
        if (arg2 && strcmp(arg2, "-c") == 0) {
            strncpy(req.flags, "-c", 3);
        }
    }

    if (strlen(req.arg1) == 0) {
        fprintf(stderr, "Usage: OPEN [-c] <foldername>\n");
        return;
    }

    send_request(g_ns_sock, MSG_C2N_FOLDER_CMD, &req, sizeof(req));
    handle_generic_response(g_ns_sock, NULL);
}


// --- NS -> SS Redirect Commands ---

void do_read(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: READ <filename>\n");
        return;
    }
    
    int ss_sock = get_ss_connection(args, MSG_C2N_READ_REQ);
    if (ss_sock < 0) return;
    
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(ss_sock, MSG_C2S_READ, &req, sizeof(req));
    
    fprintf(stderr, "[DEBUG] Waiting for READ response...\n");
    while(1) {
        MsgHeader header;
        fprintf(stderr, "[DEBUG] About to recv header...\n");
        int recv_result = recv_header(ss_sock, &header);
        fprintf(stderr, "[DEBUG] recv_header returned %d, type=%d\n", recv_result, header.type);
        if (recv_result <= 0) break;
        
        if (header.type == MSG_S2C_READ_CONTENT) {
            Res_FileContent chunk;
            // Payload is *not* a string, it can be binary data
            memset(&chunk, 0, sizeof(chunk));
            fprintf(stderr, "[DEBUG] Receiving payload of size %zu...\n", header.payload_len);
            recv_payload(ss_sock, &chunk, header.payload_len);
            fprintf(stderr, "[DEBUG] Got chunk: data_len=%zu, is_final=%d\n", chunk.data_len, chunk.is_final_chunk);
            
            // Use the data_len field to know how much actual data to print
            if (chunk.data_len > 0 && chunk.data_len <= MAX_PAYLOAD) {
                fwrite(chunk.data, 1, chunk.data_len, stdout);
            }
            
            if (chunk.is_final_chunk) {
                fprintf(stderr, "[DEBUG] Final chunk received, breaking\n");
                break;
            }
        } else {
            handle_generic_response(ss_sock, &header);
            break;
        }
    }
    printf("\n");
    close(ss_sock);
}

void do_stream(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: STREAM <filename>\n");
        return;
    }

    int ss_sock = get_ss_connection(args, MSG_C2N_STREAM_REQ);
    if (ss_sock < 0) return;

    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(ss_sock, MSG_C2S_STREAM, &req, sizeof(req));
    
    while(1) {
        MsgHeader header;
        if (recv_header(ss_sock, &header) <= 0) break;
        
        if (header.type == MSG_S2C_STREAM_WORD) {
            Res_Stream word;
            memset(&word, 0, sizeof(word));
            recv_payload(ss_sock, &word, header.payload_len);
            printf("%s ", word.word);
            fflush(stdout);
        } else if (header.type == MSG_S2C_STREAM_END) {
            break;
        } else {
            handle_generic_response(ss_sock, &header);
            break;
        }
    }
    printf("\n");
    close(ss_sock);
}

void do_write(char* args) {
    char* saveptr;
    char* filename = strtok_r(args, " ", &saveptr);
    char* sent_num_str = strtok_r(NULL, "", &saveptr);
    
    if (!filename || !sent_num_str) {
        fprintf(stderr, "Usage: WRITE <filename> <sentence_number>\n");
        return;
    }

    int ss_sock = get_ss_connection(filename, MSG_C2N_WRITE_REQ);
    if (ss_sock < 0) return;

    // 1. Send transaction start request
    Req_Write_Transaction req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, filename, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    req.sentence_num = atoi(sent_num_str);
    
    send_request(ss_sock, MSG_C2S_WRITE, &req, sizeof(req));

    // 2. Wait for OK (not locked) or FAIL (locked)
    MsgHeader header;
    if (recv_header(ss_sock, &header) <= 0) {
        close(ss_sock);
        return;
    }
    if (header.type == MSG_S2C_WRITE_LOCKED || header.type == MSG_S2C_GENERIC_FAIL) {
        handle_generic_response(ss_sock, &header);
        close(ss_sock);
        return;
    }
    // Assume OK
    
    printf("Entering write mode for '%s' (sentence %d).\n", filename, req.sentence_num);
    printf("Enter '<word_index> <content>' or 'ETIRW' to save and exit.\n");

    char line_buf[MAX_PAYLOAD + 100];
    char* w_saveptr;
    
    // 3. Enter write loop
    while(1) {
        printf("w> ");
        fflush(stdout);
        
        // Clear any error state on stdin
        if (feof(stdin)) {
            fprintf(stderr, "[DEBUG] stdin is at EOF before fgets\n");
            clearerr(stdin);
        }
        if (ferror(stdin)) {
            fprintf(stderr, "[DEBUG] stdin has error before fgets\n");
            clearerr(stdin);
        }
        
        if (!fgets(line_buf, sizeof(line_buf), stdin)) {
            // EOF or error - send ETIRW to close properly
            if (feof(stdin)) {
                fprintf(stderr, "\n[DEBUG] fgets returned NULL: stdin is at EOF\n");
            } else if (ferror(stdin)) {
                fprintf(stderr, "\n[DEBUG] fgets returned NULL: stdin has error\n");
                perror("fgets error");
            } else {
                fprintf(stderr, "\n[DEBUG] fgets returned NULL for unknown reason\n");
            }
            fprintf(stderr, "Exiting write mode without saving.\n");
            send_request(ss_sock, MSG_C2S_WRITE_ETIRW, NULL, 0);
            break;
        }
        
        // Remove newline
        size_t len = strlen(line_buf);
        if (len > 0 && line_buf[len-1] == '\n') {
            line_buf[len-1] = '\0';
        }
        
        // Skip empty lines
        if (strlen(line_buf) == 0) {
            continue;
        }
        
        if (strcmp(line_buf, "ETIRW") == 0) {
            send_request(ss_sock, MSG_C2S_WRITE_ETIRW, NULL, 0);
            break;
        }
        
        char* idx_str = strtok_r(line_buf, " ", &w_saveptr);
        char* content = strtok_r(NULL, "", &w_saveptr);
        
        if (!idx_str || !content) {
            fprintf(stderr, "Invalid format. Use: <word_index> <content>\n");
            continue;
        }
        
        Req_Write_Data data;
        memset(&data, 0, sizeof(data));
        data.word_index = atoi(idx_str);
        strncpy(data.content, content, MAX_PAYLOAD - 5);
        
        send_request(ss_sock, MSG_C2S_WRITE_DATA, &data, sizeof(data));
    }
    
    // 4. Wait for final confirmation
    handle_generic_response(ss_sock, NULL);
    close(ss_sock);
}

void do_undo(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: UNDO <filename>\n");
        return;
    }

    int ss_sock = get_ss_connection(args, MSG_C2N_UNDO_REQ);
    if (ss_sock < 0) return;
    
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(ss_sock, MSG_C2S_UNDO, &req, sizeof(req));
    
    handle_generic_response(ss_sock, NULL);
    close(ss_sock);
}

void do_checkpoint_cmd(char* args, const char* command) {
    Req_Checkpoint req;
    memset(&req, 0, sizeof(req));
    strncpy(req.command, command, 31);
    
    char* saveptr;
    char* filename = strtok_r(args, " ", &saveptr);
    char* tag = strtok_r(NULL, "", &saveptr);
    
    if (!filename) {
        fprintf(stderr, "Usage: %s <filename> [tag]\n", command);
        return;
    }
    if (!tag && (strcmp(command, "LISTCHECKPOINTS") != 0)) {
         fprintf(stderr, "Usage: %s <filename> <tag>\n", command);
         return;
    }
    
    strncpy(req.filename, filename, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    if (tag) strncpy(req.tag, tag, MAX_TAG - 1);
    
    int ss_sock = get_ss_connection(filename, MSG_C2N_CHECKPOINT_REQ);
    if (ss_sock < 0) return;
    
    send_request(ss_sock, MSG_C2S_CHECKPOINT_OP, &req, sizeof(req));
    
    // Handle responses - LIST and VIEW commands return data
    if (strcmp(command, "LISTCHECKPOINTS") == 0) {
        MsgHeader header;
        if (recv_header(ss_sock, &header) <= 0) {
            fprintf(stderr, "Error: Failed to receive response from storage server.\n");
        } else if (header.type == MSG_N2C_VIEW_RES) {
            Res_View res;
            memset(&res, 0, sizeof(res));
            recv_payload(ss_sock, &res, header.payload_len);
            printf("%s", res.data);
        } else {
            handle_generic_response(ss_sock, &header);
        }
    } else if (strcmp(command, "VIEWCHECKPOINT") == 0) {
        // VIEWCHECKPOINT returns file content like READ
        while(1) {
            MsgHeader header;
            if (recv_header(ss_sock, &header) <= 0) break;
            
            if (header.type == MSG_S2C_READ_CONTENT) {
                Res_FileContent chunk;
                memset(&chunk, 0, sizeof(chunk));
                recv_payload(ss_sock, &chunk, header.payload_len);
                // Use server-provided chunk.data_len instead of guessing from payload_len
                size_t data_len = chunk.data_len;
                if (data_len > MAX_PAYLOAD) data_len = MAX_PAYLOAD; // Safety clamp
                fwrite(chunk.data, 1, data_len, stdout);
                
                if (chunk.is_final_chunk) break;
            } else {
                handle_generic_response(ss_sock, &header);
                break;
            }
        }
        printf("\n");
    } else {
        // CHECKPOINT and REVERT return success/error
        handle_generic_response(ss_sock, NULL);
    }
    
    close(ss_sock);
}

// --- Access Request Commands (NS-Only) ---

void do_request_access(char* args) {
    if (strlen(args) == 0) {
        fprintf(stderr, "Usage: REQACCESS <filename>\n");
        return;
    }
    Req_FileOp req;
    memset(&req, 0, sizeof(req));
    strncpy(req.filename, args, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    
    send_request(g_ns_sock, MSG_C2N_REQ_ACCESS, &req, sizeof(req));
    handle_generic_response(g_ns_sock, NULL);
}

void do_view_requests(char* args) {
    send_request(g_ns_sock, MSG_C2N_VIEW_REQ_ACCESS, NULL, 0);
    
    MsgHeader header;
    if (recv_header(g_ns_sock, &header) <= 0) return;
    
    if (header.type == MSG_N2C_VIEW_REQ_ACCESS_RES) {
        Res_View res;
        memset(&res, 0, sizeof(res));
        recv_payload(g_ns_sock, &res, header.payload_len);
        printf("--- Pending Access Requests for Your Files ---\n");
        printf("%s", res.data);
        printf("----------------------------------------------\n");
    } else {
        handle_generic_response(g_ns_sock, &header);
    }
}

void do_grant_access(char* args) {
    Req_Access req;
    memset(&req, 0, sizeof(req));
    
    char* saveptr;
    char* flag = strtok_r(args, " ", &saveptr);
    char* filename = strtok_r(NULL, " ", &saveptr);
    char* username = strtok_r(NULL, "", &saveptr);
    
    if (!flag || !filename || !username) {
        fprintf(stderr, "Usage: GRANTACCESS -R|-W <filename> <username>\n");
        return;
    }
    
    if (strcmp(flag, "-R") == 0) req.perm_flag = 'R';
    else if (strcmp(flag, "-W") == 0) req.perm_flag = 'W';
    else {
         fprintf(stderr, "Usage: GRANTACCESS -R|-W <filename> <username>\n");
         return;
    }

    strncpy(req.filename, filename, MAX_FILENAME - 1);
    req.filename[MAX_FILENAME - 1] = '\0';
    strncpy(req.target_user, username, MAX_USERNAME - 1);
    
    send_request(g_ns_sock, MSG_C2N_GRANT_REQ_ACCESS, &req, sizeof(req));
    handle_generic_response(g_ns_sock, NULL);
}