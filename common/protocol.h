#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#define NS_PORT 8080         // Name Server listening port for Clients
#define NS_SS_PORT 8081      // Name Server listening port for Storage Servers
#define MAX_PAYLOAD 4096
#define MAX_USERNAME 64
#define MAX_FILENAME 256
#define MAX_PATH 1024
#define MAX_TAG 64

#define HEARTBEAT_INTERVAL 5  // Seconds
#define HEARTBEAT_TIMEOUT 15 // Seconds

// --- Message Types ---
typedef enum {
    // --- Client <-> Name Server ---
    MSG_C2N_LOGIN,           // Payload: Req_Login
    MSG_N2C_LOGIN_OK,        // Payload: none
    MSG_N2C_LOGIN_FAIL,      // Payload: Res_Error
    
    MSG_C2N_VIEW,            // Payload: Req_View
    MSG_N2C_VIEW_RES,        // Payload: Res_View
    
    MSG_C2N_CREATE,          // Payload: Req_FileOp
    MSG_C2N_DELETE,          // Payload: Req_FileOp
    MSG_C2N_INFO,            // Payload: Req_FileOp
    MSG_N2C_INFO_RES,        // Payload: Res_Info
    
    MSG_C2N_READ_REQ,        // Payload: Req_FileOp
    MSG_C2N_STREAM_REQ,      // Payload: Req_FileOp
    MSG_C2N_WRITE_REQ,       // Payload: Req_FileOp
    MSG_N2C_SS_LOC,          // Payload: Res_SSLocation
    
    MSG_C2N_UNDO_REQ,        // Payload: Req_FileOp
    MSG_C2N_CHECKPOINT_REQ,  // Payload: Req_Checkpoint
    
    MSG_C2N_LIST_USERS,      // Payload: none
    MSG_N2C_LIST_USERS_RES,  // Payload: Res_ListUsers
    
    MSG_C2N_ACCESS_ADD,      // Payload: Req_Access
    MSG_C2N_ACCESS_REM,      // Payload: Req_Access
    
    MSG_C2N_EXEC_REQ,        // Payload: Req_FileOp
    MSG_N2C_EXEC_RES,        // Payload: Res_Exec
    
    MSG_C2N_FOLDER_CMD,      // Payload: Req_Folder
    
    MSG_C2N_REQ_ACCESS,      // Payload: Req_FileOp
    MSG_N2C_REQ_ACCESS_OK,   // Payload: Res_Success
    MSG_C2N_VIEW_REQ_ACCESS, // Payload: none
    MSG_N2C_VIEW_REQ_ACCESS_RES, // Payload: Res_View
    MSG_C2N_GRANT_REQ_ACCESS, // Payload: Req_Access
    
    MSG_N2C_GENERIC_OK,      // Payload: Res_Success
    MSG_N2C_GENERIC_FAIL,    // Payload: Res_Error

    // --- Storage Server <-> Name Server ---
    MSG_S2N_REGISTER,        // Payload: Req_SSRegister
    MSG_N2S_REGISTER_ACK,    // Payload: Res_SSRegisterAck
    MSG_S2N_HEARTBEAT,       // Payload: none
    
    MSG_N2S_CREATE_FILE,     // Payload: Req_FileOp
    MSG_N2S_DELETE_FILE,     // Payload: Req_FileOp
    MSG_N2S_GET_INFO,        // Payload: Req_FileOp
    MSG_N2S_EXEC_GET_CONTENT,// Payload: Req_FileOp
    
    MSG_S2N_ACK_OK,          // Payload: Res_Success
    MSG_S2N_ACK_FAIL,        // Payload: Res_Error
    MSG_S2N_EXEC_CONTENT,    // Payload: Res_Exec
    MSG_S2N_FILE_INFO_RES,   // Payload: Res_Info
    
    // --- Client <-> Storage Server ---
    MSG_C2S_READ,            // Payload: Req_FileOp
    MSG_C2S_STREAM,          // Payload: Req_FileOp
    MSG_C2S_WRITE,           // Payload: Req_Write_Transaction (start)
    MSG_C2S_WRITE_DATA,      // Payload: Req_Write_Data
    MSG_C2S_WRITE_ETIRW,     // Payload: Req_FileOp (signals end of write)
    MSG_C2S_UNDO,            // Payload: Req_FileOp
    MSG_C2S_CHECKPOINT_OP,   // Payload: Req_Checkpoint
    
    MSG_S2C_READ_CONTENT,    // Payload: Res_FileContent
    MSG_S2C_STREAM_WORD,     // Payload: Res_Stream
    MSG_S2C_STREAM_END,      // Payload: none
    MSG_S2C_WRITE_LOCKED,    // Payload: Res_Error (sentence locked)
    MSG_S2C_WRITE_OK,        // Payload: Res_Success
    MSG_S2C_GENERIC_OK,      // Payload: Res_Success
    MSG_S2C_GENERIC_FAIL,    // Payload: Res_Error

    // --- SS <-> SS (Replication) ---
    MSG_S2S_REPLICATE_FILE,  // Payload: Req_Replicate
    MSG_S2S_DELETE_FILE,     // Payload: Req_FileOp
    MSG_S2S_ACK

} MsgType;


typedef struct {
    MsgType type;
    uint32_t payload_len;
} MsgHeader;


// --- File Metadata ---
typedef struct {
    char filename[MAX_FILENAME];
    uint64_t size_bytes;
    uint32_t word_count;
    uint32_t char_count;
    char owner[MAX_USERNAME];
    time_t last_access_time;
    time_t last_modified_time;
} FileMetadata;


// --- Payloads ---
typedef struct {
    char msg[MAX_PAYLOAD];
} Res_Error;

typedef struct {
    char msg[MAX_PAYLOAD];
} Res_Success;

// C2N: Login
typedef struct {
    char username[MAX_USERNAME];
} Req_Login;

// C2N: View
typedef struct {
    char flags[4]; // "-a", "-l", "-al", ""
} Req_View;

// C2N / N2S
typedef struct {
    char username[MAX_USERNAME]; 
    char filename[MAX_FILENAME];
} Req_FileOp;

// C2N: Access
typedef struct {
    char username[MAX_USERNAME];
    char filename[MAX_FILENAME];
    char target_user[MAX_USERNAME];
    char perm_flag; // 'R' or 'W'
} Req_Access;

// C2N: Folder
typedef struct {
    char command[32];   // "CREATEFOLDER", "VIEWFOLDER", "MOVE", etc.
    char arg1[MAX_PATH];
    char arg2[MAX_PATH];
    char flags[4];      // "-c"
} Req_Folder;

// C2S: Write
typedef struct {
    char filename[MAX_FILENAME];
    int sentence_num;
} Req_Write_Transaction;

typedef struct {
    int word_index;
    char content[MAX_PAYLOAD - 4];
} Req_Write_Data;

// S2C: Read
typedef struct {
    char data[MAX_PAYLOAD];
    bool is_final_chunk;
} Res_FileContent;

// S2C: Stream
typedef struct {
    char word[256];
} Res_Stream;

// C2N/C2S: Checkpoint
typedef struct {
    char command[32]; 
    char filename[MAX_FILENAME];
    char tag[MAX_TAG];
} Req_Checkpoint;

// N2C: SS Location
typedef struct {
    char ip[16];
    int port;
} Res_SSLocation;

// N2C: View
typedef struct {
    char data[MAX_PAYLOAD];
} Res_View; // <--- ADD THIS STRUCT

// N2C: List Users
typedef struct {
    char data[MAX_PAYLOAD];
} Res_ListUsers;

// N2C: Info
typedef struct {
    char data[MAX_PAYLOAD];
} Res_Info;

// N2C: Exec
typedef struct {
    char output[MAX_PAYLOAD];
} Res_Exec;


// S2N
typedef struct {
    char ip[16];
    int client_port;
    char backup_ip[16];
    int backup_port;
    uint32_t file_count;
    // Followed by file_count * sizeof(FileMetadata)
} Req_SSRegister;

typedef struct {
    int new_ss_id;
    bool must_recover; 
    int backup_of_ss_id;
} Res_SSRegisterAck;

// S2S
typedef struct {
    char filename[MAX_FILENAME];
    uint64_t file_size;
    // Followed by file_size bytes of data
} Req_Replicate;

#endif // PROTOCOL_H