#ifndef SS_FILE_MANAGER_H
#define SS_FILE_MANAGER_H

#include "../common/protocol.h"
#include <dirent.h>

// --- Path Utilities ---
void ss_get_path(const char* dir, const char* filename, char* out_path);
void ss_create_dirs();

// --- Metadata Utilities ---
int ss_get_file_metadata(const char* filename, FileMetadata* meta);

// --- File Reading ---
char* ss_read_file_to_memory(const char* filepath, long* file_size);

// --- Sentence/Word Parsing ---
// Splits text into sentences. Returns array, sets count. Caller frees.
char** ss_split_sentences(const char* text, int* num_sentences);
void ss_free_sentences(char** sentences, int num_sentences);

// Splits a sentence string into words. Returns array, sets count. Caller frees.
char** ss_split_words(const char* sentence, int* num_words);
void ss_free_words(char** words, int num_words);

// Joins words into a single sentence string. Caller frees.
char* ss_join_words(char** words, int num_words);

// Joins sentences into a single file string. Caller frees.
char* ss_join_sentences(char** sentences, int num_sentences);

// --- NS Request Handlers ---
void ss_handle_create_file(int ns_sock, Req_FileOp* req);
void ss_handle_delete_file(int ns_sock, Req_FileOp* req);
void ss_handle_get_info(int ns_sock, Req_FileOp* req);
void ss_handle_get_content_for_exec(int ns_sock, Req_FileOp* req);

// --- Client Request Handlers ---
void ss_handle_read(int client_sock, Req_FileOp* req);
void ss_handle_stream(int client_sock, Req_FileOp* req);
void ss_handle_undo(int client_sock, Req_FileOp* req);
void ss_handle_checkpoint(int client_sock, Req_Checkpoint* req);

// --- Complex Write Transaction ---
// This function handles the *entire* write transaction
void ss_handle_write_transaction(int client_sock, Req_Write_Transaction* req);

// --- Directory Scanner ---
int ss_scan_files(FileMetadata** file_list); // Returns count

// --- Cleanup ---
void ss_clean_swap_dir();

#endif // SS_FILE_MANAGER_H