#define _DEFAULT_SOURCE
#include "ss_file_manager.h"
#include "ss_globals.h"
#include "ss_logger.h"
#include "ss_replicator.h"
#include "../common/net_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <ctype.h>

// --- Path Utilities ---

void ss_get_path(const char* dir, const char* filename, char* out_path) {
    snprintf(out_path, MAX_PATH, "%s/%s", dir, filename);
}

void ss_create_dirs() {
    mkdir(SS_ROOT_DIR, 0755);
    mkdir(SS_FILES_DIR, 0755);
    mkdir(SS_UNDO_DIR, 0755);
    mkdir(SS_CHECKPOINT_DIR, 0755);
    mkdir(SS_SWAP_DIR, 0755);  // For WRITE operation swapfiles
}

static int _copy_file(const char* src_path, const char* dest_path) {
    int src_fd, dest_fd;
    char buf[4096];
    ssize_t nread;
    
    src_fd = open(src_path, O_RDONLY);
    if (src_fd < 0) return -1;
    
    dest_fd = open(dest_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dest_fd < 0) {
        close(src_fd);
        return -1;
    }
    
    while ((nread = read(src_fd, buf, sizeof(buf))) > 0) {
        if (write(dest_fd, buf, nread) != nread) {
            close(src_fd);
            close(dest_fd);
            return -1;
        }
    }
    
    close(src_fd);
    close(dest_fd);
    return (nread == 0) ? 0 : -1;
}


// --- Metadata Utilities ---

int ss_get_file_metadata(const char* filename, FileMetadata* meta) {
    // FIX: Don't use stat() as it updates access time on some systems
    // Use metadata from metadata table instead for VIEW -l
    
    FileMetadataNode* node = metadata_table_get(g_metadata_table, filename);
    if (!node) {
        // CRITICAL FIX: If file is not in metadata table, this is a BUG!
        // All tracked files MUST have metadata entries.
        // This can only happen if:
        // 1. File was placed on disk manually (outside CREATE/WRITE)
        // 2. Metadata table is corrupted
        // 3. There's a race condition bug
        
        ss_log("ERROR: File '%s' exists but has NO metadata entry! This should never happen for tracked files.", filename);
        ss_log("ERROR: Possible causes: manual file placement, corrupted metadata.db, or race condition bug");
        
        // Check if file actually exists on disk
        char filepath[MAX_PATH];
        ss_get_path(SS_FILES_DIR, filename, filepath);
        
        struct stat st;
        if (stat(filepath, &st) != 0) {
            // File doesn't exist on disk either
            ss_log("ERROR: File '%s' doesn't exist on disk", filename);
            return -1;
        }
        
        // File exists on disk but not in metadata table - this is an ERROR state
        // Return failure to signal that metadata is missing
        ss_log("ERROR: File '%s' exists on disk but missing from metadata table", filename);
        return -1;
    }
    
    // Use cached metadata - does NOT update access time
    strncpy(meta->filename, node->filename, MAX_FILENAME - 1);
    meta->filename[MAX_FILENAME - 1] = '\0';
    strncpy(meta->owner, node->owner, MAX_USERNAME - 1);
    meta->owner[MAX_USERNAME - 1] = '\0';
    meta->size_bytes = node->file_size;
    meta->word_count = node->word_count;
    meta->char_count = node->char_count;
    meta->last_access_time = node->last_access;
    meta->last_modified_time = node->last_modified;
    
    free(node);
    return 0;
}

int ss_scan_files(FileMetadata** file_list) {
    DIR* d = opendir(SS_FILES_DIR);
    if (!d) {
        ss_log("Failed to open files directory: %s", SS_FILES_DIR);
        *file_list = NULL;
        return 0;
    }
    
    struct dirent* dir;
    int count = 0;
    int capacity = 16;
    *file_list = (FileMetadata*)malloc(capacity * sizeof(FileMetadata));
    
    while ((dir = readdir(d)) != NULL) {
        if (dir->d_type == DT_REG) { // Regular file
            if (strcmp(dir->d_name, ".") == 0 || strcmp(dir->d_name, "..") == 0) continue;
            
            if (count == capacity) {
                capacity *= 2;
                *file_list = (FileMetadata*)realloc(*file_list, capacity * sizeof(FileMetadata));
            }
            
            if (ss_get_file_metadata(dir->d_name, &(*file_list)[count]) == 0) {
                count++;
            }
        }
    }
    closedir(d);
    return count;
}


// --- File Reading ---

char* ss_read_file_to_memory(const char* filepath, long* file_size) {
    FILE* f = fopen(filepath, "r");
    if (!f) return NULL;
    
    fseek(f, 0, SEEK_END);
    *file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    char* content = (char*)malloc(*file_size + 1);
    if (!content) {
        fclose(f);
        return NULL;
    }
    
    if (fread(content, 1, *file_size, f) != *file_size) {
        fclose(f);
        free(content);
        return NULL;
    }
    
    fclose(f);
    content[*file_size] = '\0';
    return content;
}

// --- Sentence/Word Parsing ---
// (Simplified: any '.', '!', '?' is a delimiter)

char** ss_split_sentences(const char* text, int* num_sentences) {
    int capacity = 16;
    char** sentences = (char**)malloc(capacity * sizeof(char*));
    *num_sentences = 0;
    
    const char* start = text;
    const char* ptr = text;
    
    while (*ptr) {
        if (*ptr == '.' || *ptr == '!' || *ptr == '?') {
            int len = (ptr - start) + 1;
            sentences[*num_sentences] = (char*)malloc(len + 1);
            strncpy(sentences[*num_sentences], start, len);
            sentences[*num_sentences][len] = '\0';
            
            (*num_sentences)++;
            if (*num_sentences == capacity) {
                capacity *= 2;
                sentences = (char**)realloc(sentences, capacity * sizeof(char*));
            }
            start = ptr + 1;
        }
        ptr++;
    }
    
    // Add any trailing text without a delimiter
    if (start < ptr && strlen(start) > 0) {
        // Trim leading whitespace from this 'sentence'
        while (*start && isspace(*start)) start++;
        
        if (strlen(start) > 0) {
            sentences[*num_sentences] = strdup(start);
            (*num_sentences)++;
        }
    }
    
    return sentences;
}

void ss_free_sentences(char** sentences, int num_sentences) {
    if (!sentences) return;
    for (int i = 0; i < num_sentences; i++) {
        free(sentences[i]);
    }
    free(sentences);
}

char** ss_split_words(const char* sentence, int* num_words) {
    // Split words treating delimiters (. ! ?) as separate words per spec
    int capacity = 16;
    char** words = (char**)malloc(capacity * sizeof(char*));
    *num_words = 0;
    
    const char* ptr = sentence;
    // FIX: Increase buffer size to handle large payloads and prevent overflow
    char word_buffer[MAX_PAYLOAD + 1]; 
    int word_len = 0;
    
    while (*ptr) {
        // Skip leading whitespace
        while (*ptr && isspace(*ptr)) ptr++;
        if (!*ptr) break;
        
        // Check if current char is a delimiter
        if (*ptr == '.' || *ptr == '!' || *ptr == '?') {
            // Delimiter is its own word
            if (*num_words == capacity) {
                capacity *= 2;
                words = (char**)realloc(words, capacity * sizeof(char*));
            }
            char delim[2] = {*ptr, '\0'};
            words[*num_words] = strdup(delim);
            (*num_words)++;
            ptr++;
            continue;
        }
        
        // Read a regular word until whitespace or delimiter
        word_len = 0;
        while (*ptr && !isspace(*ptr) && *ptr != '.' && *ptr != '!' && *ptr != '?') {
            if (word_len < MAX_PAYLOAD) {
                word_buffer[word_len++] = *ptr;
            }
            ptr++;
        }
        
        if (word_len > 0) {
            word_buffer[word_len] = '\0';
            if (*num_words == capacity) {
                capacity *= 2;
                words = (char**)realloc(words, capacity * sizeof(char*));
            }
            words[*num_words] = strdup(word_buffer);
            (*num_words)++;
        }
    }
    
    return words;
}

void ss_free_words(char** words, int num_words) {
    if (!words) return;
    for (int i = 0; i < num_words; i++) {
        free(words[i]);
    }
    free(words);
}

char* ss_join_words(char** words, int num_words) {
    if (num_words == 0) return strdup("");
    
    // Calculate total length accounting for spaces
    // Don't add space before delimiters (. ! ?)
    long total_len = 0;
    for (int i = 0; i < num_words; i++) {
        total_len += strlen(words[i]);
        if (i < num_words - 1) {
            // Add space before next word unless next word is a delimiter
            if (!(strlen(words[i + 1]) == 1 && 
                  (words[i + 1][0] == '.' || words[i + 1][0] == '!' || words[i + 1][0] == '?'))) {
                total_len++; // Space
            }
        }
    }
    
    char* sentence = (char*)malloc(total_len + 1);
    sentence[0] = '\0';
    
    for (int i = 0; i < num_words; i++) {
        strcat(sentence, words[i]);
        if (i < num_words - 1) {
            // Add space before next word unless next word is a delimiter
            if (!(strlen(words[i + 1]) == 1 && 
                  (words[i + 1][0] == '.' || words[i + 1][0] == '!' || words[i + 1][0] == '?'))) {
                strcat(sentence, " ");
            }
        }
    }
    return sentence;
}

char* ss_join_sentences(char** sentences, int num_sentences) {
    long total_len = 0;
    for (int i = 0; i < num_sentences; i++) {
        total_len += strlen(sentences[i]);
    }
    
    char* text = (char*)malloc(total_len + 1);
    text[0] = '\0';
    
    for (int i = 0; i < num_sentences; i++) {
        strcat(text, sentences[i]);
    }
    return text;
}


// --- NS Request Handlers ---

void ss_handle_create_file(int ns_sock, Req_FileOp* req) {
    // NOTE: Removed global g_is_syncing check
    // Recovery now uses fine-grained file locks, so files not being recovered
    // can still be created/accessed normally during recovery
    
    char filepath[MAX_PATH];
    ss_get_path(SS_FILES_DIR, req->filename, filepath);
    
    FILE* f = fopen(filepath, "w");
    if (!f) {
        ss_log("CREATE: Failed to create file %s: %s", req->filename, strerror(errno));
        send_error_response_to_ns(ns_sock, "Failed to create file");
        return;
    }
    fclose(f);
    
    // FIX: Insert into metadata table with correct owner - PRIMARY file (not backup)
    metadata_table_insert(g_metadata_table, req->filename, req->username,
                         0, 0, 0, time(NULL), time(NULL), false);
    
    // Persist metadata immediately after CREATE
    metadata_table_save(g_metadata_table, METADATA_DB_PATH);
    
    ss_log("CREATE: File %s created by %s", req->filename, req->username);
    repl_schedule_update(req->filename); // Replicate empty file
    send_success_response_to_ns(ns_sock, "File created");
}

void ss_handle_delete_file(int ns_sock, Req_FileOp* req) {
    // NOTE: Removed global g_is_syncing check
    // Recovery now uses fine-grained file locks, so files not being recovered
    // can still be deleted normally during recovery
    
    FileLock* lock = lock_map_get(&g_file_lock_map, req->filename);
    pthread_rwlock_wrlock(&lock->file_lock);
    
    // Check for active WRITE operations (swapfiles exist)
    DIR* swap_dir = opendir(SS_SWAP_DIR);
    if (swap_dir) {
        struct dirent* entry;
        char swap_prefix[MAX_FILENAME + 10];
        snprintf(swap_prefix, sizeof(swap_prefix), "%s_swap_", req->filename);
        size_t prefix_len = strlen(swap_prefix);
        int has_swapfiles = 0;
        
        while ((entry = readdir(swap_dir)) != NULL) {
            if (entry->d_type == DT_REG && strncmp(entry->d_name, swap_prefix, prefix_len) == 0) {
                has_swapfiles = 1;
                break;
            }
        }
        closedir(swap_dir);
        
        if (has_swapfiles) {
            ss_log("DELETE: Cannot delete %s - WRITE operation in progress (swapfiles exist)", req->filename);
            pthread_rwlock_unlock(&lock->file_lock);
            send_error_response_to_ns(ns_sock, "Cannot delete file - WRITE operation in progress");
            return;
        }
    }
    
    char filepath[MAX_PATH];
    char undopath[MAX_PATH];
    
    ss_get_path(SS_FILES_DIR, req->filename, filepath);
    ss_get_path(SS_UNDO_DIR, req->filename, undopath);
    
    if (unlink(filepath) != 0) {
        ss_log("DELETE: Failed to delete file %s: %s", req->filename, strerror(errno));
        pthread_rwlock_unlock(&lock->file_lock);
        send_error_response_to_ns(ns_sock, "Failed to delete file");
        return;
    }
    
    unlink(undopath); // Delete undo file, ignore error
    
    // Delete all checkpoints for this file
    DIR* checkpoint_dir = opendir(SS_CHECKPOINT_DIR);
    if (checkpoint_dir) {
        struct dirent* entry;
        // Build prefix: filename_
        char prefix[MAX_FILENAME + 1];
        snprintf(prefix, sizeof(prefix), "%s_", req->filename);
        size_t prefix_len = strlen(prefix);
        
        while ((entry = readdir(checkpoint_dir)) != NULL) {
            if (entry->d_type == DT_REG && strncmp(entry->d_name, prefix, prefix_len) == 0) {
                char checkpoint_path[MAX_PATH];
                snprintf(checkpoint_path, sizeof(checkpoint_path), "%s/%s", SS_CHECKPOINT_DIR, entry->d_name);
                if (unlink(checkpoint_path) != 0) {
                    ss_log("DELETE: Failed to delete checkpoint %s: %s", entry->d_name, strerror(errno));
                }
            }
        }
        closedir(checkpoint_dir);
    }
    
    pthread_rwlock_unlock(&lock->file_lock);
    
    // Remove from metadata table
    metadata_table_remove(g_metadata_table, req->filename);
    
    // Persist metadata immediately after DELETE
    metadata_table_save(g_metadata_table, METADATA_DB_PATH);
    
    ss_log("DELETE: File %s deleted", req->filename);
    repl_schedule_delete(req->filename);
    send_success_response_to_ns(ns_sock, "File deleted");
}

void ss_handle_get_info(int ns_sock, Req_FileOp* req) {
    // Check metadata table first
    FileMetadataNode* node = metadata_table_get(g_metadata_table, req->filename);
    if (!node) {
        ss_log("INFO: File not found in metadata table: %s", req->filename);
        send_error_response_to_ns(ns_sock, "File not found");
        return;
    }
    
    // FIX: Check if this is a VIEW -l request from NS (username is empty)
    // If username is provided and not owner, deny access
    if (req->username[0] != '\0' && strcmp(req->username, node->owner) != 0) {
        // Not owner - would need to check permissions table here
        // For now, deny access to non-owners
        ss_log("INFO: User %s is not owner of %s (owner: %s) - access denied", 
               req->username, req->filename, node->owner);
        free(node);
        send_error_response_to_ns(ns_sock, "Access denied: you are not the owner");
        return;
    }
    
    if (req->username[0] != '\0') {
        ss_log("INFO: User %s is owner of %s - access granted", req->username, req->filename);
    } else {
        ss_log("INFO: NS requesting metadata for %s (for VIEW -l)", req->filename);
    }
    
    // Don't update access time for INFO (only for READ/WRITE/STREAM)
    
    // Build FileMetadata response from cached data
    FileMetadata meta;
    strncpy(meta.filename, node->filename, MAX_FILENAME - 1);
    meta.filename[MAX_FILENAME - 1] = '\0';
    strncpy(meta.owner, node->owner, MAX_USERNAME - 1);
    meta.owner[MAX_USERNAME - 1] = '\0';
    meta.size_bytes = node->file_size;
    meta.word_count = node->word_count;
    meta.char_count = node->char_count;
    meta.last_access_time = node->last_access;
    meta.last_modified_time = node->last_modified;
    
    free(node);
    
    ss_log("INFO: Returning cached metadata for %s (size: %lu, words: %lu, chars: %lu)",
           req->filename, meta.size_bytes, meta.word_count, meta.char_count);
    
    send_response(ns_sock, MSG_S2N_FILE_INFO_RES, &meta, sizeof(meta));
}

void ss_handle_get_content_for_exec(int ns_sock, Req_FileOp* req) {
    char filepath[MAX_PATH];
    ss_get_path(SS_FILES_DIR, req->filename, filepath);
    
    long file_size;
    char* content = ss_read_file_to_memory(filepath, &file_size);
    if (!content) {
        ss_log("EXEC: Failed to read file %s", req->filename);
        send_error_response_to_ns(ns_sock, "File not found");
        return;
    }
    
    ss_log("EXEC: Read file %s, size=%ld bytes, content='%s'", req->filename, file_size, content);
    
    Res_Exec res;
    strncpy(res.output, content, MAX_PAYLOAD - 1);
    res.output[MAX_PAYLOAD - 1] = '\0';
    
    ss_log("EXEC: Sending content to NS: '%s'", res.output);
    send_response(ns_sock, MSG_S2N_EXEC_CONTENT, &res, sizeof(res));
    free(content);
}


// --- Client Request Handlers ---

void ss_handle_read(int client_sock, Req_FileOp* req) {
    ss_log("READ: Handler called for file '%s'", req->filename);
    
    // Check if SS is currently syncing
    // NOTE: Removed global g_is_syncing check
    // Recovery now uses fine-grained file locks, so files not being recovered
    // can still be read normally during recovery
    
    ss_log("READ: Starting read for file %s", req->filename);
    
    // Check metadata table first
    if (!metadata_table_exists(g_metadata_table, req->filename)) {
        ss_log("READ: File not found in metadata table: %s", req->filename);
        send_file_not_found_to_client(client_sock, "File not found");
        return;
    }
    
    // Update last access time in metadata table
    metadata_table_update_access_time(g_metadata_table, req->filename);
    
    FileLock* lock = lock_map_get(&g_file_lock_map, req->filename);
    pthread_rwlock_rdlock(&lock->file_lock);
    
    char filepath[MAX_PATH];
    ss_get_path(SS_FILES_DIR, req->filename, filepath);
    ss_log("READ: Full path is %s", filepath);

    FILE* f = fopen(filepath, "r");
    if (!f) {
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("READ: File not found on disk %s (metadata inconsistency)", req->filename);
        send_error_response_to_client(client_sock, "File not found");
        return;
    }
    
    ss_log("READ: File opened successfully, starting to read chunks");
    Res_FileContent chunk;
    size_t nread;
    bool sent_data = false;
    int chunk_count = 0;
    
    while ((nread = fread(chunk.data, 1, MAX_PAYLOAD, f)) > 0) {
        chunk_count++;
        chunk.data_len = nread;
        chunk.is_final_chunk = (nread < MAX_PAYLOAD);
        ss_log("READ: Chunk %d - read %zu bytes, is_final=%d", chunk_count, nread, chunk.is_final_chunk);
        // Send the full structure so is_final_chunk is at the correct offset
        send_response(client_sock, MSG_S2C_READ_CONTENT, &chunk, sizeof(chunk));
        ss_log("READ: Chunk %d sent", chunk_count);
        sent_data = true;
        
        if (chunk.is_final_chunk) break;
    }
    
    // If file is empty, send an empty chunk with final flag
    if (!sent_data) {
        ss_log("READ: File is empty, sending empty final chunk");
        memset(&chunk, 0, sizeof(chunk));
        chunk.data_len = 0;
        chunk.is_final_chunk = true;
        send_response(client_sock, MSG_S2C_READ_CONTENT, &chunk, sizeof(chunk));
    }
    
    fclose(f);
    pthread_rwlock_unlock(&lock->file_lock);
    ss_log("READ: Read complete for %s, sent %d chunks", req->filename, chunk_count);
}

void ss_handle_stream(int client_sock, Req_FileOp* req) {
    ss_log("STREAM: Handler called for file '%s'", req->filename);
    
    // Check metadata table first
    if (!metadata_table_exists(g_metadata_table, req->filename)) {
        ss_log("STREAM: File not found in metadata table: %s", req->filename);
        send_file_not_found_to_client(client_sock, "File not found");
        return;
    }
    
    // Update last access time
    metadata_table_update_access_time(g_metadata_table, req->filename);
    
    FileLock* lock = lock_map_get(&g_file_lock_map, req->filename);
    pthread_rwlock_rdlock(&lock->file_lock);

    char filepath[MAX_PATH];
    ss_get_path(SS_FILES_DIR, req->filename, filepath);

    FILE* f = fopen(filepath, "r");
    if (!f) {
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("STREAM: File not found on disk %s (metadata inconsistency)", req->filename);
        send_error_response_to_client(client_sock, "File not found");
        return;
    }
    
    Res_Stream word;
    while (fscanf(f, "%255s", word.word) == 1) {
        if (send_response(client_sock, MSG_S2C_STREAM_WORD, &word, sizeof(word)) < 0) {
            break; // Client disconnected
        }
        usleep(100000); // 0.1 seconds
    }
    
    send_response(client_sock, MSG_S2C_STREAM_END, NULL, 0);
    fclose(f);
    pthread_rwlock_unlock(&lock->file_lock);
}

void ss_handle_undo(int client_sock, Req_FileOp* req) {
    FileLock* lock = lock_map_get(&g_file_lock_map, req->filename);
    pthread_rwlock_wrlock(&lock->file_lock);

    char filepath[MAX_PATH], undopath[MAX_PATH], tmppath[MAX_PATH];
    ss_get_path(SS_FILES_DIR, req->filename, filepath);
    ss_get_path(SS_UNDO_DIR, req->filename, undopath);
    snprintf(tmppath, sizeof(tmppath), "%s/%s.tmp", SS_FILES_DIR, req->filename);

    // 1. Check if undo file exists
    struct stat st;
    if (stat(undopath, &st) != 0) {
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("UNDO: No undo history for %s", req->filename);
        send_error_response_to_client(client_sock, "No undo history for this file");
        return;
    }

    // 2. rename(file -> tmp)
    if (rename(filepath, tmppath) != 0) {
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("UNDO: Failed to rename %s to %s", filepath, tmppath);
        send_error_response_to_client(client_sock, "Undo failed (step 1)");
        return;
    }
    
    // 3. rename(undo -> file)
    if (rename(undopath, filepath) != 0) {
        rename(tmppath, filepath); // Rollback
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("UNDO: Failed to rename %s to %s", undopath, filepath);
        send_error_response_to_client(client_sock, "Undo failed (step 2)");
        return;
    }
    
    // 4. rename(tmp -> undo)
    rename(tmppath, undopath); // Old file is new undo. Ignore error.
    
    // 5. Recalculate metadata after undo
    struct stat file_st;
    if (stat(filepath, &file_st) == 0) {
        // Read file to calculate word/char counts
        long file_size;
        char* content = ss_read_file_to_memory(filepath, &file_size);
        if (content) {
            int num_sentences;
            char** sentences = ss_split_sentences(content, &num_sentences);
            
            uint64_t word_count = 0;
            for (int i = 0; i < num_sentences; i++) {
                int num_words;
                char** words = ss_split_words(sentences[i], &num_words);
                word_count += num_words;
                ss_free_words(words, num_words);
            }
            
            uint64_t char_count = strlen(content);
            
            // Update metadata table
            metadata_table_update_size(g_metadata_table, req->filename, file_st.st_size);
            metadata_table_update_counts(g_metadata_table, req->filename, word_count, char_count);
            metadata_table_update_modified_time(g_metadata_table, req->filename);
            
            ss_log("UNDO: Updated metadata for %s (size: %ld, words: %lu, chars: %lu)",
                   req->filename, file_st.st_size, word_count, char_count);
            
            ss_free_sentences(sentences, num_sentences);
            free(content);
        }
    }
    
    // Persist metadata immediately after UNDO
    metadata_table_save(g_metadata_table, METADATA_DB_PATH);
    
    pthread_rwlock_unlock(&lock->file_lock);
    
    ss_log("UNDO: File %s reverted", req->filename);
    repl_schedule_update(req->filename);
    send_success_response_to_client(client_sock, "Undo successful");
}

void ss_handle_checkpoint(int client_sock, Req_Checkpoint* req) {
    char undopath[MAX_PATH];
    FileLock* lock = lock_map_get(&g_file_lock_map, req->filename);
    char filepath[MAX_PATH], checkpath[MAX_PATH];
    ss_get_path(SS_FILES_DIR, req->filename, filepath);
    snprintf(checkpath, sizeof(checkpath), "%s/%s_%s", SS_CHECKPOINT_DIR, req->filename, req->tag);

    if (strcmp(req->command, "CHECKPOINT") == 0) {
        // Enforce uniqueness: do not overwrite existing checkpoint tag for this file
        if (access(checkpath, F_OK) == 0) {
            ss_log("CHECKPOINT: Tag already exists for %s -> %s", req->filename, checkpath);
            send_error_response_to_client(client_sock, "Checkpoint tag already exists");
            return;
        }

        pthread_rwlock_rdlock(&lock->file_lock);
        if (_copy_file(filepath, checkpath) != 0) {
            pthread_rwlock_unlock(&lock->file_lock);
            ss_log("CHECKPOINT: Failed to copy %s to %s", filepath, checkpath);
            send_error_response_to_client(client_sock, "Failed to create checkpoint");
            return;
        }
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("CHECKPOINT: Created checkpoint %s", checkpath);
        send_success_response_to_client(client_sock, "Checkpoint created");
    
    } else if (strcmp(req->command, "REVERT") == 0) {
        pthread_rwlock_wrlock(&lock->file_lock);
        // Make undo backup before reverting
        ss_get_path(SS_UNDO_DIR, req->filename, undopath);
        _copy_file(filepath, undopath); // Ignore error
        
        if (_copy_file(checkpath, filepath) != 0) {
            pthread_rwlock_unlock(&lock->file_lock);
            ss_log("REVERT: Failed to copy %s to %s", checkpath, filepath);
            send_error_response_to_client(client_sock, "Failed to revert checkpoint");
            return;
        }
        
        // Recalculate metadata after revert
        struct stat st;
        if (stat(filepath, &st) == 0) {
            long file_size;
            char* content = ss_read_file_to_memory(filepath, &file_size);
            if (content) {
                int num_sentences;
                char** sentences = ss_split_sentences(content, &num_sentences);
                
                uint64_t word_count = 0;
                for (int i = 0; i < num_sentences; i++) {
                    int num_words;
                    char** words = ss_split_words(sentences[i], &num_words);
                    word_count += num_words;
                    ss_free_words(words, num_words);
                }
                
                uint64_t char_count = strlen(content);
                
                metadata_table_update_size(g_metadata_table, req->filename, st.st_size);
                metadata_table_update_counts(g_metadata_table, req->filename, word_count, char_count);
                metadata_table_update_modified_time(g_metadata_table, req->filename);
                
                ss_log("REVERT: Updated metadata for %s (size: %ld, words: %lu, chars: %lu)",
                       req->filename, st.st_size, word_count, char_count);
                
                ss_free_sentences(sentences, num_sentences);
                free(content);
            }
        }
        
        // Persist metadata immediately after REVERT
        metadata_table_save(g_metadata_table, METADATA_DB_PATH);
        
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("REVERT: Reverted %s to checkpoint %s", filepath, req->tag);
        repl_schedule_update(req->filename);
        send_success_response_to_client(client_sock, "Revert successful");
    
    } else if (strcmp(req->command, "VIEWCHECKPOINT") == 0) {
        pthread_rwlock_rdlock(&lock->file_lock);
        
        FILE* f = fopen(checkpath, "r");
        if (!f) {
            pthread_rwlock_unlock(&lock->file_lock);
            ss_log("VIEWCHECKPOINT: Checkpoint not found %s", checkpath);
            send_error_response_to_client(client_sock, "Checkpoint not found");
            return;
        }
        
        // Read and send checkpoint content like regular read
        Res_FileContent chunk;
        size_t nread;
        bool sent_data = false;
        
        while ((nread = fread(chunk.data, 1, MAX_PAYLOAD, f)) > 0) {
            chunk.data_len = nread;
            chunk.is_final_chunk = (nread < MAX_PAYLOAD);
            // Send the full structure so is_final_chunk is at the correct offset
            send_response(client_sock, MSG_S2C_READ_CONTENT, &chunk, sizeof(chunk));
            sent_data = true;
            
            if (chunk.is_final_chunk) break;
        }
        
        // If checkpoint is empty, send an empty chunk with final flag
        if (!sent_data) {
            memset(&chunk, 0, sizeof(chunk));
            chunk.data_len = 0;
            chunk.is_final_chunk = true;
            send_response(client_sock, MSG_S2C_READ_CONTENT, &chunk, sizeof(chunk));
        }
        
        fclose(f);
        pthread_rwlock_unlock(&lock->file_lock);
        ss_log("VIEWCHECKPOINT: Sent checkpoint %s", req->tag);
    
    } else if (strcmp(req->command, "LISTCHECKPOINTS") == 0) {
        pthread_rwlock_rdlock(&lock->file_lock);
        
        // List all checkpoints for this file
        DIR* checkpoint_dir = opendir(SS_CHECKPOINT_DIR);
        if (!checkpoint_dir) {
            pthread_rwlock_unlock(&lock->file_lock);
            ss_log("LISTCHECKPOINTS: Failed to open checkpoint directory");
            send_error_response_to_client(client_sock, "Failed to access checkpoints");
            return;
        }
        
        // Build list of checkpoints
        char checkpoint_list[MAX_PAYLOAD];
        snprintf(checkpoint_list, sizeof(checkpoint_list), "Checkpoints for '%s':\n", req->filename);
        
        struct dirent* entry;
        char prefix[MAX_FILENAME + 1];
        snprintf(prefix, sizeof(prefix), "%s_", req->filename);
        size_t prefix_len = strlen(prefix);
        
        bool found_any = false;
        while ((entry = readdir(checkpoint_dir)) != NULL) {
            if (entry->d_type == DT_REG && strncmp(entry->d_name, prefix, prefix_len) == 0) {
                // Extract tag from filename (after prefix)
                const char* tag = entry->d_name + prefix_len;
                
                // Get checkpoint file stats
                char checkpoint_path[MAX_PATH];
                snprintf(checkpoint_path, sizeof(checkpoint_path), "%s/%s", SS_CHECKPOINT_DIR, entry->d_name);
                struct stat st;
                if (stat(checkpoint_path, &st) == 0) {
                    char time_str[64];
                    struct tm local_tm;
                    localtime_r(&st.st_mtime, &local_tm);
                    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &local_tm);
                    
                    char line[256];
                    snprintf(line, sizeof(line), "  - %s (created: %s, size: %lld bytes)\n", 
                            tag, time_str, (long long)st.st_size);
                    
                    if (strlen(checkpoint_list) + strlen(line) < MAX_PAYLOAD - 1) {
                        strcat(checkpoint_list, line);
                        found_any = true;
                    }
                }
            }
        }
        closedir(checkpoint_dir);
        
        if (!found_any) {
            strcat(checkpoint_list, "  (no checkpoints found)\n");
        }
        
        pthread_rwlock_unlock(&lock->file_lock);
        
        // Send as a view response
        Res_View res;
        strncpy(res.data, checkpoint_list, MAX_PAYLOAD - 1);
        res.data[MAX_PAYLOAD - 1] = '\0';
        send_response(client_sock, MSG_N2C_VIEW_RES, &res, sizeof(res));
        ss_log("LISTCHECKPOINTS: Listed checkpoints for %s", req->filename);
    }
}

// --- Complex Write Transaction ---
void ss_handle_write_transaction(int client_sock, Req_Write_Transaction* req) {
    // NOTE: Removed global g_is_syncing check
    // Recovery now uses fine-grained file locks, so files not being recovered
    // can still be written normally during recovery
    
    FileLock* lock = lock_map_get(&g_file_lock_map, req->filename);
    pthread_mutex_t* sentence_lock = lock_map_get_sentence_lock(lock, req->sentence_num);

    // 1. Try to lock the sentence. If busy, tell client.
    if (pthread_mutex_trylock(sentence_lock) != 0) {
        ss_log("WRITE: Sentence %d of %s is already locked", req->sentence_num, req->filename);
        send_lock_error_to_client(client_sock, "Sentence is locked by another user");
        return;
    }

    // 2. Create swapfile for isolation (readers see pre-WRITE state)
    char filepath[MAX_PATH], swappath[MAX_PATH], undopath[MAX_PATH];
    ss_get_path(SS_FILES_DIR, req->filename, filepath);
    snprintf(swappath, sizeof(swappath), "%s/%s_swap_%d", SS_SWAP_DIR, req->filename, req->sentence_num);
    ss_get_path(SS_UNDO_DIR, req->filename, undopath);
    
    if (_copy_file(filepath, swappath) != 0) {
        ss_log("WRITE: Failed to create swapfile for %s", req->filename);
        pthread_mutex_unlock(sentence_lock);
        send_error_response_to_client(client_sock, "Write failed (could not create swapfile)");
        return;
    }
    ss_log("WRITE: Created swapfile %s (sentence %d)", req->filename, req->sentence_num);

    // 3. Make undo backup from swapfile
    if (_copy_file(swappath, undopath) != 0) {
        ss_log("WRITE: Failed to create undo copy for %s", req->filename);
        unlink(swappath);  // Clean up swapfile
        pthread_mutex_unlock(sentence_lock);
        send_error_response_to_client(client_sock, "Write failed (could not create undo)");
        return;
    }

    // 4. Read swapfile and split into sentences
    long file_size;
    char* file_content = ss_read_file_to_memory(swappath, &file_size);
    if (!file_content && file_size > 0) {
        ss_log("WRITE: Failed to read swapfile %s", swappath);
        unlink(swappath);  // Clean up swapfile
        pthread_mutex_unlock(sentence_lock);
        send_error_response_to_client(client_sock, "Write failed (could not read swapfile)");
        return;
    }
    
    int num_sentences;
    char** sentences = ss_split_sentences(file_content ? file_content : "", &num_sentences);
    if (file_content) free(file_content);

    // Validate sentence index:
    // - Negative indices are invalid
    // - Indices 0..(num_sentences-1) are always valid (editing existing sentences)
    // - Index num_sentences (appending) is ONLY valid if last sentence ends with delimiter
    if (req->sentence_num < 0) {
        ss_log("WRITE: Sentence index %d is negative", req->sentence_num);
        char err_msg[256];
        snprintf(err_msg, sizeof(err_msg), "ERROR: Sentence index %d is invalid (negative not allowed)", req->sentence_num);
        send_error_response_to_client(client_sock, err_msg);
        ss_free_sentences(sentences, num_sentences);
        unlink(swappath);  // Clean up swapfile
        pthread_mutex_unlock(sentence_lock);
        return;
    }
    
    if (req->sentence_num > num_sentences) {
        ss_log("WRITE: Sentence index %d out of range (max: %d)", req->sentence_num, num_sentences);
        char err_msg[256];
        snprintf(err_msg, sizeof(err_msg), "ERROR: Sentence index %d out of range (file has %d sentence%s, valid indices: 0-%d)", 
                 req->sentence_num, num_sentences, num_sentences == 1 ? "" : "s", num_sentences > 0 ? num_sentences - 1 : 0);
        send_error_response_to_client(client_sock, err_msg);
        ss_free_sentences(sentences, num_sentences);
        unlink(swappath);  // Clean up swapfile
        pthread_mutex_unlock(sentence_lock);
        return;
    }
    
    if (req->sentence_num == num_sentences) {
        // Appending new sentence - only valid if last sentence ends with delimiter
        if (num_sentences == 0) {
            // Empty file - sentence 0 is valid for appending
            ss_log("WRITE: Appending sentence 0 to empty file");
        } else {
            // Check if last sentence ends with delimiter
            const char* last_sentence = sentences[num_sentences - 1];
            size_t len = strlen(last_sentence);
            if (len == 0 || (last_sentence[len-1] != '.' && last_sentence[len-1] != '!' && last_sentence[len-1] != '?')) {
                ss_log("WRITE: Cannot append sentence %d - last sentence does not end with delimiter", req->sentence_num);
                send_error_response_to_client(client_sock, "ERROR: Cannot append new sentence - last sentence is incomplete (missing delimiter . ! or ?)");
                ss_free_sentences(sentences, num_sentences);
                unlink(swappath);  // Clean up swapfile
                pthread_mutex_unlock(sentence_lock);
                return;
            }
            ss_log("WRITE: Appending sentence %d (last sentence ends with delimiter)", req->sentence_num);
        }
    }
    
    // We are OK to proceed, tell client
    send_response(client_sock, MSG_S2C_WRITE_OK, NULL, 0);

    // 5. Initialize working sentence
    // If appending (sentence_num == num_sentences), start with empty sentence
    int num_words = 0;
    char** words = NULL;
    
    if (req->sentence_num < num_sentences) {
        // Editing existing sentence
        words = ss_split_words(sentences[req->sentence_num], &num_words);
    } else {
        // Appending new sentence (sentence_num == num_sentences)
        words = (char**)malloc(16 * sizeof(char*));
        num_words = 0;
    }

    // 6. Receive and apply changes incrementally (one at a time)
    // Per spec: each subquery updates indices for the next one
    MsgHeader header;
    int connection_lost = 0;
    
    while (recv_header(client_sock, &header) > 0) {
        if (header.type == MSG_C2S_WRITE_DATA) {
            // FIX: Validate payload length to prevent stack buffer overflow
            if (header.payload_len > sizeof(Req_Write_Data)) {
                ss_log("WRITE: Payload too large (%d > %lu) - aborting transaction", 
                       header.payload_len, sizeof(Req_Write_Data));
                connection_lost = 1;
                break;
            }

            Req_Write_Data data;
            if (recv_payload(client_sock, &data, header.payload_len) <= 0) {
                ss_log("WRITE: Connection lost while receiving payload");
                connection_lost = 1;
                break;
            }
            
            // SAFETY: Ensure content is null-terminated
            // Calculate how much data is in content
            int content_len = header.payload_len - sizeof(int); // word_index is int
            if (content_len < 0) content_len = 0;
            if (content_len >= sizeof(data.content)) content_len = sizeof(data.content) - 1;
            data.content[content_len] = '\0';
            
            // Validate word index per spec:
            // - Negative indices are invalid
            // - Indices > num_words are invalid (can insert at num_words but not beyond)
            if (data.word_index < 0 || data.word_index > num_words) {
                ss_log("WRITE: Invalid word index %d for sentence with %d words", data.word_index, num_words);
                char err_msg[256];
                snprintf(err_msg, sizeof(err_msg), "ERROR: Invalid word index %d. Current sentence has %d words (valid indices: 0-%d)", 
                         data.word_index, num_words, num_words);
                send_error_response_to_client(client_sock, err_msg);
                
                // Continue accepting more subqueries (spec says return ERROR for that subquery, not abort)
                continue;
            }
            
            // Split the content into individual words (handling delimiters)
            int content_num_words;
            char** content_words = ss_split_words(data.content, &content_num_words);
            
            // Insert all content words at the specified index
            // Allocate space for new words
            words = (char**)realloc(words, (num_words + content_num_words) * sizeof(char*));
            
            // Shift existing words to make room
            memmove(&words[data.word_index + content_num_words], 
                    &words[data.word_index], 
                    (num_words - data.word_index) * sizeof(char*));
            
            // Insert new words
            for (int j = 0; j < content_num_words; j++) {
                words[data.word_index + j] = content_words[j]; // Transfer ownership
            }
            num_words += content_num_words;
            
            // Free only the array, not the strings (ownership transferred)
            free(content_words);
            
        } else if (header.type == MSG_C2S_WRITE_ETIRW) {
            break; // End of transaction
        }
    }
    
    // Check if connection was lost during transaction
    if (connection_lost) {
        ss_log("WRITE: Connection lost during transaction for %s - aborting (no changes saved)", req->filename);
        ss_free_words(words, num_words);
        ss_free_sentences(sentences, num_sentences);
        unlink(swappath);  // Delete swapfile
        pthread_mutex_unlock(sentence_lock);
        // Client already disconnected, no response needed
        return;
    }

    // 7. Re-join words into the modified sentence
    char* new_sentence = ss_join_words(words, num_words);
    
    // Update or append to sentences array
    if (req->sentence_num < num_sentences) {
        // Replace existing sentence
        free(sentences[req->sentence_num]);
        sentences[req->sentence_num] = new_sentence;
    } else {
        // Append new sentence (sentence_num == num_sentences)
        sentences = (char**)realloc(sentences, (num_sentences + 1) * sizeof(char*));
        sentences[num_sentences] = new_sentence;
        num_sentences++;
    }

    // 8. Re-join sentences and write to swapfile
    char* new_file_content = ss_join_sentences(sentences, num_sentences);
    
    FILE* f = fopen(swappath, "w");
    if (!f) {
        ss_log("WRITE: Failed to open swapfile for writing %s", swappath);
        free(new_file_content);
        ss_free_words(words, num_words);
        ss_free_sentences(sentences, num_sentences);
        unlink(swappath);
        pthread_mutex_unlock(sentence_lock);
        send_error_response_to_client(client_sock, "Write failed (could not write to swapfile)");
        return;
    }
    fwrite(new_file_content, 1, strlen(new_file_content), f);
    fclose(f);
    
    // 9. Acquire global file lock for atomic commit
    pthread_rwlock_wrlock(&lock->file_lock);
    ss_log("WRITE: Acquired global file lock for commit (file %s, sentence %d)", req->filename, req->sentence_num);
    
    // 10. Re-read original file and merge changes (to handle concurrent WRITEs to different sentences)
    // CRITICAL: The original file may have been modified by other WRITEs since we created the swapfile
    long current_file_size;
    char* current_file_content = ss_read_file_to_memory(filepath, &current_file_size);
    if (!current_file_content && current_file_size > 0) {
        ss_log("WRITE: Failed to re-read original file during commit for %s", req->filename);
        free(new_file_content);
        ss_free_words(words, num_words);
        ss_free_sentences(sentences, num_sentences);
        unlink(swappath);
        pthread_rwlock_unlock(&lock->file_lock);
        pthread_mutex_unlock(sentence_lock);
        send_error_response_to_client(client_sock, "Write failed (could not re-read file during commit)");
        return;
    }
    
    // Split current file into sentences
    int current_num_sentences;
    char** current_sentences = ss_split_sentences(current_file_content ? current_file_content : "", &current_num_sentences);
    if (current_file_content) free(current_file_content);
    
    // Replace or append our modified sentence in the current file's sentences
    if (req->sentence_num < current_num_sentences) {
        // Replace existing sentence in current file
        free(current_sentences[req->sentence_num]);
        current_sentences[req->sentence_num] = strdup(new_sentence);
    } else if (req->sentence_num == current_num_sentences) {
        // Append new sentence to current file
        current_sentences = (char**)realloc(current_sentences, (current_num_sentences + 1) * sizeof(char*));
        current_sentences[current_num_sentences] = strdup(new_sentence);
        current_num_sentences++;
    } else {
        // Sentence index out of range in current file - this shouldn't happen if validated earlier
        ss_log("WRITE: Sentence index %d out of range during commit (current file has %d sentences)", 
               req->sentence_num, current_num_sentences);
        free(new_file_content);
        ss_free_words(words, num_words);
        ss_free_sentences(sentences, num_sentences);
        ss_free_sentences(current_sentences, current_num_sentences);
        unlink(swappath);
        pthread_rwlock_unlock(&lock->file_lock);
        pthread_mutex_unlock(sentence_lock);
        send_error_response_to_client(client_sock, "Write failed (file changed during transaction)");
        return;
    }
    
    // Re-join the merged sentences and write to file
    char* merged_content = ss_join_sentences(current_sentences, current_num_sentences);
    
    // 11. Write merged content to original file (atomic commit)
    FILE* f_commit = fopen(filepath, "w");
    if (!f_commit) {
        ss_log("WRITE: Failed to open original file for commit %s", req->filename);
        free(new_file_content);
        free(merged_content);
        ss_free_words(words, num_words);
        ss_free_sentences(sentences, num_sentences);
        ss_free_sentences(current_sentences, current_num_sentences);
        unlink(swappath);
        pthread_rwlock_unlock(&lock->file_lock);
        pthread_mutex_unlock(sentence_lock);
        send_error_response_to_client(client_sock, "Write failed (could not write to file)");
        return;
    }
    fwrite(merged_content, 1, strlen(merged_content), f_commit);
    fclose(f_commit);
    
    // 12. Delete swapfile
    unlink(swappath);
    ss_log("WRITE: Committed and deleted swapfile for %s (sentence %d)", req->filename, req->sentence_num);
    
    // 13. Update metadata table with new counts (using merged content)
    uint64_t new_char_count = strlen(merged_content);
    uint64_t new_word_count = 0;
    for (int i = 0; i < current_num_sentences; i++) {
        int words_in_sentence;
        char** sentence_words = ss_split_words(current_sentences[i], &words_in_sentence);
        new_word_count += words_in_sentence;
        ss_free_words(sentence_words, words_in_sentence);
    }
    
    // Get file size from disk
    struct stat st;
    stat(filepath, &st);
    
    // Update metadata table
    metadata_table_update_size(g_metadata_table, req->filename, st.st_size);
    metadata_table_update_counts(g_metadata_table, req->filename, new_word_count, new_char_count);
    metadata_table_update_modified_time(g_metadata_table, req->filename);
    
    ss_log("WRITE: Updated metadata for %s (size: %ld, words: %lu, chars: %lu)",
           req->filename, st.st_size, new_word_count, new_char_count);
    
    // Persist metadata immediately after WRITE to ensure durability
    metadata_table_save(g_metadata_table, METADATA_DB_PATH);

    // 14. Clean up
    free(new_file_content);
    free(merged_content);
    ss_free_words(words, num_words);
    ss_free_sentences(sentences, num_sentences);
    ss_free_sentences(current_sentences, current_num_sentences);

    // 15. Release locks and send response
    pthread_rwlock_unlock(&lock->file_lock);
    pthread_mutex_unlock(sentence_lock);

    ss_log("WRITE: Completed for %s (sentence %d)", req->filename, req->sentence_num);
    repl_schedule_update(req->filename);
    send_success_response_to_client(client_sock, "Write successful");
}