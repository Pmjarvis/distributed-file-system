#ifndef SS_GLOBALS_H
#define SS_GLOBALS_H

#include "ss_data_structs.h"
#include "ss_metadata.h"

// Connection to Name Server
extern int g_ns_sock;

// This SS's info
extern char g_ss_ip[16];
extern int g_ss_client_port;
extern int g_ss_id;

// Backup SS's info
extern char g_backup_ip[16];
extern int g_backup_port;

// Global lock manager
extern FileLockMap g_file_lock_map;

// Global replication queue
extern ReplicationQueue g_repl_queue;

// Global metadata hash table
extern MetadataHashTable* g_metadata_table;

// Graceful shutdown flag
extern volatile int g_shutdown;

// Recovery sync flag (blocks operations during recovery)
extern volatile int g_is_syncing;
extern pthread_mutex_t g_sync_mutex;  // Protects g_is_syncing

// Data directories
#define SS_ROOT_DIR "ss_data"
#define SS_FILES_DIR "ss_data/files"
#define SS_UNDO_DIR "ss_data/undo"
#define SS_CHECKPOINT_DIR "ss_data/checkpoints"

#endif // SS_GLOBALS_H