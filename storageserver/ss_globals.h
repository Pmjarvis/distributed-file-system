#ifndef SS_GLOBALS_H
#define SS_GLOBALS_H

#include "ss_data_structs.h"
#include "ss_metadata.h"
#include <stdbool.h>

// Connection to Name Server
extern int g_ns_sock;

// This SS's info
extern char g_ss_ip[16];
extern int g_ss_client_port;
extern int g_ss_id;

// Backup SS's info
extern char g_backup_ip[16];
extern int g_backup_port;
extern pthread_mutex_t g_backup_config_mutex;  // Protects g_backup_ip and g_backup_port
// Local replication listener port (distinct from g_backup_port which is remote target)
extern int g_repl_listen_port;

// Global lock manager
extern FileLockMap g_file_lock_map;

// Global replication queue
extern ReplicationQueue g_repl_queue;

// Global metadata hash table
extern MetadataHashTable* g_metadata_table;

// Graceful shutdown flag
extern volatile int g_shutdown;

// Server ready flag (true when metadata is loaded and ready to serve)
extern volatile bool g_ss_ready;

// Recovery sync flag (blocks operations during recovery)
extern volatile int g_is_syncing;
extern pthread_mutex_t g_sync_mutex;  // Protects g_is_syncing

// Data directories (dynamically set based on SS ID)
extern char g_ss_root_dir[256];
extern char g_ss_files_dir[256];
extern char g_ss_undo_dir[256];
extern char g_ss_checkpoint_dir[256];
extern char g_ss_swap_dir[256];  // For WRITE operation swapfiles
extern char g_metadata_db_path[512];

// Helper macros for backward compatibility
#define SS_ROOT_DIR g_ss_root_dir
#define SS_FILES_DIR g_ss_files_dir
#define SS_UNDO_DIR g_ss_undo_dir
#define SS_CHECKPOINT_DIR g_ss_checkpoint_dir
#define SS_SWAP_DIR g_ss_swap_dir
#define METADATA_DB_PATH g_metadata_db_path

#endif // SS_GLOBALS_H