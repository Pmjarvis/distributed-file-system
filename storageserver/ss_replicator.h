#ifndef SS_REPLICATOR_H
#define SS_REPLICATOR_H

#include "../common/protocol.h"

// Starts the replication worker thread
void repl_start_worker();

// Schedules a file for replication
void repl_schedule_update(const char* filename);

// Schedules a file for deletion
void repl_schedule_delete(const char* filename);

// Stops the replication worker thread
void repl_shutdown_worker();

// Function to handle an incoming connection from another SS (for receiving replicas)
void handle_replication_receive(int sock);

// Function to sync all data from our backup (on recovery)
void handle_recovery_sync();

#endif // SS_REPLICATOR_H