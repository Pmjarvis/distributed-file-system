#ifndef SS_LOGGER_H
#define SS_LOGGER_H

#include <stdarg.h>
#include <pthread.h>

void log_init(const char* filename);
// Logs to file and console
void ss_log(const char* format, ...);
// Logs only to console
void ss_log_console(const char* format, ...);
void log_cleanup();

#endif // SS_LOGGER_H