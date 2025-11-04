#include "ss_logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

static FILE* g_log_file = NULL;
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;

void log_init(const char* filename) {
    g_log_file = fopen(filename, "a");
    if (g_log_file == NULL) {
        perror("Failed to open log file, logging to stderr");
        g_log_file = stderr; 
    }
    ss_log("--- LOGGING STARTED ---");
}

static void get_timestamp(char* buffer, size_t len) {
    time_t now = time(NULL);
    struct tm local_tm;
    localtime_r(&now, &local_tm);
    strftime(buffer, len, "%Y-%m-%d %H:%M:%S", &local_tm);
}

void ss_log(const char* format, ...) {
    pthread_mutex_lock(&g_log_mutex);
    
    char time_buf[32];
    get_timestamp(time_buf, sizeof(time_buf));
    
    // 1. Log to file
    fprintf(g_log_file, "[%s] ", time_buf);
    va_list args;
    va_start(args, format);
    vfprintf(g_log_file, format, args);
    va_end(args);
    fprintf(g_log_file, "\n");
    fflush(g_log_file);
    
    // 2. Log to console (per spec)
    printf("[%s] ", time_buf);
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    
    pthread_mutex_unlock(&g_log_mutex);
}

void ss_log_console(const char* format, ...) {
    pthread_mutex_lock(&g_log_mutex);
    
    char time_buf[32];
    get_timestamp(time_buf, sizeof(time_buf));

    // Log only to console
    printf("[%s] ", time_buf);
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    
    pthread_mutex_unlock(&g_log_mutex);
}

void log_cleanup() {
    if (g_log_file && g_log_file != stderr) {
        ss_log("--- LOGGING STOPPED ---");
        fclose(g_log_file);
    }
}