CC = gcc
# Use -std=c99 or gnu99, and define _POSIX_C_SOURCE for full POSIX compliance
CFLAGS = -Wall -g -pthread -std=gnu99 -D_POSIX_C_SOURCE=200809L
LIBS = -pthread

# Source files
COMMON_OBJ = ../common/net_utils.o
SS_OBJ = ss_main.o ss_handler.o ss_file_manager.o ss_logger.o ss_data_structs.o ss_replicator.o ss_metadata.o

# Target
TARGET = storageserver

all: $(TARGET)

$(TARGET): $(SS_OBJ) $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o $(TARGET) $(SS_OBJ) $(COMMON_OBJ) $(LIBS)

# Pattern rule for .c to .o
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

../common/net_utils.o: ../common/net_utils.c ../common/net_utils.h ../common/protocol.h
	$(CC) $(CFLAGS) -c ../common/net_utils.c -o ../common/net_utils.o

clean:
	rm -f $(TARGET) $(SS_OBJ) $(COMMON_OBJ) *.log