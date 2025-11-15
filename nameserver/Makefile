CC = gcc
CFLAGS = -Wall -g -pthread
LIBS = -pthread
# Add GLib flags
CFLAGS += $(shell pkg-config --cflags glib-2.0)
LIBS += $(shell pkg-config --libs glib-2.0)

# Source files
COMMON_OBJ = ../common/net_utils.o
NS_OBJ = ns_main.o ns_handler.o ns_user_manager.o ns_cache.o ns_ss_manager.o ns_access.o ns_folders.o ns_file_map.o

# Target
TARGET = nameserver

all: $(TARGET)

$(TARGET): $(NS_OBJ) $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o $(TARGET) $(NS_OBJ) $(COMMON_OBJ) $(LIBS)

# Pattern rule for .c to .o
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

../common/net_utils.o: ../common/net_utils.c ../common/net_utils.h ../common/protocol.h
	$(CC) $(CFLAGS) -c ../common/net_utils.c -o ../common/net_utils.o

clean:
	rm -f $(TARGET) $(NS_OBJ) $(COMMON_OBJ)