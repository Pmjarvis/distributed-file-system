CC = gcc
# Use -std=c99 or gnu99, and define _POSIX_C_SOURCE for full POSIX compliance
CFLAGS = -Wall -g -pthread -std=gnu99 -D_POSIX_C_SOURCE=200809L
LIBS = -pthread

# Source files
OBJ = client_main.o client_commands.o client_net.o

# Target
TARGET = client

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJ) $(LIBS)

# Pattern rule for .c to .o
%.o: %.c %.h ../common/protocol.h
	$(CC) $(CFLAGS) -c $< -o $@

# Specific dependency for files that include client_net.h
client_main.o: client_main.c client_net.h client_commands.h
client_commands.o: client_commands.c client_commands.h client_net.h

clean:
	rm -f $(TARGET) $(OBJ)