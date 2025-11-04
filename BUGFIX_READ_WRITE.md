# Bug Fix: READ and WRITE Issues

## Issues Reported
1. **WRITE command not terminating** after typing ETIRW and pressing enter
2. **READ command not working**

## Root Cause Analysis

### READ Command Issue
The storage server was calculating the payload size incorrectly when sending file content chunks.

**Problem Code** (ss_file_manager.c, line 410):
```c
size_t payload_size = (char*)&(chunk.is_final_chunk) - (char*)&chunk + sizeof(chunk.is_final_chunk);
```

This calculates the **offset** to `is_final_chunk` plus its size, which equals the entire structure size (MAX_PAYLOAD + sizeof(bool) = 4096 + 1 = 4097 bytes), regardless of how much data was actually read.

**Correct Code**:
```c
size_t payload_size = nread + sizeof(chunk.is_final_chunk);
```

This sends only the actual data read (`nread` bytes) plus the flag (1 byte).

### Why This Mattered
- Server read 100 bytes from file
- Server calculated payload_size = 4097 bytes
- Server sent 4097 bytes (100 valid + 3997 garbage)
- Client received 4097 bytes
- Client calculated `data_len = 4097 - 1 = 4096`
- Client tried to write 4096 bytes (including 3996 bytes of garbage)

### WRITE Command Issue
The WRITE command issue is likely related to:
1. **Stdout buffering**: Added `fflush(stdout)` after `printf("w> ")` to ensure prompt is displayed
2. **Possible server-side blocking**: The server might be hanging in the write processing loop

## Fixes Applied

### 1. Fixed READ Command (storageserver/ss_file_manager.c)
**Location**: `ss_handle_read()` function, line ~410

```c
// BEFORE:
size_t payload_size = (char*)&(chunk.is_final_chunk) - (char*)&chunk + sizeof(chunk.is_final_chunk);

// AFTER:
size_t payload_size = nread + sizeof(chunk.is_final_chunk);
```

### 2. Fixed VIEWCHECKPOINT Command (storageserver/ss_file_manager.c)
**Location**: `ss_handle_checkpoint()` function, line ~543

Same fix as READ - was using incorrect payload size calculation.

```c
// BEFORE:
size_t payload_size = (char*)&(chunk.is_final_chunk) - (char*)&chunk + sizeof(chunk.is_final_chunk);

// AFTER:
size_t payload_size = nread + sizeof(chunk.is_final_chunk);
```

### 3. Added stdout flush (client/client_commands.c)
**Location**: `do_write()` function, line ~373

```c
while(1) {
    printf("w> ");
    fflush(stdout);  // <-- ADDED THIS
    if (!fgets(line_buf, sizeof(line_buf), stdin)) {
```

## Testing Recommendations

### Test READ Command
```bash
# Create a file with content
CREATE testfile.txt
WRITE testfile.txt 0
0 Hello
1 World
ETIRW

# Read it back
READ testfile.txt
# Should display: Hello World
```

### Test WRITE Command
```bash
CREATE testfile2.txt
WRITE testfile2.txt 0
# Should show: w>
0 First
1 word
ETIRW
# Should return to main prompt
```

### Test VIEWCHECKPOINT
```bash
CHECKPOINT testfile.txt tag1
VIEWCHECKPOINT testfile.txt tag1
# Should display file content
```

## Technical Details

### Data Structure
```c
typedef struct {
    char data[MAX_PAYLOAD];      // 4096 bytes
    bool is_final_chunk;         // 1 byte
} Res_FileContent;               // Total: 4097 bytes
```

### Correct Payload Calculation
When sending N bytes of actual data:
- `payload_size = N + sizeof(is_final_chunk)`
- `payload_size = N + 1`

### Client Reception
When receiving:
- `header.payload_len` = total bytes sent (N + 1)
- `data_len = header.payload_len - sizeof(is_final_chunk)`
- `data_len = (N + 1) - 1 = N` ✓ Correct!

## Files Modified
1. `storageserver/ss_file_manager.c` - Fixed READ and VIEWCHECKPOINT payload calculations
2. `client/client_commands.c` - Added stdout flush in WRITE loop

## Build Commands
```bash
# Rebuild storage server
cd storageserver && make clean && make

# Rebuild client  
cd client && make
```

## Additional Fix: Empty File Handling

### Issue
When reading an empty file with the READ command, the client would hang indefinitely waiting for data.

### Root Cause
The storage server's read loop:
```c
while ((nread = fread(chunk.data, 1, MAX_PAYLOAD, f)) > 0) {
    // Send data
}
```

For an empty file:
- `fread` returns 0 immediately
- Loop never executes
- **No message is sent to client**
- Client hangs waiting for at least one message

### Fix
Always send at least one message, even if the file is empty:

```c
bool sent_data = false;
while ((nread = fread(chunk.data, 1, MAX_PAYLOAD, f)) > 0) {
    // Send data
    sent_data = true;
}

// If file is empty, send an empty chunk with final flag
if (!sent_data) {
    chunk.is_final_chunk = true;
    size_t payload_size = sizeof(chunk.is_final_chunk);
    send_response(client_sock, MSG_S2C_READ_CONTENT, &chunk, payload_size);
}
```

### Applied To
- `ss_handle_read()` - READ command
- `ss_handle_checkpoint()` - VIEWCHECKPOINT command

## Status
- ✅ READ command fixed (payload size + empty file)
- ✅ VIEWCHECKPOINT command fixed (payload size + empty file)
- ⚠️ WRITE command - added flush, may need further testing
