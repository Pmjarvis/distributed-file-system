# Bug Fix: Stack Smashing Detected in Name Server

## Problem
The Name Server was crashing with "stack smashing detected" error when a client created a file.

## Root Cause
Buffer overflow vulnerabilities were found in multiple locations where `strncpy()` was used with incorrect size parameters:

### 1. **Primary Issue** - `handle_create()` in ns_handler.c (Line 248)
```c
// BEFORE (WRONG):
strncpy(meta.filename, payload.filename, MAX_PATH);  // MAX_PATH = 1024
strncpy(meta.owner, session->username, MAX_USERNAME);

// AFTER (FIXED):
strncpy(meta.filename, payload.filename, MAX_FILENAME - 1);  // MAX_FILENAME = 256
meta.filename[MAX_FILENAME - 1] = '\0';
strncpy(meta.owner, session->username, MAX_USERNAME - 1);
meta.owner[MAX_USERNAME - 1] = '\0';
```

**Issue**: The code was trying to copy up to 1024 bytes (MAX_PATH) into a buffer sized for 256 bytes (MAX_FILENAME), causing stack corruption.

### 2. **Secondary Issue** - `handle_ss_redirect()` in ns_handler.c (Line 528)
```c
// BEFORE (WRONG):
strncpy(payload.filename, chk_payload.filename, MAX_PATH);

// AFTER (FIXED):
strncpy(payload.filename, chk_payload.filename, MAX_FILENAME - 1);
payload.filename[MAX_FILENAME - 1] = '\0';
```

**Issue**: Same problem - copying MAX_PATH bytes into MAX_FILENAME buffer.

### 3. **Data Structure Issue** - `AccessRequest` in ns_globals.h
```c
// BEFORE (INCONSISTENT):
typedef struct AccessRequest {
    char requester[MAX_USERNAME];
    char filename[MAX_PATH];  // Should be MAX_FILENAME
    struct AccessRequest* next;
} AccessRequest;

// AFTER (FIXED):
typedef struct AccessRequest {
    char requester[MAX_USERNAME];
    char filename[MAX_FILENAME];  // Consistent with protocol
    struct AccessRequest* next;
} AccessRequest;
```

### 4. **Usage Fix** - `handle_req_access()` in ns_handler.c (Line 575)
```c
// BEFORE (WRONG):
strncpy(new_req->requester, session->username, MAX_USERNAME);
strncpy(new_req->filename, payload.filename, MAX_PATH);

// AFTER (FIXED):
strncpy(new_req->requester, session->username, MAX_USERNAME - 1);
new_req->requester[MAX_USERNAME - 1] = '\0';
strncpy(new_req->filename, payload.filename, MAX_FILENAME - 1);
new_req->filename[MAX_FILENAME - 1] = '\0';
```

## Technical Details

### Why This Happened
The confusion arose from having two different constants:
- `MAX_PATH` = 1024 bytes (for full file paths with directories)
- `MAX_FILENAME` = 256 bytes (for just the filename component)

The `FileMetadata` structure correctly uses `MAX_FILENAME` for the filename field, but the code was incorrectly using `MAX_PATH` when copying into it.

### Stack Smashing
When `strncpy(meta.filename, payload.filename, MAX_PATH)` was called:
1. `meta.filename` is a 256-byte stack buffer
2. `strncpy` was told it could copy up to 1024 bytes
3. If the source string was long enough, this would write beyond the buffer
4. This corrupted the stack canary (security feature)
5. When the function returned, the canary check detected corruption
6. The program aborted with "stack smashing detected"

### Why It Crashed on CREATE
The CREATE command creates a `FileMetadata` structure on the stack and populates it. This was the first operation that triggered the buffer overflow because:
1. It created the metadata on the stack
2. It copied the filename with the wrong size
3. The stack canary was immediately corrupted
4. The function return triggered the canary check

## Prevention
To prevent similar issues:

1. **Always match buffer sizes**: Use the same constant for buffer declaration and strncpy size
2. **Use `size - 1`**: Always use `sizeof(buffer) - 1` or `MAX_xxx - 1` to leave room for null terminator
3. **Null terminate**: Always explicitly null-terminate after strncpy: `buffer[size-1] = '\0'`
4. **Enable warnings**: The compiler warned about this: `-Wstringop-overflow`
5. **Code review**: Check all strncpy/strcpy calls for size mismatches

## Files Modified
1. `nameserver/ns_handler.c` - Fixed 3 buffer overflow issues
2. `nameserver/ns_globals.h` - Fixed AccessRequest structure definition

## Testing
After the fix:
- ✅ Nameserver compiles without warnings
- ✅ CREATE command works without crashing
- ✅ All buffer operations respect size limits
- ✅ Stack canary checks pass

## Additional Client-Side Fixes

The same buffer overflow issue was found in the **client code**, where it was copying filenames with `MAX_PATH` size into `MAX_FILENAME` buffers.

### Fixed Files
- `client/client_commands.c` - Fixed 9 instances of buffer overflow
- `client/client_net.c` - Fixed 1 instance of buffer overflow

### Example Fix
```c
// BEFORE (WRONG):
strncpy(req.filename, args, MAX_PATH - 1);

// AFTER (FIXED):
strncpy(req.filename, args, MAX_FILENAME - 1);
req.filename[MAX_FILENAME - 1] = '\0';
```

This was causing the client to crash after successfully creating a file.

## Commit Message
```
Fix stack buffer overflow in file operations (NS and Client)

Name Server:
- Change strncpy size from MAX_PATH to MAX_FILENAME in handle_create()
- Fix buffer overflow in handle_ss_redirect() for checkpoint requests
- Update AccessRequest structure to use MAX_FILENAME consistently
- Add explicit null termination after all strncpy calls

Client:
- Fix all Req_FileOp.filename strncpy calls to use MAX_FILENAME
- Add explicit null termination after all strncpy calls
- Fixed in: do_create, do_delete, do_info, do_exec, do_read, do_stream,
  do_write, do_undo, do_request_access, do_checkpoint_cmd, get_ss_connection

This fixes the "stack smashing detected" crash in both NS and client.
```
