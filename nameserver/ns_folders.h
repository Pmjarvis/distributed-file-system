#ifndef NS_FOLDERS_H
#define NS_FOLDERS_H

#include <stdbool.h>

// --- Data Structures ---

typedef enum {
    NODE_ROOT,
    NODE_FOLDER,
    NODE_FILE
} NodeType;

typedef struct Node {
    char* name;
    NodeType type;
    struct Node* parent;
    struct Node** children;
    int child_count;
    int capacity;
} Node;

// --- Refactored Function Prototypes ---

Node* createNode(const char* name, NodeType type, Node* parent);
void addChild(Node* parent, Node* child);
Node* findChildByName(Node* parent, const char* name);
Node* findChild(Node* parent, const char* name, NodeType type);
Node* removeChild(Node* parent, Node* child);
void freeTree(Node* node);

// Creates a file *node* (not a real file) in the tree
// Returns true on success
bool createTreeFile(Node* current_directory, const char* filename);

// --- Command Implementations ---

// Returns error message on failure, NULL on success
const char* createTreeFolder(Node* current_directory, const char* foldername);

// Returns a malloc'd string of folder contents. Caller must free.
// Returns NULL on error.
char* viewTreeFolder(Node* current_directory);

// Returns error message on failure, NULL on success
const char* moveTreeFile(Node* current_directory, const char* filename, const char* foldername);

// Returns error message on failure, NULL on success
const char* upMoveTreeFile(Node* current_directory, const char* filename);

// Returns new directory on success, NULL on error
Node* openTreeFolder(Node* current_directory, const char* foldername, bool createIfMissing);

// Returns parent directory on success, NULL on error
Node* openTreeParentDirectory(Node* current_directory);

#endif // NS_FOLDERS_H