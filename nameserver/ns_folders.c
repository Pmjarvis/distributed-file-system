#include "ns_folders.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../common/protocol.h"
// --- Initialization ---

Node* createNode(const char* name, NodeType type, Node* parent) {
    Node* newNode = (Node*)malloc(sizeof(Node));
    if (!newNode) return NULL;

    newNode->name = strdup(name);
    if (!newNode->name) {
        free(newNode);
        return NULL;
    }

    newNode->type = type;
    newNode->parent = parent;
    newNode->children = NULL;
    newNode->child_count = 0;
    newNode->capacity = 0;

    if (type == NODE_ROOT || type == NODE_FOLDER) {
        newNode->capacity = 4;
        newNode->children = (Node**)malloc(newNode->capacity * sizeof(Node*));
        if (!newNode->children) {
            free(newNode->name);
            free(newNode);
            return NULL;
        }
    }
    return newNode;
}

// --- Helper Functions ---

void addChild(Node* parent, Node* child) {
    if (parent->type == NODE_FILE) return;

    if (parent->child_count == parent->capacity) {
        parent->capacity = (parent->capacity == 0) ? 4 : parent->capacity * 2;
        Node** newChildren = (Node**)realloc(parent->children, parent->capacity * sizeof(Node*));
        if (!newChildren) return;
        parent->children = newChildren;
    }
    parent->children[parent->child_count] = child;
    parent->child_count++;
    child->parent = parent;
}

Node* findChildByName(Node* parent, const char* name) {
    if (parent->type == NODE_FILE) return NULL;
    for (int i = 0; i < parent->child_count; i++) {
        if (strcmp(parent->children[i]->name, name) == 0) {
            return parent->children[i];
        }
    }
    return NULL;
}

Node* findChild(Node* parent, const char* name, NodeType type) {
    Node* child = findChildByName(parent, name);
    if (child && child->type == type) {
        return child;
    }
    return NULL;
}

Node* removeChild(Node* parent, Node* child) {
    if (parent->type == NODE_FILE) return NULL;
    
    int foundIndex = -1;
    for (int i = 0; i < parent->child_count; i++) {
        if (parent->children[i] == child) {
            foundIndex = i;
            break;
        }
    }

    if (foundIndex == -1) return NULL;

    for (int i = foundIndex; i < parent->child_count - 1; i++) {
        parent->children[i] = parent->children[i + 1];
    }
    parent->children[parent->child_count - 1] = NULL;
    parent->child_count--;
    child->parent = NULL;
    return child;
}

void freeTree(Node* node) {
    if (!node) return;
    if (node->type == NODE_ROOT || node->type == NODE_FOLDER) {
        for (int i = 0; i < node->child_count; i++) {
            freeTree(node->children[i]);
        }
        free(node->children);
    }
    free(node->name);
    free(node);
}

bool createTreeFile(Node* current_directory, const char* filename) {
    if (findChildByName(current_directory, filename) != NULL) {
        return false;
    }
    Node* newFile = createNode(filename, NODE_FILE, current_directory);
    if (!newFile) return false;
    addChild(current_directory, newFile);
    return true;
}

// --- 1. CREATEFOLDER ---
const char* createTreeFolder(Node* current_directory, const char* foldername) {
    if (strcmp(foldername, "ROOT") == 0) {
        return "Error: Cannot create folder with reserved name 'ROOT'.";
    }
    if (strchr(foldername, '/') != NULL) {
        return "Error: Folder name cannot contain '/'.";
    }
    if (findChildByName(current_directory, foldername) != NULL) {
        return "Error: Name already exists in current directory.";
    }

    Node* newFolder = createNode(foldername, NODE_FOLDER, current_directory);
    if (!newFolder) return "Error: Failed to allocate memory.";
    
    addChild(current_directory, newFolder);
    return NULL; // Success
}

// --- 2. & 3. VIEWFOLDER ---
char* viewTreeFolder(Node* current_directory) {
    if (!current_directory) {
        return strdup("Error: Invalid directory.");
    }

    size_t buf_size = 1024;
    char* buffer = (char*)malloc(buf_size);
    if (!buffer) return NULL;
    
    // Show the current directory name
    const char* dir_name = (current_directory->type == NODE_ROOT) ? "ROOT" : current_directory->name;
    snprintf(buffer, buf_size, "--- Contents of '%s' ---\n", dir_name);
    
    if (current_directory->child_count == 0) {
        strncat(buffer, " (empty)\n", buf_size - strlen(buffer) - 1);
    } else {
        for (int i = 0; i < current_directory->child_count; i++) {
            Node* child = current_directory->children[i];
            char line[MAX_PATH + 10];
            if (child->type == NODE_FILE) {
                snprintf(line, sizeof(line), "  FILE: %s\n", child->name);
            } else if (child->type == NODE_FOLDER) {
                snprintf(line, sizeof(line), "  DIR : %s/\n", child->name);
            }
            
            if (strlen(buffer) + strlen(line) + 1 > buf_size) {
                buf_size *= 2;
                char* new_buffer = (char*)realloc(buffer, buf_size);
                if (!new_buffer) {
                    free(buffer);
                    return NULL;
                }
                buffer = new_buffer;
            }
            strncat(buffer, line, buf_size - strlen(buffer) - 1);
        }
    }
    strncat(buffer, "---------------------------\n", buf_size - strlen(buffer) - 1);
    return buffer;
}

// --- 4. MOVE ---
const char* moveTreeFile(Node* current_directory, const char* filename, const char* foldername) {
    Node* fileToMove = findChild(current_directory, filename, NODE_FILE);
    if (!fileToMove) {
        return "Error: File not found in current directory.";
    }
    Node* targetFolder = findChild(current_directory, foldername, NODE_FOLDER);
    if (!targetFolder) {
        return "Error: Target folder not found in current directory.";
    }
    if (findChildByName(targetFolder, filename) != NULL) {
        return "Error: A file or folder with that name already exists in target.";
    }
    removeChild(current_directory, fileToMove);
    addChild(targetFolder, fileToMove);
    return NULL; // Success
}

// --- 5. UPMOVE ---
const char* upMoveTreeFile(Node* current_directory, const char* filename) {
    if (current_directory->parent == NULL) { // In ROOT
        return "Error: Cannot 'upmove' a file from the ROOT directory.";
    }
    if (current_directory->parent->type == NODE_ROOT) {
        return "Error: Cannot move file up. Current folder is in the ROOT directory.";
    }

    Node* fileToMove = findChild(current_directory, filename, NODE_FILE);
    if (!fileToMove) {
        return "Error: File not found in current directory.";
    }
    Node* targetParent = current_directory->parent;
    if (targetParent == NULL) {
        return "Error: Critical - current directory has no parent link.";
    }
    if (findChildByName(targetParent, filename) != NULL) {
        return "Error: A file/folder with that name already exists in parent.";
    }
    removeChild(current_directory, fileToMove);
    addChild(targetParent, fileToMove);
    return NULL; // Success
}

// --- 6. OPEN ---
Node* openTreeFolder(Node* current_directory, const char* foldername, bool createIfMissing) {
    if (strcmp(foldername, "ROOT") == 0) {
        return NULL; // Error
    }
    Node* folderToOpen = findChild(current_directory, foldername, NODE_FOLDER);
    if (folderToOpen == NULL) {
        if (createIfMissing) {
            if (findChildByName(current_directory, foldername) != NULL) {
                return NULL; // Name collision
            }
            folderToOpen = createNode(foldername, NODE_FOLDER, current_directory);
            if (!folderToOpen) return NULL;
            addChild(current_directory, folderToOpen);
        } else {
            return NULL; // Not found, not creating
        }
    }
    return folderToOpen; // Success
}

// --- 7. OPENPARENT ---
// Returns parent directory on success, NULL on error
Node* openTreeParentDirectory(Node* current_directory) {
    // Can't go up if we're already in ROOT
    if (current_directory->type == NODE_ROOT) {
        return NULL; // Already in ROOT, can't go up further
    }
    
    // Parent should always exist (at minimum, ROOT is the parent)
    if (current_directory->parent == NULL) {
        return NULL; // Should never happen in a well-formed tree
    }
    
    // Return parent (could be ROOT or another folder)
    return current_directory->parent;
}

Node* resolvePath(Node* root, Node* current, const char* path) {
    if (!path || strlen(path) == 0) return current;
    
    Node* currNode = current;
    
    // Check for absolute path indicators
    if (path[0] == '/') {
        currNode = root;
    }
    
    char* pathCopy = strdup(path);
    if (!pathCopy) return NULL;

    char* saveptr;
    char* token = strtok_r(pathCopy, "/", &saveptr);
    
    // Check if first token is ROOT (also absolute path)
    if (token && strcmp(token, "ROOT") == 0) {
        currNode = root;
        token = strtok_r(NULL, "/", &saveptr);
    }
    
    while (token) {
        if (strcmp(token, ".") == 0) {
            // Stay
        } else if (strcmp(token, "..") == 0) {
            if (currNode->parent) currNode = currNode->parent;
        } else {
            Node* next = findChildByName(currNode, token);
            if (!next || next->type == NODE_FILE) {
                free(pathCopy);
                return NULL;
            }
            currNode = next;
        }
        token = strtok_r(NULL, "/", &saveptr);
    }
    
    free(pathCopy);
    return currNode;
}
