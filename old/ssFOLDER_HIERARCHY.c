#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h> // For boolean flags

/**
 * @brief Defines the type of a node in the file system tree.
 */
typedef enum {
    NODE_ROOT,
    NODE_FOLDER,
    NODE_FILE
} NodeType;

/**
 * @brief Represents a single node (file or folder) in the tree.
 */
typedef struct Node {
    char* name;             // Filename or foldername ("ROOT" for root)
    NodeType type;          // ROOT, FOLDER, or FILE
    struct Node* parent;    // Pointer to the parent node (NULL for root)
    struct Node** children; // Dynamic array of child node pointers
    int child_count;        // Number of children currently held
    int capacity;           // Max number of children before realloc
} Node;

// --- Global State ---
Node* root_node = NULL;
Node* current_directory = NULL;

// --- Forward Declarations of Helper Functions ---
Node* createNode(const char* name, NodeType type, Node* parent);
void addChild(Node* parent, Node* child);
Node* findChildByName(Node* parent, const char* name);
Node* findChild(Node* parent, const char* name, NodeType type);
Node* removeChild(Node* parent, Node* child);
void freeTree(Node* node);
void createFile(const char* filename); // Helper to make the demo work

// --- Initialization ---

/**
 * @brief Allocates and initializes a new Node.
 */
Node* createNode(const char* name, NodeType type, Node* parent) {
    Node* newNode = (Node*)malloc(sizeof(Node));
    if (!newNode) {
        perror("Error: Failed to allocate memory for node");
        exit(1);
    }

    newNode->name = strdup(name);
    if (!newNode->name) {
        perror("Error: Failed to allocate memory for node name");
        free(newNode);
        exit(1);
    }

    newNode->type = type;
    newNode->parent = parent;
    newNode->children = NULL;
    newNode->child_count = 0;
    newNode->capacity = 0;

    // Root and Folders can have children
    if (type == NODE_ROOT || type == NODE_FOLDER) {
        newNode->capacity = 4; // Initial capacity of 4 children
        newNode->children = (Node**)malloc(newNode->capacity * sizeof(Node*));
        if (!newNode->children) {
            perror("Error: Failed to allocate memory for children array");
            free(newNode->name);
            free(newNode);
            exit(1);
        }
    }
    return newNode;
}

/**
 * @brief Initializes the file system with a ROOT node.
 */
void initFileSystem() {
    root_node = createNode("ROOT", NODE_ROOT, NULL);
    current_directory = root_node;
    printf("File system initialized. Current directory: ROOT\n");
}

// --- Helper Functions ---

/**
 * @brief Adds a child node to a parent node, resizing the children array if needed.
 */
void addChild(Node* parent, Node* child) {
    if (parent->type == NODE_FILE) {
        printf("Error: Cannot add child to a FILE node.\n");
        return;
    }

    // Resize children array if full
    if (parent->child_count == parent->capacity) {
        parent->capacity = (parent->capacity == 0) ? 4 : parent->capacity * 2;
        Node** newChildren = (Node**)realloc(parent->children, parent->capacity * sizeof(Node*));
        if (!newChildren) {
            perror("Error: Failed to reallocate children array");
            return;
        }
        parent->children = newChildren;
    }

    // Add the new child
    parent->children[parent->child_count] = child;
    parent->child_count++;
    child->parent = parent; // Set the parent pointer
}

/**
 * @brief Finds a child node by name, regardless of type.
 * @return Pointer to the child node or NULL if not found.
 */
Node* findChildByName(Node* parent, const char* name) {
    if (parent->type == NODE_FILE) return NULL;
    for (int i = 0; i < parent->child_count; i++) {
        if (strcmp(parent->children[i]->name, name) == 0) {
            return parent->children[i];
        }
    }
    return NULL;
}

/**
 * @brief Finds a child node by name and a specific type.
 * @return Pointer to the child node or NULL if not found.
 */
Node* findChild(Node* parent, const char* name, NodeType type) {
    Node* child = findChildByName(parent, name);
    if (child && child->type == type) {
        return child;
    }
    return NULL;
}

/**
 * @brief Removes a child from a parent's children array *without* freeing the child.
 * @return The pointer to the removed child, or NULL on failure.
 */
Node* removeChild(Node* parent, Node* child) {
    if (parent->type == NODE_FILE) return NULL;
    
    int foundIndex = -1;
    for (int i = 0; i < parent->child_count; i++) {
        if (parent->children[i] == child) {
            foundIndex = i;
            break;
        }
    }

    if (foundIndex == -1) {
        printf("Error: Child not found in parent's list (internal error).\n");
        return NULL;
    }

    // Shift all subsequent elements left by one
    for (int i = foundIndex; i < parent->child_count - 1; i++) {
        parent->children[i] = parent->children[i + 1];
    }

    parent->children[parent->child_count - 1] = NULL; // Clear the last (now duplicate) pointer
    parent->child_count--;
    
    child->parent = NULL; // Detach child from parent
    return child;
}

/**
 * @brief Recursively frees the entire tree from memory.
 */
void freeTree(Node* node) {
    if (!node) return;

    // Recursively free all children
    if (node->type == NODE_ROOT || node->type == NODE_FOLDER) {
        for (int i = 0; i < node->child_count; i++) {
            freeTree(node->children[i]);
        }
        free(node->children); // Free the children array itself
    }

    // Free the node's components
    free(node->name);
    free(node);
}

/**
 * @brief Helper function to create a file in the current directory.
 * (Not a specified command, but necessary for MOVE/UPMOVE to be testable).
 */
void createFile(const char* filename) {
    if (findChildByName(current_directory, filename) != NULL) {
        printf("Error: Name '%s' already exists in current directory.\n", filename);
        return;
    }
    Node* newFile = createNode(filename, NODE_FILE, current_directory);
    addChild(current_directory, newFile);
    printf("File '%s' created.\n", filename);
}


// --- 1. CREATEFOLDER ---
/**
 * @brief Command: CREATEFOLDER <foldername>
 * Creates a new folder in the current directory.
 */
void createFolder(const char* foldername) {
    if (strcmp(foldername, "ROOT") == 0) {
        printf("Error: Cannot create folder with reserved name 'ROOT'.\n");
        return;
    }
    if (findChildByName(current_directory, foldername) != NULL) {
        printf("Error: Name '%s' already exists in current directory.\n", foldername);
        return;
    }

    Node* newFolder = createNode(foldername, NODE_FOLDER, current_directory);
    addChild(current_directory, newFolder);
    printf("Folder '%s' created.\n", foldername);
}

// --- 2. & 3. VIEWFOLDER ---
/**
 * @brief Command: VIEWFOLDER <foldername> | VIEWFOLDER ROOT
 * Lists all files and subfolders in the specified folder or ROOT.
 */
void viewFolder(const char* foldername) {
    Node* folderToView = NULL;

    if (strcmp(foldername, "ROOT") == 0) {
        folderToView = root_node;
    } else {
        // Search for the folder within the *current* directory
        folderToView = findChild(current_directory, foldername, NODE_FOLDER);
        if (folderToView == NULL) {
            printf("Error: Folder '%s' not found in current directory.\n", foldername);
            return;
        }
    }

    printf("--- Contents of '%s' ---\n", foldername);
    if (folderToView->child_count == 0) {
        printf(" (empty)\n");
        return;
    }

    for (int i = 0; i < folderToView->child_count; i++) {
        Node* child = folderToView->children[i];
        if (child->type == NODE_FILE) {
            printf("  FILE: %s\n", child->name);
        } else if (child->type == NODE_FOLDER) {
            printf("  DIR : %s/\n", child->name);
        }
    }
    printf("---------------------------\n");
}

// --- 4. MOVE ---
/**
 * @brief Command: MOVE <filename> <foldername>
 * Moves a file into a subfolder, both within the current directory.
 */
void moveFile(const char* filename, const char* foldername) {
    // Find the file in the current directory
    Node* fileToMove = findChild(current_directory, filename, NODE_FILE);
    if (!fileToMove) {
        printf("Error: File '%s' not found in current directory.\n", filename);
        return;
    }

    // Find the target folder in the current directory
    Node* targetFolder = findChild(current_directory, foldername, NODE_FOLDER);
    if (!targetFolder) {
        printf("Error: Target folder '%s' not found in current directory.\n", foldername);
        return;
    }

    // Check for name collision in the target folder
    if (findChildByName(targetFolder, filename) != NULL) {
        printf("Error: A file or folder named '%s' already exists in '%s'.\n", 
               filename, foldername);
        return;
    }

    // 1. Detach from current directory
    removeChild(current_directory, fileToMove);
    
    // 2. Attach to target folder
    addChild(targetFolder, fileToMove);

    printf("Moved '%s' into '%s'.\n", filename, foldername);
}

// --- 5. UPMOVE ---
/**
 * @brief Command: UPMOVE <filename>
 * Moves a file from the current directory into its parent directory.
 */
void upMoveFile(const char* filename) {
    // Check for "no parent folder" (i.e., in ROOT)
    if (current_directory == root_node) {
        printf("Error: Cannot 'upmove' a file from the ROOT directory itself.\n");
        return;
    }

    // Check for "no parent folder" (i.e., current folder is a child of ROOT)
    if (current_directory->parent == root_node) {
        printf("Error: Cannot move file up. Current folder '%s' is in the ROOT directory (no parent folder).\n", 
               current_directory->name);
        return;
    }

    // Find the file to move
    Node* fileToMove = findChild(current_directory, filename, NODE_FILE);
    if (!fileToMove) {
        printf("Error: File '%s' not found in current directory.\n", filename);
        return;
    }

    Node* targetParent = current_directory->parent;
    if (targetParent == NULL) {
        // This should be impossible if logic above is correct, but good safeguard.
        printf("Error: Critical - current directory has no parent link.\n");
        return;
    }

    // Check for name collision in the parent
    if (findChildByName(targetParent, filename) != NULL) {
        printf("Error: A file/folder named '%s' already exists in parent ('%s').\n", 
               filename, targetParent->name);
        return;
    }

    // 1. Detach from current directory
    removeChild(current_directory, fileToMove);

    // 2. Attach to parent directory
    addChild(targetParent, fileToMove);

    printf("Moved '%s' up to '%s'.\n", filename, targetParent->name);
}

// --- 6. OPEN ---
/**
 * @brief Command: OPEN -flags <foldername>
 * Opens a folder in the current directory and switches to it.
 * @param createIfMissing Corresponds to the '-c' flag.
 */
void openFolder(const char* foldername, bool createIfMissing) {
    if (strcmp(foldername, "ROOT") == 0) {
        printf("Error: Cannot 'open' ROOT. Current directory is already a child of ROOT.\n");
        return;
    }

    Node* folderToOpen = findChild(current_directory, foldername, NODE_FOLDER);

    if (folderToOpen == NULL) {
        if (createIfMissing) {
            printf("Folder '%s' not found. Creating...\n", foldername);
            // Check for file/folder name collision before creating
            if (findChildByName(current_directory, foldername) != NULL) {
                printf("Error: Cannot create folder. Name '%s' already exists.\n", foldername);
                return;
            }
            // Create and add the new folder
            folderToOpen = createNode(foldername, NODE_FOLDER, current_directory);
            addChild(current_directory, folderToOpen);
            printf("Folder '%s' created.\n", foldername);
        } else {
            printf("Error: Folder '%s' not found in current directory.\n", foldername);
            return;
        }
    }

    // Switch the global current directory
    current_directory = folderToOpen;
    printf("Current directory changed to '%s'.\n", current_directory->name);
}

// --- 7. OPENPARENT --- (Listed as 8 in prompt)
/**
 * @brief Command: OPENPARENT <foldername>
 * Opens the parent folder of the current folder.
 * (Note: Per prompt, this fails if the parent is ROOT).
 */
void openParentDirectory() {
    // Error if in ROOT
    if (current_directory == root_node) {
        printf("Error: Already in ROOT directory. No parent folder to open.\n");
        return;
    }

    // Error if current folder is a child of ROOT
    if (current_directory->parent == root_node) {
        printf("Error: Current folder '%s' is in ROOT. No parent folder to open.\n", 
               current_directory->name);
        return;
    }

    if (current_directory->parent == NULL) {
        printf("Error: Critical - current directory has no parent link.\n");
        return;
    }

    // Switch to the parent
    current_directory = current_directory->parent;
    printf("Current directory changed to '%s'.\n", current_directory->name);
}


// --- Main function to demonstrate usage ---
int main() {
    initFileSystem();

    printf("\n--- Test 1: Create folders and files in ROOT ---\n");
    createFolder("Documents");
    createFolder("Pictures");
    createFile("readme.txt");
    viewFolder("ROOT");

    printf("\n--- Test 2: MOVE file into a folder ---\n");
    moveFile("readme.txt", "Documents");
    viewFolder("ROOT");
    viewFolder("Documents"); // This fails (Documents is not in ROOT)
    
    printf("\n--- Test 3: OPEN folder and check contents ---\n");
    openFolder("Documents", false); // Switch to "Documents"
    printf("Current directory is now: %s\n", current_directory->name);
    // Now viewFolder("Documents") will fail, because it looks *inside* Documents
    // Let's create a file here
    createFile("report.doc");
    
    // To view the contents of "Documents", we need to view its parent (ROOT)
    // This file system design is a bit tricky!
    // Let's add a "viewCurrentFolder" helper for testing.
    printf("--- Contents of current folder (%s) ---\n", current_directory->name);
    for(int i=0; i < current_directory->child_count; i++) {
        printf("  %s: %s\n", 
            current_directory->children[i]->type == NODE_FILE ? "FILE" : "DIR",
            current_directory->children[i]->name);
    }
    printf("----------------------------------------\n");


    printf("\n--- Test 4: Test UPMOVE and OPENPARENT (should fail) ---\n");
    upMoveFile("report.doc");      // Fails, parent is ROOT
    openParentDirectory();       // Fails, parent is ROOT

    printf("\n--- Test 5: Create nested folders and test working UPMOVE ---\n");
    openFolder("Projects", true); // Create and open "Projects" (inside "Documents")
    printf("Current directory is now: %s\n", current_directory->name);
    createFile("game.c");
    
    // Now we are in "Projects", parent is "Documents", grandparent is "ROOT"
    // UPMOVE should work.
    upMoveFile("game.c");
    
    // Check that "game.c" is no longer in "Projects"
    printf("--- Contents of current folder (%s) ---\n", current_directory->name);
    for(int i=0; i < current_directory->child_count; i++) {
        printf("  %s: %s\n", 
            current_directory->children[i]->type == NODE_FILE ? "FILE" : "DIR",
            current_directory->children[i]->name);
    }
    printf("----------------------------------------\n");

    // OPENPARENT should also work
    openParentDirectory(); // Should move back to "Documents"
    printf("Current directory is now: %s\n", current_directory->name);

    // Check "Documents" to see if "game.c" arrived
    printf("--- Contents of current folder (%s) ---\n", current_directory->name);
    for(int i=0; i < current_directory->child_count; i++) {
        printf("  %s: %s\n", 
            current_directory->children[i]->type == NODE_FILE ? "FILE" : "DIR",
            current_directory->children[i]->name);
    }
    printf("----------------------------------------\n");

    // --- Cleanup ---
    printf("\nCleaning up file system...\n");
    freeTree(root_node);
    return 0;
}