[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/0ek2UV58)

Implementing ACCESS CONTROL DATA STRUCTURE in nm.c:
- Using nested HASH TABLES (2 level)
- Hash functions used: FNV-1a and djb2
- FNV-1a is primary, djb2 is secondary (used for handling collisions i.e. open addressing w/ double )
- Collisions handled using OPEN ADDRESSING W/ DOUBLE HASHING
- hash function hashes the entire path string
- for persistance, all data is stored in "permission_db", a separate file for each user containing one line
for each file that user has some permissions for, and the corresponding permissions.
- hash function hashes the entire path string
- This persistence model is simple and human-readable, but it assumes that filenames and permissions do not contain the | character or newline characters. [because it uses '|' to separate filename and perms in each line of the file for a specific user]

Implementing SS<->FILE MAPPING:
- Array of hash tables, one hash table for each SS
- Similar hash table architecture to access control data structure (but not nested)
'''ADDING OF NEW SS BEYOND MAX_NUMBER_OF_SS HAS NOT YET BEEN IMPLEMENTED'''

Implementing HIERARCHICAL FOLDER STRUCTURE:
Used a tree
each node can have any number of children
have an array for children in each node, and a parent pointer in each node
only a "folder" node has children
leaf nodes represent files, having a "filename" (char*)
root node of the entire tree is not a folder neither a file, just "root"
this is the default directory of the user. any folder inside the root directory (i.e. a "folder" node that is a child of the root has no parent folder)
the following operations have to be implemented as functions that search through/update/manipulate the tree data structure:


1. CREATEFOLDER <foldername> # Creates a new folder
2. VIEWFOLDER <foldername> # Lists all files in the specified folder
3. VIEWFOLDER ROOT -> to view the root directory (also no folder can be named ROOT)
4. MOVE <filename> <foldername> # Moves the file to the specified folder. This action only works when both <filename> and <foldername> are present in the same directory

5. UPMOVE <filename>. # Moves the file into parent folder of current folder. Print error message if no parent folder (i.e. the file was present in root directory)

6. OPEN -flags <foldername> # opens a folder in the current directory and switches
FLAGS:
-c # also creates the folder if folder is not already created

8. OPENPARENT <foldername> # opens the parent folder. Print error message if no parent folder (i.e. current folder was in root directory) 