Distributed File System (DFS)
A robust, multi-component distributed system designed for concurrent file operations, featuring a centralized naming authority and scalable storage servers.

Core Components
Naming Server (Central Coordinator): Manages the directory hierarchy and maps file paths to specific storage servers. Orchestrates communication to ensure data consistency.

Storage Servers: Distributed nodes responsible for data persistence. Designed to handle concurrent read/write requests from multiple clients.

User Clients: A command-line interface allowing users to perform standard file operations (Create, Read, Write, Delete, Info) across the distributed network.

Key Technical Features
Dynamic Scalability: Supports hot-swapping of Storage Servers and Clients; nodes can disconnect and reconnect without crashing the system.

Concurrency: Implemented thread-safe operations to allow multiple clients to interact with the Name Server and Storage Servers simultaneously.

Network Communication: Custom protocol built on top of TCP sockets for reliable message passing between components.
