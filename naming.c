#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>

// Function declarations for Trie operations
void initialize_trie();
void insert_path(const char *path, int server_index);
int delete_path(const char *path);

// Function to get system IP (unchanged)
void getsystemip(char *buffer) {
    char cmd[] = "hostname -I | awk '{print $1}'";
    FILE *fp = popen(cmd, "r");
    if (fp == NULL) {
        perror("popen failed");
        exit(1);
    }
    fgets(buffer, 16, fp);
    pclose(fp);
}

// Structure declarations and global variables (unchanged)
struct StorageServerInfo {
    char type[8];     // Type of server (e.g., "storage")
    char ip[16];
    int nm_port;      // Dedicated port for NM connection
    int client_port;  // Port for clients to connect
    char file_paths[100][256]; // Array to store file paths
    int file_count;   // Number of file paths
};

struct ClientInfo {
    char ip[16];
    int port;
};

#define MAX_STORAGE_SERVERS 10
#define MAX_CLIENTS 10

struct StorageServerInfo storage_servers[MAX_STORAGE_SERVERS];
int storage_server_count = 0;

struct ClientInfo clients[MAX_CLIENTS];
int client_count = 0;

// Global array to store storage server sockets
int storage_sockets[MAX_STORAGE_SERVERS];

// Function declarations (unchanged, paste as is)
int find_storage_server_by_ip_port(const char *ip, int port);
int find_storage_server_by_path(const char *path, char *ip, int *port);
int find_storage_server_index_by_path(const char *path);
int find_storage_server_index_by_parent(const char *path);

// Thread function to handle communication with a storage server (unchanged)
// Thread function to handle communication with a storage server
void *handle_storage_server(void *arg) {
    int storage_socket = *(int *)arg;
    free(arg); // Free the allocated memory for the socket descriptor
    struct StorageServerInfo info;

    // Receive the storage server info
    read(storage_socket, &info, sizeof(info));
    printf("Received storage server info:\n");
    printf("Type: %s\n", info.type);
    printf("IP Address: %s\n", info.ip);
    printf("NM Port: %d\n", info.nm_port);
    printf("Client Port: %d\n", info.client_port);
    printf("File Paths:\n");
    for (int i = 0; i < info.file_count; i++) {
        printf("%s\n", info.file_paths[i]);
    }

    // Store the storage server info and socket
    if (storage_server_count < MAX_STORAGE_SERVERS) {
        storage_servers[storage_server_count] = info;
        storage_sockets[storage_server_count] = storage_socket;
        storage_server_count++;
        printf("Storage server info stored\n");
    } else {
        printf("Storage server limit reached\n");
    }

    for (int i = 0; i < info.file_count; i++) {
        insert_path(info.file_paths[i], storage_server_count - 1); // Correct server index
    }

    // Keep the connection open to handle future instructions
    while (1) {
        char buffer[256];
        int bytes_received = recv(storage_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received > 0) {
            buffer[bytes_received] = '\0'; // Null-terminate the received string
            printf("Received from storage server: %s\n", buffer);
        } else if (bytes_received == 0) {
            printf("Storage server disconnected\n");
            break;
        } else {
            perror("recv");
            break;
        }
    }

    close(storage_socket);
    return NULL;
}

// Thread function to handle communication with a client
void *handle_client(void *arg) {
    int client_socket = *(int *)arg;
    free(arg); // Free the allocated memory for the socket descriptor
    struct ClientInfo info;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    // Get client IP and port
    getpeername(client_socket, (struct sockaddr *)&addr, &addr_len);
    inet_ntop(AF_INET, &addr.sin_addr, info.ip, sizeof(info.ip));
    info.port = ntohs(addr.sin_port);

    // Store the client info
    if (client_count < MAX_CLIENTS) {
        clients[client_count++] = info;
        printf("Client connected: %s:%d\n", info.ip, info.port);
    } else {
        printf("Client limit reached\n");
    }

    // Receive and print requests from the client, then send an ACK
    char buffer[256];
    while (1) {
        int bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received > 0) {
            buffer[bytes_received] = '\0'; // Null-terminate the received string
            printf("Received from client: %s\n", buffer);

            if (strcmp(buffer, "paths") == 0) {
                // Handle 'paths' command
                char paths[1024] = "";
                for (int i = 0; i < storage_server_count; i++) {
                    for (int j = 0; j < storage_servers[i].file_count; j++) {
                        strcat(paths, storage_servers[i].file_paths[j]);
                        strcat(paths, "\n");
                    }
                }
                // Send the paths back to client
                send(client_socket, paths, strlen(paths), 0);
            } else if (strncmp(buffer, "stored ", 7) == 0) {
                // Handle 'stored <path>' command
                char path[256];
                sscanf(buffer + 7, "%s", path);
                char ip[16];
                int port;
                if (find_storage_server_by_path(path, ip, &port) == 0) {
                    char response[512]; // Increased buffer size
                    snprintf(response, sizeof(response), "Path %s is stored on server %s:%d", path, ip, port);
                    send(client_socket, response, strlen(response), 0);
                } else {
                    char response[] = "Path not found";
                    send(client_socket, response, strlen(response), 0);
                }
            } else if (strncmp(buffer, "CREATE ", 7) == 0 || strncmp(buffer, "CREATE_DIR ", 11) == 0) {
                // Handle 'CREATE <path> <name>' and 'CREATE_DIR <path> <name>' commands
                char command[16], path[256], name[256];
                sscanf(buffer, "%s %s %s", command, path, name);

                // Check if the parent path exists
                int parent_exists = 0;
                for (int i = 0; i < storage_server_count; i++) {
                    for (int j = 0; j < storage_servers[i].file_count; j++) {
                        if (strcmp(storage_servers[i].file_paths[j], path) == 0) {
                            parent_exists = 1;
                            break;
                        }
                    }
                    if (parent_exists) {
                        break;
                    }
                }

                if (!parent_exists) {
                    send(client_socket, "Parent path not found", 21, 0);
                    continue;
                }

                // Check if the full path already exists
                char full_path[512];
                snprintf(full_path, sizeof(full_path), "%s/%s", path, name);

                int path_exists = 0;
                for (int i = 0; i < storage_server_count; i++) {
                    for (int j = 0; j < storage_servers[i].file_count; j++) {
                        if (strcmp(storage_servers[i].file_paths[j], full_path) == 0) {
                            path_exists = 1;
                            break;
                        }
                    }
                    if (path_exists) {
                        break;
                    }
                }

                if (path_exists) {
                    send(client_socket, "Path already exists", 19, 0);
                    continue;
                }

                int server_index = 0; // Use the first storage server for simplicity
                int storage_socket = storage_sockets[server_index];
                // Send the command to the storage server
                send(storage_socket, buffer, strlen(buffer), 0);

                // Receive acknowledgment from the storage server
                char ack[256];
                int bytes_received = recv(storage_socket, ack, sizeof(ack) - 1, 0);
                if (bytes_received > 0) {
                    ack[bytes_received] = '\0'; // Null-terminate the received string
                    if (strcmp(ack, "Success") == 0) {
                        // Add the new path to the list of accessible paths
                        strncpy(storage_servers[server_index].file_paths[storage_servers[server_index].file_count++], full_path, sizeof(storage_servers[server_index].file_paths[0]));
                        insert_path(full_path, server_index); // Insert the new path into the Trie
                        send(client_socket, "Success", 7, 0);
                    } else {
                        send(client_socket, "Failed to create", 16, 0);
                    }
                } else {
                    send(client_socket, "Failed to receive acknowledgment", 31, 0);
                }
            } else if (strncmp(buffer, "delete ", 7) == 0) {
                // Handle 'delete <path>' command
                char path[256];
                sscanf(buffer + 7, "%s", path);
                int server_index = find_storage_server_index_by_path(path);
                if (server_index >= 0) {
                    int storage_socket = storage_sockets[server_index];
                    // Send the command to the storage server via existing socket
                    send(storage_socket, buffer, strlen(buffer), 0);

                    // Receive acknowledgment from the storage server
                    char ack[256];
                    int bytes_received = recv(storage_socket, ack, sizeof(ack) - 1, 0);
                    if (bytes_received > 0) {
                        ack[bytes_received] = '\0'; // Null-terminate the received string
                        if (strcmp(ack, "Success") == 0) {
                            // Remove the path from the list of accessible paths
                            for (int i = 0; i < storage_servers[server_index].file_count; i++) {
                                if (strcmp(storage_servers[server_index].file_paths[i], path) == 0) {
                                    for (int j = i; j < storage_servers[server_index].file_count - 1; j++) {
                                        strcpy(storage_servers[server_index].file_paths[j], storage_servers[server_index].file_paths[j + 1]);
                                    }
                                    storage_servers[server_index].file_count--;
                                    break;
                                }
                            }
                            delete_path(path); // Delete the path from the Trie
                            send(client_socket, "Success", 7, 0);
                        } else {
                            send(client_socket, "Failed to delete", 16, 0);
                        }
                    } else {
                        send(client_socket, "Failed to receive acknowledgment", 31, 0);
                    }
                } else {
                    send(client_socket, "Path not found", 14, 0);
                }
            } else if (strncmp(buffer, "read ", 5) == 0 || strncmp(buffer, "write ", 6) == 0 || strncmp(buffer, "info ", 5) == 0 || strncmp(buffer, "stream ", 7) == 0) {
                // Handle 'read <path>', 'write <path>', 'info <path>', 'stream <path>' commands
                char path[256];
                sscanf(buffer + (strncmp(buffer, "read ", 5) == 0 ? 5 : (strncmp(buffer, "write ", 6) == 0 ? 6 : (strncmp(buffer, "info ", 5) == 0 ? 5 : 7))), "%s", path);
                char ip[16];
                int port;
                if (find_storage_server_by_path(path, ip, &port) == 0) {
                    char response[512]; // Increased buffer size
                    snprintf(response, sizeof(response), "Path %s is stored on server %s:%d", path, ip, port);
                    send(client_socket, response, strlen(response), 0);
                } else {
                    char response[] = "Path not found";
                    send(client_socket, response, strlen(response), 0);
                }
            } else {
                // Send an ACK back to the client
                char ack[] = "ACK";
                send(client_socket, ack, strlen(ack), 0);
            }
        } else if (bytes_received == 0) {
            printf("Client disconnected\n");
            break;
        } else {
            perror("recv");
            break;
        }

        // Clear the buffer for the next input
        memset(buffer, 0, sizeof(buffer));
    }

    close(client_socket);
    return NULL;
}


// Main function logic (partially changed for clarity)
int main() {
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    char ip[16];

    // Fetch system IP (unchanged)
    getsystemip(ip);
    printf("System IP: %s\n", ip);

    // Create socket and bind to port (unchanged)
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(5050);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    printf("Listening on port 5050\n");

    // Start listening for connections (unchanged)
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("Waiting for connections...\n");

    initialize_trie(); // Initialize the Trie

    while (1) {
        int *new_socket = malloc(sizeof(int));
        if ((*new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen)) < 0) {
            perror("accept");
            free(new_socket);
            continue;
        }

        // Handle connection type: storage or client (unchanged)
        char buffer[8];
        int bytes_received = recv(*new_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            perror("recv");
            close(*new_socket);
            free(new_socket);
            continue;
        }
        buffer[bytes_received] = '\0';

        if (strcmp(buffer, "storage") == 0) {
            pthread_t thread_id;
            if (pthread_create(&thread_id, NULL, handle_storage_server, new_socket) != 0) {
                perror("pthread_create");
                close(*new_socket);
                free(new_socket);
                continue;
            }
            pthread_detach(thread_id);
        } else if (strcmp(buffer, "client") == 0) {
            pthread_t thread_id;
            if (pthread_create(&thread_id, NULL, handle_client, new_socket) != 0) {
                perror("pthread_create");
                close(*new_socket);
                free(new_socket);
                continue;
            }
            pthread_detach(thread_id);
        } else {
            printf("Unknown connection type: %s\n", buffer);
            close(*new_socket);
            free(new_socket);
        }
    }

    close(server_fd);
    return 0;
}


// Trie Node Definition
struct TrieNode {
    struct TrieNode *children[256]; // Support all ASCII characters
    int server_index;              // Index of the storage server storing this path
    int is_end_of_path;            // Flag indicating the end of a valid path
};

// Create a new Trie Node
struct TrieNode *create_trie_node() {
    struct TrieNode *node = (struct TrieNode *)malloc(sizeof(struct TrieNode));
    for (int i = 0; i < 256; i++) {
        node->children[i] = NULL;
    }
    node->server_index = -1;
    node->is_end_of_path = 0;
    return node;
}

// Root of the Trie
struct TrieNode *root = NULL;

// Initialize the Trie
void initialize_trie() {
    root = create_trie_node();
}

// Insert a path into the Trie
void insert_path(const char *path, int server_index) {
    struct TrieNode *current = root;
    for (int i = 0; path[i] != '\0'; i++) {
        if (current->children[(unsigned char)path[i]] == NULL) {
            current->children[(unsigned char)path[i]] = create_trie_node();
        }
        current = current->children[(unsigned char)path[i]];
    }
    current->is_end_of_path = 1;
    current->server_index = server_index;
}

// Search for a path in the Trie
int search_path(const char *path, int *server_index) {
    struct TrieNode *current = root;
    for (int i = 0; path[i] != '\0'; i++) {
        if (current->children[(unsigned char)path[i]] == NULL) {
            return 0; // Path not found
        }
        current = current->children[(unsigned char)path[i]];
    }
    if (current->is_end_of_path) {
        *server_index = current->server_index;
        return 1; // Path found
    }
    return 0; // Path not found
}

// Function to delete a path from the Trie
int delete_path(const char *path) {
    struct TrieNode *current = root;
    struct TrieNode *stack[strlen(path)];
    int stack_index = 0;

    // Traverse and push nodes onto stack
    for (int i = 0; path[i] != '\0'; i++) {
        if (current->children[(unsigned char)path[i]] == NULL) {
            return 0; // Path not found
        }
        stack[stack_index++] = current;
        current = current->children[(unsigned char)path[i]];
    }

    if (!current->is_end_of_path) {
        return 0; // Path not found
    }

    current->is_end_of_path = 0;

    // Remove unused nodes
    for (int i = strlen(path) - 1; i >= 0; i--) {
        int has_children = 0;
        for (int j = 0; j < 256; j++) {
            if (stack[i]->children[j] != NULL) {
                has_children = 1;
                break;
            }
        }
        if (!has_children && !stack[i]->is_end_of_path) {
            free(stack[i]->children[(unsigned char)path[i]]);
            stack[i]->children[(unsigned char)path[i]] = NULL;
        } else {
            break;
        }
    }
    return 1; // Path successfully deleted
}

// Modify find_storage_server_by_path to use the Trie
int find_storage_server_by_path(const char *path, char *ip, int *port) {
    int server_index;
    if (search_path(path, &server_index)) {
        strcpy(ip, storage_servers[server_index].ip);
        *port = storage_servers[server_index].client_port;
        return 0;
    }
    return -1;
}
// Function to find a storage server by IP and port
int find_storage_server_by_ip_port(const char *ip, int port) {
    for (int i = 0; i < storage_server_count; i++) {
        if (strcmp(storage_servers[i].ip, ip) == 0 && storage_servers[i].client_port == port) {
            return i;
        }
    }
    return -1;
}



// Add this function to find the storage server index by path
int find_storage_server_index_by_path(const char *path) {
    for (int i = 0; i < storage_server_count; i++) {
        for (int j = 0; j < storage_servers[i].file_count; j++) {
            if (strcmp(storage_servers[i].file_paths[j], path) == 0) {
                return i;
            }
        }
    }
    return -1;
}

// Add this function to find the storage server index by parent directory
int find_storage_server_index_by_parent(const char *path) {
    for (int i = 0; i < storage_server_count; i++) {
        for (int j = 0; j < storage_servers[i].file_count; j++) {
            if (strncmp(storage_servers[i].file_paths[j], path, strlen(path)) == 0) {
                return i;
            }
        }
    }
    return -1;
}
