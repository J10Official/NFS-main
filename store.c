#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

// Function to get system IP
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

struct StorageServerInfo {
    char type[8];     // Type of server (e.g., "storage")
    char ip[16];
    int nm_port;      // Dedicated port for NM connection
    int client_port;  // Port for clients to connect
    char file_paths[100][256]; // Array to store file paths
    int file_count;   // Number of file paths
};

// Thread function to handle instructions from the naming server
void *handle_naming_server(void *arg) {
    int sock = *(int *)arg;
    char instruction[256];

    while (1) {
        int bytes_received = recv(sock, instruction, sizeof(instruction) - 1, 0);
        if (bytes_received > 0) {
            instruction[bytes_received] = '\0'; // Null-terminate the received string
            printf("Received instruction from naming server: %s\n", instruction);

            if (strncmp(instruction, "CREATE ", 7) == 0) {
                // Handle 'CREATE <path> <name>' command
                char path[256], name[256];
                sscanf(instruction + 7, "%s %s", path, name);
                char full_path[512];
                snprintf(full_path, sizeof(full_path), "%s/%s", path, name);

                // Create file
                FILE *fp = fopen(full_path, "w");
                if (fp) {
                    fclose(fp);
                    printf("Created file: %s\n", full_path);
                    send(sock, "Success", 7, 0);
                } else {
                    perror("Failed to create file");
                    send(sock, "Failed", 6, 0);
                }
            } else if (strncmp(instruction, "CREATE_DIR ", 11) == 0) {
                // Handle 'CREATE_DIR <path> <name>' command
                char path[256], name[256];
                sscanf(instruction + 11, "%s %s", path, name);
                char full_path[512];
                snprintf(full_path, sizeof(full_path), "%s/%s", path, name);

                // Create directory
                if (mkdir(full_path, 0777) == 0) {
                    printf("Created directory: %s\n", full_path);
                    send(sock, "Success", 7, 0);
                } else {
                    perror("Failed to create directory");
                    send(sock, "Failed", 6, 0);
                }
            } else if (strncmp(instruction, "delete ", 7) == 0) {
                // Handle 'delete <path>' command
                char path[256];
                sscanf(instruction + 7, "%s", path);
                if (remove(path) == 0) {
                    printf("Deleted: %s\n", path);
                    send(sock, "Success", 7, 0);
                } else {
                    perror("Failed to delete");
                    send(sock, "Failed", 6, 0);
                }
            } else {
                printf("Unknown or malformed instruction: %s\n", instruction);
            }
        } else if (bytes_received == 0) {
            printf("Connection closed by naming server\n");
            break;
        } else {
            perror("recv");
            break;
        }
    }

    close(sock); // Close the naming server socket
    return NULL;
}

// Thread function to accept client connections
void *handle_clients(void *arg) {
    int client_sock = *(int *)arg;
    int new_socket;
    struct sockaddr_in client_addr;
    socklen_t addrlen = sizeof(client_addr);

    printf("Storage server is ready to accept client connections...\n");

    while (1) {
        new_socket = accept(client_sock, (struct sockaddr *)&client_addr, &addrlen);
        if (new_socket < 0) {
            perror("accept");
            continue;
        }

        printf("Client connected\n");

        // Handle client requests
        char buffer[256];
        int bytes_received = recv(new_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received > 0) {
            buffer[bytes_received] = '\0'; // Null-terminate the received string
            printf("Received from client: %s\n", buffer);

            if (strncmp(buffer, "read ", 5) == 0) {
                // Handle 'read <path>' command
                char path[256];
                sscanf(buffer + 5, "%s", path);
                FILE *fp = fopen(path, "r");
                if (fp) {
                    char file_content[1024];
                    while (fgets(file_content, sizeof(file_content), fp) != NULL) {
                        send(new_socket, file_content, strlen(file_content), 0);
                    }
                    fclose(fp);
                } else {
                    char response[] = "Failed to read file";
                    send(new_socket, response, strlen(response), 0);
                }
            } else if (strncmp(buffer, "write ", 6) == 0) {
                // Handle 'write <path>' command
                char path[256];
                sscanf(buffer + 6, "%s", path);
                FILE *fp = fopen(path, "w");
                if (fp) {
                    char file_content[1024];
                    while ((bytes_received = recv(new_socket, file_content, sizeof(file_content) - 1, 0)) > 0) {
                        file_content[bytes_received] = '\0'; // Null-terminate the received string
                        if (strcmp(file_content, "STOP") == 0) {
                            break;
                        }
                        fputs(file_content, fp);
                    }
                    fclose(fp);
                    send(new_socket, "File written successfully", 25, 0);
                } else {
                    char response[] = "Failed to write file";
                    send(new_socket, response, strlen(response), 0);
                }
            } else if (strncmp(buffer, "info ", 5) == 0) {
                // Handle 'info <path>' command
                char path[256];
                sscanf(buffer + 5, "%s", path);
                struct stat file_stat;
                if (stat(path, &file_stat) == 0) {
                    char response[256];
                    snprintf(response, sizeof(response), "File size: %ld bytes\nLast modified: %s", file_stat.st_size, ctime(&file_stat.st_mtime));
                    send(new_socket, response, strlen(response), 0);
                } else {
                    char response[] = "Failed to retrieve file info";
                    send(new_socket, response, strlen(response), 0);
                }
            } else if (strncmp(buffer, "stream ", 7) == 0) {
                // Handle 'stream <path>' command
                char path[256];
                sscanf(buffer + 7, "%s", path);
                FILE *fp = fopen(path, "rb");
                if (fp) {
                    char file_content[1024];
                    size_t bytes_read;
                    while ((bytes_read = fread(file_content, 1, sizeof(file_content), fp)) > 0) {
                        send(new_socket, file_content, bytes_read, 0);
                    }
                    fclose(fp);
                    // Send a termination message
                    send(new_socket, "STOP", 4, 0);
                } else {
                    char response[] = "Failed to stream file";
                    send(new_socket, response, strlen(response), 0);
                }
            }
        }

        close(new_socket);
    }

    close(client_sock); // Close the client socket
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <Naming Server IP>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *naming_server_ip = argv[1];
    int sock = 0, nm_sock = 0, client_sock = 0;
    struct sockaddr_in serv_addr, nm_addr, client_addr;
    struct StorageServerInfo info;
    socklen_t addrlen = sizeof(nm_addr);

    // Automatically use port 5050 for connection
    int port = 5050;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return 1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, naming_server_ip, &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return 1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed \n");
        return 1;
    }

    printf("Connected to naming server at %s on port %d\n", naming_server_ip, port);

    // Send the connection type to the naming server
    char *type_message = "storage";
    send(sock, type_message, strlen(type_message), 0);

    // Create and bind a dedicated port for NM connection
    if ((nm_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n NM socket creation error \n");
        return 1;
    }

    nm_addr.sin_family = AF_INET;
    nm_addr.sin_addr.s_addr = INADDR_ANY;
    nm_addr.sin_port = 0; // Let the OS choose a random available port

    if (bind(nm_sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
        printf("\n NM bind failed \n");
        return 1;
    }

    // Get the assigned port number
    if (getsockname(nm_sock, (struct sockaddr *)&nm_addr, &addrlen) == -1) {
        perror("getsockname");
        return 1;
    }

    int nm_port = ntohs(nm_addr.sin_port);
    printf("Dedicated NM connection port %d created and bound\n", nm_port);

    // Create and bind a port for clients to connect
    if ((client_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Client socket creation error \n");
        return 1;
    }

    // Initialize client_addr structure
    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_addr.s_addr = INADDR_ANY;
    client_addr.sin_port = 0; // Let the OS choose a random available port

    if (bind(client_sock, (struct sockaddr *)&client_addr, sizeof(client_addr)) < 0) {
        printf("\n Client bind failed \n");
        return 1;
    }

    // Start listening on the client socket
    if (listen(client_sock, 5) < 0) {
        printf("\n Client listen failed \n");
        return 1;
    }

    // Get the assigned port number for client connections
    struct sockaddr_in assigned_addr;
    socklen_t assigned_addrlen = sizeof(assigned_addr);
    if (getsockname(client_sock, (struct sockaddr *)&assigned_addr, &assigned_addrlen) == -1) {
        perror("getsockname");
        return 1;
    }

    int client_port = ntohs(assigned_addr.sin_port);
    printf("Client connection port %d created and bound\n", client_port);

    // Get the storage server's IP address
    char storage_ip[16];
    getsystemip(storage_ip);

    // Fill the storage server info
    strncpy(info.type, "storage", sizeof(info.type));
    strncpy(info.ip, storage_ip, sizeof(info.ip));
    info.nm_port = nm_port;
    info.client_port = client_port;

    // Prompt the user to enter the list of file paths and folder paths
    printf("Enter the list of file paths and folder paths (enter 'done' to finish):\n");
    char path[256];
    info.file_count = 0;

    while (1) {
        printf("> ");
        fgets(path, sizeof(path), stdin);
        path[strcspn(path, "\n")] = 0; // Remove newline character
        if (strcmp(path, "done") == 0) {
            break;
        }
        strncpy(info.file_paths[info.file_count++], path, sizeof(info.file_paths[0]));
    }

    // Send the storage server info to the naming server
    send(sock, &info, sizeof(info), 0);
    printf("Storage server info sent to naming server\n");

    pthread_t naming_thread, client_thread;

    // Create thread to handle naming server instructions
    if (pthread_create(&naming_thread, NULL, handle_naming_server, &sock) != 0) {
        perror("pthread_create for naming_thread");
        exit(EXIT_FAILURE);
    }

    // Create thread to handle client connections
    if (pthread_create(&client_thread, NULL, handle_clients, &client_sock) != 0) {
        perror("pthread_create for client_thread");
        exit(EXIT_FAILURE);
    }

    // Wait for both threads to finish (they won't in this case)
    pthread_join(naming_thread, NULL);
    pthread_join(client_thread, NULL);

    // Close sockets before exiting
    close(nm_sock);
    return 0;
}
