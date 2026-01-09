#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/wait.h> // Include this header for wait function

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <Naming Server IP>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *naming_server_ip = argv[1];
    int port = 5050; // Hardcoded port number
    int sock = 0;
    struct sockaddr_in serv_addr;

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
    char *type_message = "client";
    send(sock, type_message, strlen(type_message), 0);

    // Send requests to the naming server and receive replies
    char request[1036]; // Increased buffer size
    char reply[256];
    while (1) {
        printf("Enter request (or 'quit' to exit): ");
        fgets(request, sizeof(request), stdin);
        request[strcspn(request, "\n")] = 0; // Remove newline character

        if (strcmp(request, "quit") == 0) {
            break;
        }

        // If the user wants to create a file or directory, prompt accordingly
        if (strcmp(request, "create") == 0) {
            // Create file
            char path[512], name[512]; // Increased buffer size
            printf("Enter the path: ");
            fgets(path, sizeof(path), stdin);
            path[strcspn(path, "\n")] = 0;
            printf("Enter the file name: ");
            fgets(name, sizeof(name), stdin);
            name[strcspn(name, "\n")] = 0;
            snprintf(request, sizeof(request), "CREATE %s %s", path, name);
        } else if (strcmp(request, "create_dir") == 0) {
            // Create directory
            char path[512], name[512]; // Increased buffer size
            printf("Enter the path: ");
            fgets(path, sizeof(path), stdin);
            path[strcspn(path, "\n")] = 0;
            printf("Enter the directory name: ");
            fgets(name, sizeof(name), stdin);
            name[strcspn(name, "\n")] = 0;
            snprintf(request, sizeof(request), "CREATE_DIR %s %s", path, name);
        }

        send(sock, request, strlen(request), 0);

        // Receive reply from the server
        int bytes_received = recv(sock, reply, sizeof(reply) - 1, 0);
        if (bytes_received > 0) {
            reply[bytes_received] = '\0'; // Null-terminate the received string
            if (strcmp(request, "paths") == 0) {
                printf("List of paths received:\n%s\n", reply);
            } else if (strncmp(request, "CREATE ", 7) == 0 || strncmp(request, "CREATE_DIR ", 11) == 0 || strncmp(request, "delete ", 7) == 0) {
                // Handle 'CREATE <path> <name>', 'CREATE_DIR <path> <name>', and 'delete <path>' commands
                printf("%s\n", reply);
            } else if (strncmp(request, "read ", 5) == 0 || strncmp(request, "write ", 6) == 0 || strncmp(request, "info ", 5) == 0 || strncmp(request, "stream ", 7) == 0) {
                // Handle 'read <path>', 'write <path>', 'info <path>', 'stream <path>' commands
                char ip[16];
                int port;
                sscanf(reply, "Path %*s is stored on server %15[^:]:%d", ip, &port);

                // Remove newline character from the IP address
                ip[strcspn(ip, "\n")] = 0;

                // Connect to the storage server
                int storage_sock;
                struct sockaddr_in storage_addr;

                if ((storage_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                    perror("Socket creation error");
                    continue;
                }

                storage_addr.sin_family = AF_INET;
                storage_addr.sin_port = htons(port);

                if (inet_pton(AF_INET, ip, &storage_addr.sin_addr) <= 0) {
                    printf("Invalid address/ Address not supported\n");
                    close(storage_sock);
                    continue;
                }

                if (connect(storage_sock, (struct sockaddr *)&storage_addr, sizeof(storage_addr)) < 0) {
                    perror("Connection Failed");
                    close(storage_sock);
                    continue;
                }

                // Send the request to the storage server
                send(storage_sock, request, strlen(request), 0);

                if (strncmp(request, "write ", 6) == 0) {
                    // Send the file content to the storage server
                    printf("Enter file content (end with 'STOP'):\n");
                    while (1) {
                        fgets(request, sizeof(request), stdin);
                        request[strcspn(request, "\n")] = 0; // Remove newline character
                        send(storage_sock, request, strlen(request), 0);
                        if (strcmp(request, "STOP") == 0) {
                            break;
                        }
                    }
                }

                // Receive the response from the storage server
                while ((bytes_received = recv(storage_sock, reply, sizeof(reply) - 1, 0)) > 0) {
                    reply[bytes_received] = '\0'; // Null-terminate the received string
                    if (strcmp(reply, "STOP") == 0) {
                        break;
                    }
                    printf("%s\n", reply);
                }

                if (bytes_received < 0) {
                    perror("recv");
                }

                close(storage_sock);
            } else if (strncmp(request, "stream ", 7) == 0) {
                char ip[16];
                int port;
                sscanf(reply, "Path %*s is stored on server %15[^:]:%d", ip, &port);

                // Remove newline character from the IP address
                ip[strcspn(ip, "\n")] = 0;

                // Connect to the storage server
                int storage_sock;
                struct sockaddr_in storage_addr;

                if ((storage_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                    perror("Socket creation error");
                    continue;
                }

                storage_addr.sin_family = AF_INET;
                storage_addr.sin_port = htons(port);

                if (inet_pton(AF_INET, ip, &storage_addr.sin_addr) <= 0) {
                    printf("Invalid address/ Address not supported\n");
                    close(storage_sock);
                    continue;
                }

                if (connect(storage_sock, (struct sockaddr *)&storage_addr, sizeof(storage_addr)) < 0) {
                    perror("Connection Failed");
                    close(storage_sock);
                    continue;
                }

                // Send the request to the storage server
                send(storage_sock, request, strlen(request), 0);

                // Open a pipe to MPV
                FILE *mpv_pipe = popen("mpv --no-cache --really-quiet -", "w");
                if (!mpv_pipe) {
                    perror("Failed to open pipe to MPV");
                    close(storage_sock);
                    continue;
                }

                // Stream data from the storage server to MPV
                while ((bytes_received = recv(storage_sock, reply, sizeof(reply), 0)) > 0) {
                    if (fwrite(reply, 1, bytes_received, mpv_pipe) != bytes_received) {
                        perror("Error writing to MPV");
                        break;
                    }
                }

                if (bytes_received < 0) {
                    perror("Error receiving stream");
                }

                // Cleanup
                pclose(mpv_pipe);
                close(storage_sock);
            } else {
                printf("Received from server: %s\n", reply);
            }
        } else if (bytes_received == 0) {
            printf("Server disconnected\n");
            break;
        } else {
            perror("recv");
            break;
        }

        // Clear the request buffer for the next input
        memset(request, 0, sizeof(request));
    }

    // Close the socket
    close(sock);
    return 0;
}
