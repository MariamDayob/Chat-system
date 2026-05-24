#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <signal.h>

#define MAX_PEERS 10
#define MAX_MESSAGE_LEN 512
#define BUFFER_SIZE 1024
#define DISCOVERY_PORT 9999
#define BASE_PORT 10000

// Message types
typedef enum {
    MSG_DISCOVERY,
    MSG_DISCOVERY_RESPONSE,
    MSG_JOIN_NETWORK,
    MSG_PEER_LIST,
    MSG_TEXT,
    MSG_FILE_REQUEST,
    MSG_DISCONNECT
} MessageType;

// Message structure
typedef struct {
    MessageType type;
    char sender_name[50];
    char sender_ip[16];
    int sender_port;
    char content[MAX_MESSAGE_LEN];
    time_t timestamp;
} Message;

// Peer structure
typedef struct {
    char name[50];
    char ip[16];
    int port;
    int socket;
    int connected;
    time_t last_seen;
} Peer;

// Global variables
char my_name[50];
char my_ip[16] = "127.0.0.1";
int my_port;
Peer peers[MAX_PEERS];
int peer_count = 0;
int server_socket;
pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;
int running = 1;

// Function prototypes
void initialize_node();
void* server_thread(void* arg);
void* client_handler(void* arg);
void* discovery_thread(void* arg);
void* input_thread(void* arg);
void discover_peers();
void connect_to_peer(char* ip, int port);
void add_peer(char* name, char* ip, int port, int socket);
void remove_peer(int socket);
void broadcast_message(char* message);
void send_message_to_peer(int peer_index, Message* msg);
void handle_message(Message* msg, int sender_socket);
void print_peers();
void print_menu();
void cleanup();
void signal_handler(int sig);

int main(int argc, char* argv[]) {
    if (argc != 2) {
        printf("Usage: %s <your_name>\n", argv[0]);
        exit(1);
    }

    strcpy(my_name, argv[1]);
    signal(SIGINT, signal_handler);

    initialize_node();

    pthread_t server_tid, discovery_tid, input_tid;

    // Start server thread
    if (pthread_create(&server_tid, NULL, server_thread, NULL) != 0) {
        perror("Failed to create server thread");
        exit(1);
    }

    // Start discovery thread
    if (pthread_create(&discovery_tid, NULL, discovery_thread, NULL) != 0) {
        perror("Failed to create discovery thread");
        exit(1);
    }

    // Start input thread
    if (pthread_create(&input_tid, NULL, input_thread, NULL) != 0) {
        perror("Failed to create input thread");
        exit(1);
    }

    printf("P2P Chat Node '%s' started on port %d\n", my_name, my_port);
    printf("Discovering peers...\n\n");

    // Discover existing peers
    discover_peers();

    // Keep main thread alive
    pthread_join(server_tid, NULL);
    pthread_join(discovery_tid, NULL);
    pthread_join(input_tid, NULL);

    cleanup();
    return 0;
}

void initialize_node() {
    srand(time(NULL));
    my_port = BASE_PORT + (rand() % 1000);
    
    // Initialize peers array
    for (int i = 0; i < MAX_PEERS; i++) {
        peers[i].connected = 0;
        peers[i].socket = -1;
    }
}

void* server_thread(void* arg) {
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    pthread_t client_tid;

    // Create server socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        perror("Server socket creation failed");
        pthread_exit(NULL);
    }

    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Setup server address
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(my_port);

    // Bind and listen
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        pthread_exit(NULL);
    }

    if (listen(server_socket, MAX_PEERS) < 0) {
        perror("Listen failed");
        pthread_exit(NULL);
    }

    while (running) {
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_len);
        if (client_socket < 0) {
            if (running) perror("Accept failed");
            continue;
        }

        // Create thread for each client
        int* socket_ptr = malloc(sizeof(int));
        *socket_ptr = client_socket;
        
        if (pthread_create(&client_tid, NULL, client_handler, socket_ptr) != 0) {
            perror("Client thread creation failed");
            close(client_socket);
            free(socket_ptr);
        } else {
            pthread_detach(client_tid);
        }
    }

    pthread_exit(NULL);
}

void* client_handler(void* arg) {
    int client_socket = *((int*)arg);
    free(arg);
    
    Message msg;
    char buffer[sizeof(Message)];

    while (running) {
        int bytes_received = recv(client_socket, buffer, sizeof(Message), 0);
        if (bytes_received <= 0) {
            break;
        }

        memcpy(&msg, buffer, sizeof(Message));
        handle_message(&msg, client_socket);
    }

    remove_peer(client_socket);
    close(client_socket);
    pthread_exit(NULL);
}

void* discovery_thread(void* arg) {
    int discovery_socket;
    struct sockaddr_in broadcast_addr, recv_addr;
    socklen_t recv_len = sizeof(recv_addr);
    Message msg, response;
    char buffer[sizeof(Message)];

    // Create UDP socket for discovery
    discovery_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (discovery_socket < 0) {
        perror("Discovery socket creation failed");
        pthread_exit(NULL);
    }

    int broadcast = 1;
    setsockopt(discovery_socket, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

    // Bind to discovery port
    struct sockaddr_in local_addr;
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_port = htons(DISCOVERY_PORT);

    if (bind(discovery_socket, (struct sockaddr*)&local_addr, sizeof(local_addr)) < 0) {
        perror("Discovery bind failed");
        close(discovery_socket);
        pthread_exit(NULL);
    }

    while (running) {
        int bytes_received = recvfrom(discovery_socket, buffer, sizeof(Message), 0,
                                    (struct sockaddr*)&recv_addr, &recv_len);
        
        if (bytes_received > 0) {
            memcpy(&msg, buffer, sizeof(Message));
            
            if (msg.type == MSG_DISCOVERY && strcmp(msg.sender_name, my_name) != 0) {
                // Send discovery response
                response.type = MSG_DISCOVERY_RESPONSE;
                strcpy(response.sender_name, my_name);
                strcpy(response.sender_ip, my_ip);
                response.sender_port = my_port;
                response.timestamp = time(NULL);

                sendto(discovery_socket, &response, sizeof(Message), 0,
                      (struct sockaddr*)&recv_addr, recv_len);
            }
            else if (msg.type == MSG_DISCOVERY_RESPONSE && strcmp(msg.sender_name, my_name) != 0) {
                // Try to connect to discovered peer
                connect_to_peer(msg.sender_ip, msg.sender_port);
            }
        }
    }

    close(discovery_socket);
    pthread_exit(NULL);
}

void* input_thread(void* arg) {
    char input[BUFFER_SIZE];
    char command[20], message[MAX_MESSAGE_LEN];

    while (running) {
        printf("\n> ");
        fflush(stdout);
        
        if (fgets(input, sizeof(input), stdin) == NULL) {
            break;
        }

        input[strcspn(input, "\n")] = 0; // Remove newline

        if (strlen(input) == 0) continue;

        if (sscanf(input, "%s", command) == 1) {
            if (strcmp(command, "/help") == 0) {
                print_menu();
            }
            else if (strcmp(command, "/peers") == 0) {
                print_peers();
            }
            else if (strcmp(command, "/discover") == 0) {
                discover_peers();
            }
            else if (strcmp(command, "/quit") == 0) {
                running = 0;
                break;
            }
            else if (sscanf(input, "/msg %[^\n]", message) == 1) {
                broadcast_message(message);
            }
            else {
                printf("Unknown command. Type /help for available commands.\n");
            }
        }
    }

    pthread_exit(NULL);
}

void discover_peers() {
    int discovery_socket;
    struct sockaddr_in broadcast_addr;
    Message msg;

    discovery_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (discovery_socket < 0) {
        perror("Discovery socket creation failed");
        return;
    }

    int broadcast = 1;
    setsockopt(discovery_socket, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

    // Setup broadcast address
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    broadcast_addr.sin_port = htons(DISCOVERY_PORT);

    // Create discovery message
    msg.type = MSG_DISCOVERY;
    strcpy(msg.sender_name, my_name);
    strcpy(msg.sender_ip, my_ip);
    msg.sender_port = my_port;
    msg.timestamp = time(NULL);

    // Send discovery broadcast
    if (sendto(discovery_socket, &msg, sizeof(Message), 0,
              (struct sockaddr*)&broadcast_addr, sizeof(broadcast_addr)) < 0) {
        perror("Discovery broadcast failed");
    } else {
        printf("Discovery broadcast sent\n");
    }

    close(discovery_socket);
}

void connect_to_peer(char* ip, int port) {
    pthread_mutex_lock(&peers_mutex);

    // Check if already connected
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].connected && strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            pthread_mutex_unlock(&peers_mutex);
            return;
        }
    }

    pthread_mutex_unlock(&peers_mutex);

    int peer_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_socket < 0) {
        perror("Peer socket creation failed");
        return;
    }

    struct sockaddr_in peer_addr;
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_addr.s_addr = inet_addr(ip);
    peer_addr.sin_port = htons(port);

    if (connect(peer_socket, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) == 0) {
        // Send join message
        Message join_msg;
        join_msg.type = MSG_JOIN_NETWORK;
        strcpy(join_msg.sender_name, my_name);
        strcpy(join_msg.sender_ip, my_ip);
        join_msg.sender_port = my_port;
        join_msg.timestamp = time(NULL);

        send(peer_socket, &join_msg, sizeof(Message), 0);
        
        add_peer("Unknown", ip, port, peer_socket);
        printf("Connected to peer at %s:%d\n", ip, port);
        
        // Start client handler for this peer
        pthread_t client_tid;
        int* socket_ptr = malloc(sizeof(int));
        *socket_ptr = peer_socket;
        
        if (pthread_create(&client_tid, NULL, client_handler, socket_ptr) == 0) {
            pthread_detach(client_tid);
        } else {
            free(socket_ptr);
            close(peer_socket);
        }
    } else {
        close(peer_socket);
    }
}

void add_peer(char* name, char* ip, int port, int socket) {
    pthread_mutex_lock(&peers_mutex);

    if (peer_count < MAX_PEERS) {
        strcpy(peers[peer_count].name, name);
        strcpy(peers[peer_count].ip, ip);
        peers[peer_count].port = port;
        peers[peer_count].socket = socket;
        peers[peer_count].connected = 1;
        peers[peer_count].last_seen = time(NULL);
        peer_count++;
    }

    pthread_mutex_unlock(&peers_mutex);
}

void remove_peer(int socket) {
    pthread_mutex_lock(&peers_mutex);

    for (int i = 0; i < peer_count; i++) {
        if (peers[i].socket == socket) {
            printf("Peer %s disconnected\n", peers[i].name);
            peers[i].connected = 0;
            
            // Shift remaining peers
            for (int j = i; j < peer_count - 1; j++) {
                peers[j] = peers[j + 1];
            }
            peer_count--;
            break;
        }
    }

    pthread_mutex_unlock(&peers_mutex);
}

void broadcast_message(char* message) {
    Message msg;
    msg.type = MSG_TEXT;
    strcpy(msg.sender_name, my_name);
    strcpy(msg.sender_ip, my_ip);
    msg.sender_port = my_port;
    strcpy(msg.content, message);
    msg.timestamp = time(NULL);

    pthread_mutex_lock(&peers_mutex);

    for (int i = 0; i < peer_count; i++) {
        if (peers[i].connected) {
            send_message_to_peer(i, &msg);
        }
    }

    pthread_mutex_unlock(&peers_mutex);

    // Display sent message
    printf("[%s] %s: %s\n", my_name, my_name, message);
}

void send_message_to_peer(int peer_index, Message* msg) {
    if (send(peers[peer_index].socket, msg, sizeof(Message), 0) < 0) {
        printf("Failed to send message to %s\n", peers[peer_index].name);
        peers[peer_index].connected = 0;
    }
}

void handle_message(Message* msg, int sender_socket) {
    time_t now = time(NULL);
    struct tm* timeinfo = localtime(&msg->timestamp);
    char time_str[20];
    strftime(time_str, sizeof(time_str), "%H:%M:%S", timeinfo);

    switch (msg->type) {
        case MSG_JOIN_NETWORK:
            printf("Peer %s joined the network\n", msg->sender_name);
            
            // Update peer info
            pthread_mutex_lock(&peers_mutex);
            for (int i = 0; i < peer_count; i++) {
                if (peers[i].socket == sender_socket) {
                    strcpy(peers[i].name, msg->sender_name);
                    peers[i].last_seen = now;
                    break;
                }
            }
            pthread_mutex_unlock(&peers_mutex);
            break;

        case MSG_TEXT:
            printf("[%s] %s: %s\n", time_str, msg->sender_name, msg->content);
            break;

        case MSG_DISCONNECT:
            printf("Peer %s is disconnecting\n", msg->sender_name);
            remove_peer(sender_socket);
            break;

        default:
            break;
    }
}

void print_peers() {
    pthread_mutex_lock(&peers_mutex);

    printf("\n=== Connected Peers ===\n");
    if (peer_count == 0) {
        printf("No peers connected\n");
    } else {
        for (int i = 0; i < peer_count; i++) {
            if (peers[i].connected) {
                printf("%d. %s (%s:%d)\n", i + 1, peers[i].name, 
                       peers[i].ip, peers[i].port);
            }
        }
    }
    printf("====================\n");

    pthread_mutex_unlock(&peers_mutex);
}

void print_menu() {
    printf("\n=== P2P Chat Commands ===\n");
    printf("/help      - Show this menu\n");
    printf("/peers     - List connected peers\n");
    printf("/msg <text> - Send message to all peers\n");
    printf("/discover  - Discover new peers\n");
    printf("/quit      - Exit the chat\n");
    printf("========================\n");
}

void cleanup() {
    running = 0;

    // Send disconnect message to all peers
    Message disconnect_msg;
    disconnect_msg.type = MSG_DISCONNECT;
    strcpy(disconnect_msg.sender_name, my_name);
    disconnect_msg.timestamp = time(NULL);

    pthread_mutex_lock(&peers_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].connected) {
            send(peers[i].socket, &disconnect_msg, sizeof(Message), 0);
            close(peers[i].socket);
        }
    }
    pthread_mutex_unlock(&peers_mutex);

    if (server_socket >= 0) {
        close(server_socket);
    }

    printf("\nP2P Chat Node '%s' shutdown complete\n", my_name);
}

void signal_handler(int sig) {
    if (sig == SIGINT) {
        printf("\nReceived SIGINT, shutting down...\n");
        running = 0;
        cleanup();
        exit(0);
    }
}