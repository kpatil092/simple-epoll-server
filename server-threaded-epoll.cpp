
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>

#define SERVER_PORT 2525
#define MAX_EVENTS 128
#define MAX_CONNECTIONS 8192 // Bumped up for safety
#define THREAD_COUNT 8
#define BUFFER_SIZE 4096
#define IDLE_TIMEOUT_SECONDS 30

typedef struct {
    int fd;
    int active;
    int epoll_fd;      // Tracks which thread's epoll owns this connection
    time_t lastActive;
    char recvBuffer[BUFFER_SIZE];
    int recvLen;
    char sendBuffer[BUFFER_SIZE];
    int sendLen;
    int sendOffset;
    // NOTICE: Mutex and processing flags are completely gone!
} Connection;

// Context passed to each thread
typedef struct {
    int thread_id;
} ThreadContext;

static Connection gConnections[MAX_CONNECTIONS];

// Helper functions
void setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if(flags == -1) {
        perror("fcntl");
        exit(EXIT_FAILURE);
    }
    if(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl");
        exit(EXIT_FAILURE);
    }
}

void closeConnection(Connection* conn) {
    if (conn->fd != -1) {
        epoll_ctl(conn->epoll_fd, EPOLL_CTL_DEL, conn->fd, NULL);
        close(conn->fd);
        // printf("Client disconnected (fd=%d)\n", conn->fd); // Commented for throughput
        conn->fd = -1;
    }
    conn->active = 0;
    conn->recvLen = 0;
    conn->sendLen = 0;
    conn->sendOffset = 0;
}

void setEpollEvents(Connection* conn) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    
    // Dynamically adjust what we listen for based on buffer state
    // NOTICE: EPOLLONESHOT is gone! Only this thread will ever touch this socket.
    if(conn->sendLen > conn->sendOffset) {
        ev.events = EPOLLIN | EPOLLOUT; 
    } else {
        ev.events = EPOLLIN;
    }

    ev.data.fd = conn->fd;
    epoll_ctl(conn->epoll_fd, EPOLL_CTL_MOD, conn->fd, &ev);
}

int flushSendBuffer(Connection *conn) {
    while(conn->sendOffset < conn->sendLen) {
        ssize_t n = send(conn->fd, conn->sendBuffer + conn->sendOffset, conn->sendLen - conn->sendOffset, MSG_NOSIGNAL);
        if(n > 0) {
            conn->sendOffset += n;
            continue;
        }
        if(n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 0;        
        return -1;          
    }
    conn->sendLen = 0;
    conn->sendOffset = 0;
    return 1;
}

// Network processing
void processConnection(Connection* conn) {
    int canRead = 1;

    // 1. Finish leftover sends
    if (conn->sendLen > conn->sendOffset) {
        int rc = flushSendBuffer(conn);
        if (rc == -1) {
            closeConnection(conn);
            return;
        }
        if (rc == 0) canRead = 0; 
    }

    // 2. Read new data
    if (canRead) {
        while(1) {
            int bytes = recv(conn->fd, conn->recvBuffer + conn->recvLen, BUFFER_SIZE - conn->recvLen, 0);

            if(bytes == 0) {
                closeConnection(conn);
                return;
            }
            if(bytes < 0) {
                if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                closeConnection(conn);
                return;
            }

            conn->recvLen += bytes;
            conn->lastActive = time(NULL);
            
            // Echo it back
            memcpy(conn->sendBuffer, conn->recvBuffer, conn->recvLen);
            conn->sendLen = conn->recvLen;
            conn->sendOffset = 0;
            conn->recvLen = 0; 

            int rc = flushSendBuffer(conn);
            if(rc == -1) {
                closeConnection(conn);
                return;
            }
            if(rc == 0) break;
        }
    }
    
    // 3. Update epoll state
    setEpollEvents(conn);
}

void acceptClients(int serverfd, int epoll_fd) {
    while(1) {
        struct sockaddr_in clientAddr;
        socklen_t len = sizeof(clientAddr);

        int clientfd = accept(serverfd, (struct sockaddr*)&clientAddr, &len);
        if(clientfd < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) break; 
            break;
        }

        if(clientfd >= MAX_CONNECTIONS) { 
            close(clientfd);
            continue;
        }
        
        setNonBlocking(clientfd);
        Connection *conn = &gConnections[clientfd];
        
        conn->fd = clientfd;
        conn->active = 1;
        conn->epoll_fd = epoll_fd; // Tag this connection to this thread's epoll
        conn->lastActive = time(NULL);
        conn->recvLen = 0;       
        conn->sendLen = 0;       
        conn->sendOffset = 0;    

        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.fd = clientfd;
        
        if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, clientfd, &ev) < 0) {
            close(clientfd);
            conn->active = 0; 
            conn->fd = -1;
            continue;
        }
    }
}

// ==========================================
// THE MULTI-REACTOR CORE
// ==========================================
void* workerThread(void* args) {
    ThreadContext* ctx = (ThreadContext*)args;
    
    // 1. Every thread creates its OWN server socket
    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // THE MAGIC TRICK: Allow multiple threads to bind to the same port
    setsockopt(serverfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(serverfd, (struct sockaddr*)&addr, sizeof(addr));
    listen(serverfd, 4096);
    setNonBlocking(serverfd);

    // 2. Every thread creates its OWN epoll instance
    int epoll_fd = epoll_create1(0);
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.fd = serverfd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, serverfd, &ev);

    struct epoll_event events[MAX_EVENTS];
    time_t last_sweep = time(NULL);

    printf("Worker %d listening on port %d...\n", ctx->thread_id, SERVER_PORT);

    // 3. The Thread's Private Event Loop
    while(1) {
        // Wait for up to 1000ms (1 second). No infinite blocking!
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);

        time_t now = time(NULL);
        
        // Timeout Sweeper built directly into the loop (No mutexes needed!)
        if (now - last_sweep >= 5) {
            for (int i = 0; i < MAX_CONNECTIONS; i++) {
                Connection* conn = &gConnections[i];
                // Only sweep connections that belong to THIS thread
                if (conn->active && conn->epoll_fd == epoll_fd) {
                    if (now - conn->lastActive > IDLE_TIMEOUT_SECONDS) {
                        closeConnection(conn);
                    }
                }
            }
            last_sweep = now;
        }

        // Process network events
        for(int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            
            if(fd == serverfd) {
                acceptClients(serverfd, epoll_fd);
            } else {
                Connection *conn = &gConnections[fd];
                if(!conn->active) continue;

                if(events[i].events & (EPOLLERR | EPOLLHUP)) {
                    closeConnection(conn);
                    continue;
                }
                processConnection(conn);
            }
        }
    }
    
    free(ctx);
    return NULL;
}

int main() {
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        gConnections[i].fd = -1;
        gConnections[i].active = 0;
    }

    printf("-------------------------------------\n");
    printf("Starting Share-Nothing Server...\n");
    printf("Port : %d | Threads: %d\n", SERVER_PORT, THREAD_COUNT);
    printf("-------------------------------------\n");

    pthread_t threads[THREAD_COUNT];

    for (int i = 0; i < THREAD_COUNT; i++) {
        ThreadContext* ctx = (ThreadContext*)malloc(sizeof(ThreadContext));
        ctx->thread_id = i;
        
        if(pthread_create(&threads[i], NULL, workerThread, ctx) != 0) {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        }
    }

    // Main thread just waits for workers to finish (which is never)
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}