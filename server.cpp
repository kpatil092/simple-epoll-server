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
#define MAX_CONNECTIONS 4096
#define THREAD_COUNT 8
#define BUFFER_SIZE 4096
#define TASK_QUEUE_SIZE 4096
#define IDLE_TIMEOUT_SECONDS 30

typedef struct {
    int fd;
    int active;
    int processing;
    time_t lastActive;
    char recvBuffer[BUFFER_SIZE];
    int recvLen;
    char sendBuffer[BUFFER_SIZE];
    int sendLen;
    int sendOffset;
    pthread_mutex_t mutex;
} Connection;

typedef struct {
    Connection *conn;
} Task;

typedef struct {
    Task tasks[TASK_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} TaskQueue;

static Connection gConnections[MAX_CONNECTIONS];
static TaskQueue gQueue;
static int gEpollFD;

// Queue operations
void queueInit(TaskQueue *q) {
    q->head = 0;
    q->tail = 0;
    q->count = 0;

    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
}

void queuePush(TaskQueue *q, Task task) {
    pthread_mutex_lock(&q->mutex);
    while(q->count == TASK_QUEUE_SIZE) pthread_cond_wait(&q->cond, &q->mutex);

    q->tasks[q->tail] = task;
    q->tail++;

    if(q->tail == TASK_QUEUE_SIZE) q->tail = 0;
    q->count++;

    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

Task queuePop(TaskQueue *q) {
    pthread_mutex_lock(&q->mutex);
    while (q->count == 0) pthread_cond_wait(&q->cond, &q->mutex);

    Task task = q->tasks[q->head];
    q->head++;
    if(q->head == TASK_QUEUE_SIZE) q->head = 0;
    q->count--;

    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);

    return task;
}

// Helper functions
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

// Processing connection
void closeConnection(Connection* conn) {
    pthread_mutex_lock(&conn->mutex);
    
    if (conn->fd != -1) {
        epoll_ctl(gEpollFD, EPOLL_CTL_DEL, conn->fd, NULL);
        close(conn->fd);
        // printf("Client disconnected (fd=%d)\n", conn->fd);
        conn->fd = -1;
    }

    conn->active = 0;
    conn->processing = 0;
    conn->recvLen = 0;
    conn->sendLen = 0;
    conn->sendOffset = 0; // BUG FIXED: Properly reset offset

    pthread_mutex_unlock(&conn->mutex);
}

void rearmConnection(Connection* conn) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLONESHOT;
    
    if(conn->sendLen > conn->sendOffset) {
        ev.events |= EPOLLOUT;
    } else {
        ev.events |= EPOLLIN;
    }

    ev.data.fd = conn->fd;
    if(epoll_ctl(gEpollFD, EPOLL_CTL_MOD, conn->fd, &ev) == -1) {
        perror("epoll_ctl MOD");
    }
}

void processConnection(Connection* conn) {
    int canRead = 1;

    if (conn->sendLen > conn->sendOffset) {
        int rc = flushSendBuffer(conn);
        if (rc == -1) {
            closeConnection(conn);
            return;
        }
        if (rc == 0) {
            canRead = 0; 
        } else {
            conn->sendLen = 0;
            conn->sendOffset = 0;
        }
    }

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
            // printf("Socket %d received %d bytes\n", conn->fd, bytes);
            memcpy(conn->sendBuffer, conn->recvBuffer, conn->recvLen);
            
            conn->sendLen = conn->recvLen;
            conn->sendOffset = 0;
            conn->recvLen = 0; 

            // Try sending the data back
            int rc = flushSendBuffer(conn);

            if(rc == -1) {
                closeConnection(conn);
                return;
            }
            if(rc == 0) break;
            conn->sendLen = 0;
            conn->sendOffset = 0;
        }
    }
    
    pthread_mutex_lock(&conn->mutex);
    conn->processing = 0;
    pthread_mutex_unlock(&conn->mutex);
    
    rearmConnection(conn);
}

// Threads operations
void* workerThread(void* args) {
    (void)args;
    while(1) {
        Task task = queuePop(&gQueue);
        processConnection(task.conn);
    }
    return NULL;
}

void createThreadPool() {
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_t tid;
        if(pthread_create(&tid, NULL, workerThread, NULL) != 0) {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        } 
        pthread_detach(tid);
    }
    printf("Thread Pool Created (%d workers)\n", THREAD_COUNT);
}

// socket operations
int createServerSocket(void) {
    int opt = 1;
    struct sockaddr_in addr;
    
    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if(serverfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if(bind(serverfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    if(listen(serverfd, 4096) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    setNonBlocking(serverfd);

    gEpollFD = epoll_create1(0);
    if(gEpollFD < 0) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));

    ev.events = EPOLLIN;
    ev.data.fd = serverfd;

    if(epoll_ctl(gEpollFD, EPOLL_CTL_ADD, serverfd, &ev) < 0) {
        perror("epoll_ctl");
        exit(EXIT_FAILURE);
    }

    return serverfd;
}

void acceptClients(int serverfd) {
    while(1) {
        struct sockaddr_in clientAddr;
        socklen_t len = sizeof(clientAddr);

        int clientfd = accept(serverfd, (struct sockaddr*)&clientAddr, &len);
        if(clientfd < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) break; 
            perror("accept");
            break;
        }

        if(clientfd >= MAX_CONNECTIONS) { 
            close(clientfd);
            continue;
        }
        setNonBlocking(clientfd);

        Connection *conn = &gConnections[clientfd];

        // BUG FIXED: Lock to safely initialize reused connections 
        pthread_mutex_lock(&conn->mutex);
        
        conn->fd = clientfd;
        conn->active = 1;
        conn->processing = 0;
        conn->lastActive = time(NULL);
        conn->recvLen = 0;       // Explicit state reset
        conn->sendLen = 0;       // Explicit state reset
        conn->sendOffset = 0;    // Explicit state reset
        memset(conn->recvBuffer, 0, sizeof(conn->recvBuffer));
        memset(conn->sendBuffer, 0, sizeof(conn->sendBuffer));

        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));

        ev.events = EPOLLIN | EPOLLONESHOT;
        ev.data.fd = clientfd;
        
        if(epoll_ctl(gEpollFD, EPOLL_CTL_ADD, clientfd, &ev) < 0) {
            perror("epoll_ctl");
            close(clientfd);
            conn->active = 0; // BUG FIXED: Don't leave ghost connections
            conn->fd = -1;
            pthread_mutex_unlock(&conn->mutex);
            continue;
        }

        pthread_mutex_unlock(&conn->mutex);
        // printf("Client Connected : fd=%d\n", clientfd);
    }
}

// event loop operations
void eventLoop(int serverfd) {
    struct epoll_event events[MAX_EVENTS];

    while(1) {
        int n = epoll_wait(gEpollFD, events, MAX_EVENTS, -1);

        if(n < 0) {
            if(errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for(int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if(fd == serverfd) {
                acceptClients(serverfd);
                continue;
            }
 
            Connection *conn = &gConnections[fd];

            // Fast lockless check
            if(!conn->active) continue;

            pthread_mutex_lock(&conn->mutex);

            // THE FIX: Double check inside the lock!
            if (!conn->active) {
                pthread_mutex_unlock(&conn->mutex);
                continue;
            }

            // Check ERR/HUP to prevent racing the worker thread
            if(events[i].events & (EPOLLERR | EPOLLHUP)) {
                if (conn->processing) {
                    pthread_mutex_unlock(&conn->mutex);
                    continue; 
                } else {
                    pthread_mutex_unlock(&conn->mutex);
                    closeConnection(conn);
                    continue;
                }
            }

            if(conn->processing) {
                pthread_mutex_unlock(&conn->mutex);
                continue;
            }

            conn->processing = 1;
            pthread_mutex_unlock(&conn->mutex);

            Task task;
            task.conn = conn;

            queuePush(&gQueue, task);
        }
    }
}

// time-out functions
void* timeoutSweeperThread(void* args) {
    (void)args;
    while(1) {
        sleep(5); // Run the sweeper check every 5 seconds
        time_t now = time(NULL);

        for (int i = 0; i < MAX_CONNECTIONS; i++) {
            Connection *conn = &gConnections[i];

            // Fast lockless check first to save CPU
            if (!conn->active) continue; 

            pthread_mutex_lock(&conn->mutex);

            // Double check inside the lock to prevent race conditions
            if (conn->active && !conn->processing) {
                if (now - conn->lastActive > IDLE_TIMEOUT_SECONDS) {
                    printf("Client fd=%d idle timeout. Closing connection.\n", conn->fd);
                    
                    epoll_ctl(gEpollFD, EPOLL_CTL_DEL, conn->fd, NULL);
                    close(conn->fd);
                    
                    conn->fd = -1;
                    conn->active = 0;
                    conn->recvLen = 0;
                    conn->sendLen = 0;
                    conn->sendOffset = 0;
                }
            }
            
            pthread_mutex_unlock(&conn->mutex);
        }
    }
    return NULL;
}

int main() {
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        gConnections[i].fd = -1;
        gConnections[i].active = 0;
        gConnections[i].processing = 0;
        gConnections[i].recvLen = 0;
        gConnections[i].sendLen = 0;
        gConnections[i].sendOffset = 0;

        pthread_mutex_init(&gConnections[i].mutex, NULL);
    }

    queueInit(&gQueue);
    int serverfd = createServerSocket();
    createThreadPool();

    pthread_t sweeperTid;
    if(pthread_create(&sweeperTid, NULL, timeoutSweeperThread, NULL) != 0) {
        perror("Failed to create sweeper thread");
    }
    pthread_detach(sweeperTid);

    printf("-------------------------------------\n");
    printf("Server Started\n");
    printf("Port : %d\n", SERVER_PORT);
    printf("Workers : %d\n", THREAD_COUNT);
    printf("-------------------------------------\n");

    eventLoop(serverfd);

    close(serverfd);
    close(gEpollFD);

    return 0;
}