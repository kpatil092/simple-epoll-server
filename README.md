# Epoll server

### Results

#### simple epoll
```bash
-------------------------------------
Test Complete!
Time Elapsed     : 10.05 seconds
Total Messages   : 651,983
Data Transferred : 3.26 MB
Throughput       : 2.59 Mbps
Socket Errors    : 0
-------------------------------------
```

#### multithreaded epoll

```bash
-------------------------------------
Test Complete!
Time Elapsed     : 10.04 seconds
Total Messages   : 766,652
Data Transferred : 3.83 MB
Throughput       : 3.05 Mbps
Socket Errors    : 0
-------------------------------------
```

### Run

1. ` gcc -O3 -march=native -std=c11 server.cpp -o server -pthread`
2. `./server`
3. `python3 load_test.py`