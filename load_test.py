import asyncio
import time
import multiprocessing as mp

# --- Test Configuration ---
HOST = '127.0.0.1'
PORT = 2525
TOTAL_CONNECTIONS = 400   # 400 total clients
PROCESSES = 4             # Use 4 CPU cores to generate load
DURATION = 10.0
PAYLOAD = b"Ping!"

async def client_task():
    messages = 0
    bytes_transferred = 0
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
        end_time = time.time() + DURATION
        
        while time.time() < end_time:
            writer.write(PAYLOAD)
            await writer.drain()
            
            data = await reader.read(1024)
            if not data:
                break
                
            messages += 1
            bytes_transferred += len(data)
            
        writer.close()
        await writer.wait_closed()
        return messages, bytes_transferred, 0
    except Exception:
        return 0, 0, 1

async def worker_loop(concurrency):
    tasks = [client_task() for _ in range(concurrency)]
    results = await asyncio.gather(*tasks)
    
    total_msgs = sum(r[0] for r in results)
    total_bytes = sum(r[1] for r in results)
    total_errors = sum(r[2] for r in results)
    return total_msgs, total_bytes, total_errors

def run_worker(concurrency):
    # This runs in a completely separate CPU process
    return asyncio.run(worker_loop(concurrency))

if __name__ == '__main__':
    print(f"Starting Multi-Core Load Test...")
    print(f"Target: {HOST}:{PORT} | Total Connections: {TOTAL_CONNECTIONS} | Processes: {PROCESSES}")
    start_time = time.time()
    
    # Divide the connections evenly across the CPU cores
    concurrency_per_process = TOTAL_CONNECTIONS // PROCESSES
    
    with mp.Pool(PROCESSES) as pool:
        # Launch the 4 Python processes simultaneously
        results = pool.map(run_worker, [concurrency_per_process] * PROCESSES)
    
    elapsed = time.time() - start_time
    
    final_msgs = sum(r[0] for r in results)
    final_bytes = sum(r[1] for r in results)
    final_errors = sum(r[2] for r in results)
    
    mbps = ((final_bytes * 8) / 1_000_000) / elapsed
    
    print("\n-------------------------------------")
    print("Test Complete!")
    print(f"Time Elapsed     : {elapsed:.2f} seconds")
    print(f"Total Messages   : {final_msgs:,}")
    print(f"Data Transferred : {final_bytes / 1_000_000:.2f} MB")
    print(f"Throughput       : {mbps:.2f} Mbps")
    print(f"Socket Errors    : {final_errors}")
    print("-------------------------------------")