import asyncio
import time
import sys

# --- Test Configuration ---
HOST = '127.0.0.1'
PORT = 2525
CONCURRENCY = 100       # Number of simultaneous clients
DURATION = 10.0         # How long to run the test (seconds)
PAYLOAD = b"Ping!"      # The data to send

async def client_task(stats):
    try:
        # Open a non-blocking TCP connection to your C Server
        reader, writer = await asyncio.open_connection(HOST, PORT)
        end_time = time.time() + DURATION
        
        while time.time() < end_time:
            writer.write(PAYLOAD)
            await writer.drain()
            
            # Wait for the C server to echo it back
            data = await reader.read(1024)
            if not data:
                break
                
            stats['messages'] += 1
            stats['bytes'] += len(data)
            
        writer.close()
        await writer.wait_closed()
        
    except Exception as e:
        stats['errors'] += 1

async def main():
    print(f"Starting Load Test...")
    print(f"Target: {HOST}:{PORT} | Connections: {CONCURRENCY} | Time: {DURATION}s")
    
    stats = {'messages': 0, 'bytes': 0, 'errors': 0}
    start_time = time.time()
    
    # Spawn the concurrent clients
    tasks = [client_task(stats) for _ in range(CONCURRENCY)]
    await asyncio.gather(*tasks)
    
    elapsed = time.time() - start_time
    
    # Calculate Megabits per second (Mbps)
    bits = stats['bytes'] * 8
    mbps = (bits / 1_000_000) / elapsed
    
    print("\n-------------------------------------")
    print("Test Complete!")
    print(f"Time Elapsed     : {elapsed:.2f} seconds")
    print(f"Total Messages   : {stats['messages']:,}")
    print(f"Data Transferred : {stats['bytes'] / 1_000_000:.2f} MB")
    print(f"Throughput       : {mbps:.2f} Mbps")
    print(f"Socket Errors    : {stats['errors']}")
    print("-------------------------------------")

if __name__ == "__main__":
    # Windows users might need a different event loop policy, but on Linux this is perfect
    asyncio.run(main())
