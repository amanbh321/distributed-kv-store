import requests
import time
import subprocess
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CONTROLLER_HOST, CONTROLLER_PORT

CONTROLLER_URL = f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}"
WORKER_URLS = {
    'worker_1': 'http://localhost:6000',
    'worker_2': 'http://localhost:6001',
    'worker_3': 'http://localhost:6002',
    'worker_4': 'http://localhost:6003'
}


def print_section(title):
    print("\n" + "="*70)
    print(title)
    print("="*70)


def check_key_on_workers(key):
    """Check which workers have a specific key"""
    results = {}
    for worker_id, url in WORKER_URLS.items():
        try:
            response = requests.get(f"{url}/get?key={key}", timeout=2)
            if response.status_code == 200:
                results[worker_id] = response.json().get('value')
            else:
                results[worker_id] = None
        except:
            results[worker_id] = "UNREACHABLE"
    return results


def test_rereplication():
    """Complete re-replication test"""
    
    print_section("🧪 RE-REPLICATION TEST")
    print("This test will:")
    print("  1. Store test data")
    print("  2. Kill a worker")
    print("  3. Wait for re-replication")
    print("  4. Verify data is re-replicated")
    
    input("\nPress Enter to start...")
    
    # Step 1: Store test data
    print_section("Step 1: Storing Test Data")
    
    test_keys = {
        'user:alice': 'Alice Smith',
        'user:bob': 'Bob Jones',
        'product:laptop': 'MacBook Pro',
        'product:phone': 'iPhone 15'
    }
    
    for key, value in test_keys.items():
        try:
            # Query controller for key location
            query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
            if query_resp.status_code != 200:
                print(f"  ✗ Failed to query for {key}")
                continue
            
            data = query_resp.json()
            primary_worker = data['primary_worker']
            replica_ids = data['replica_ids']
            
            # PUT operation
            put_resp = requests.post(
                f"{primary_worker}/put",
                json={'key': key, 'value': value},
                timeout=10
            )
            
            if put_resp.status_code == 200:
                print(f"  ✓ Stored: {key} = {value}")
                print(f"    Replicas: {replica_ids}")
            else:
                print(f"  ✗ Failed to store {key}")
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    time.sleep(2)
    
    # Step 2: Check initial replication
    print_section("Step 2: Checking Initial Replication")
    
    for key in test_keys.keys():
        results = check_key_on_workers(key)
        count = sum(1 for v in results.values() if v and v != "UNREACHABLE")
        print(f"  {key}: found on {count} workers")
        for worker_id, value in results.items():
            if value and value != "UNREACHABLE":
                print(f"    ✓ {worker_id}")
    
    # Step 3: Kill a worker
    print_section("Step 3: Simulating Worker Failure")
    
    print("\nKilling worker_4 (port 6003)...")
    try:
        subprocess.run(['pkill', '-f', 'worker.py worker_4'], check=False)
        print("✓ Worker killed")
    except Exception as e:
        print(f"⚠ Could not kill worker: {e}")
        print("Please manually run: pkill -f 'worker.py worker_4'")
        input("Press Enter after killing worker_4...")
    
    # Step 4: Wait for failure detection
    print_section("Step 4: Waiting for Failure Detection")
    print("Waiting 20 seconds for heartbeat timeout and re-replication...")
    
    for i in range(20, 0, -1):
        print(f"  {i} seconds remaining...", end='\r')
        time.sleep(1)
    print("\n")
    
    # Check worker status
    try:
        workers_resp = requests.get(f"{CONTROLLER_URL}/workers", timeout=5)
        if workers_resp.status_code == 200:
            workers = workers_resp.json().get('workers', {})
            print("Worker Status:")
            for worker_id, info in workers.items():
                status = info['status']
                symbol = "✓" if status == "active" else "⚠"
                print(f"  {symbol} {worker_id}: {status}")
    except:
        print("⚠ Could not check worker status")
    
    # Step 5: Verify re-replication
    print_section("Step 5: Verifying Re-replication")
    
    all_recovered = True
    
    for key in test_keys.keys():
        results = check_key_on_workers(key)
        
        # Count active replicas (excluding unreachable workers)
        active_count = sum(1 for w_id, v in results.items() 
                          if v and v != "UNREACHABLE")
        
        print(f"\n  Key: {key}")
        print(f"  Active replicas: {active_count}")
        
        for worker_id, value in results.items():
            if value == "UNREACHABLE":
                print(f"    ⚠ {worker_id}: UNREACHABLE (expected for worker_4)")
            elif value:
                print(f"    ✓ {worker_id}: has data")
            else:
                print(f"    ✗ {worker_id}: no data")
        
        if active_count < 2:
            print(f"    ⚠ WARNING: Only {active_count} active replicas (need 2+)")
            all_recovered = False
        else:
            print(f"    ✓ Sufficient replicas ({active_count})")
    
    # Final result
    print_section("TEST RESULT")
    
    if all_recovered:
        print("✓ SUCCESS: All keys have sufficient replicas after failure")
        print("  Re-replication is working correctly!")
        return True
    else:
        print("⚠ PARTIAL: Some keys may not have enough replicas")
        print("  Re-replication may need more time or manual verification")
        return False


def restart_worker_4():
    """Restart worker_4 for cleanup"""
    print_section("Cleanup: Restarting worker_4")
    try:
        subprocess.Popen([
            'python', 'worker/worker.py', 'worker_4', '6003'
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        print("✓ Worker restarted")
    except Exception as e:
        print(f"⚠ Could not restart worker: {e}")
        print("Please manually restart: python worker/worker.py worker_4 6003")


if __name__ == '__main__':
    try:
        success = test_rereplication()
        
        print("\n")
        input("Press Enter to restart worker_4 and cleanup...")
        restart_worker_4()
        
        if success:
            print("\n✓ Test completed successfully!")
            sys.exit(0)
        else:
            print("\n⚠ Test completed with warnings")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nTest interrupted. Cleaning up...")
        restart_worker_4()
        sys.exit(1)