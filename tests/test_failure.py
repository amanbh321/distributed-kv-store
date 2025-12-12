import requests
import time
import subprocess
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CONTROLLER_HOST, CONTROLLER_PORT

CONTROLLER_URL = f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}"


def print_header(title):
    print("\n" + "="*70)
    print(f"{title}")
    print("="*70)


def get_worker_status():
    """Get current worker status from controller"""
    try:
        response = requests.get(f"{CONTROLLER_URL}/workers", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return data.get('workers', {})
    except:
        pass
    return {}


def test_heartbeat_timeout():
    """Test heartbeat timeout detection"""
    print_header("TEST: Heartbeat Timeout Detection")
    
    print("\nStep 1: Check initial worker status")
    workers = get_worker_status()
    active_count = sum(1 for w in workers.values() if w['status'] == 'active')
    print(f"Active workers: {active_count}")
    
    if active_count < 4:
        print("⚠ Warning: Not all 4 workers are active")
        return False
    
    print("\nStep 2: Kill one worker (worker_4 on port 6003)")
    print("Run this command in another terminal:")
    print("  pkill -f 'worker.py worker_4'")
    print("\nOr programmatically:")
    
    try:
        # Kill worker_4
        subprocess.run(['pkill', '-f', 'worker.py worker_4'], check=False)
        print("✓ Worker killed")
    except Exception as e:
        print(f"✗ Could not kill worker: {e}")
        print("Please manually kill worker_4")
    
    print("\nStep 3: Wait for heartbeat timeout (15 seconds)...")
    time.sleep(16)
    
    print("\nStep 4: Check worker status after timeout")
    workers = get_worker_status()
    
    for worker_id, info in workers.items():
        status = info['status']
        symbol = "✓" if status == "active" else "⚠"
        print(f"  {symbol} {worker_id}: {status}")
    
    failed_count = sum(1 for w in workers.values() if w['status'] == 'failed')
    
    if failed_count >= 1:
        print(f"\n✓ SUCCESS: {failed_count} worker(s) detected as failed")
        return True
    else:
        print(f"\n✗ FAILED: No workers detected as failed")
        return False


def test_data_availability_after_failure():
    """Test if data is still available after one worker fails"""
    print_header("TEST: Data Availability After Failure")
    
    key = "test_availability"
    value = "still_here"
    
    print("\nStep 1: PUT data when all workers active")
    try:
        query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
        if query_resp.status_code != 200:
            print("✗ Failed to query controller")
            return False
        
        primary_worker = query_resp.json()['primary_worker']
        replica_urls = query_resp.json()['replicas']
        
        print(f"Primary: {primary_worker}")
        print(f"Replicas: {replica_urls}")
        
        put_resp = requests.post(
            f"{primary_worker}/put",
            json={'key': key, 'value': value},
            timeout=10
        )
        
        if put_resp.status_code != 200:
            print("✗ PUT failed")
            return False
        
        print("✓ Data written")
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    
    print("\nStep 2: Verify data on all replicas")
    available_on = []
    for replica_url in replica_urls:
        try:
            get_resp = requests.get(f"{replica_url}/get?key={key}", timeout=2)
            if get_resp.status_code == 200:
                available_on.append(replica_url)
                print(f"  ✓ Available on {replica_url}")
        except:
            print(f"  ✗ Not reachable: {replica_url}")
    
    print(f"\nData available on {len(available_on)}/{len(replica_urls)} replicas")
    
    if len(available_on) >= 2:
        print("✓ SUCCESS: Data replicated and available")
        return True
    else:
        print("✗ FAILED: Insufficient replicas")
        return False


def restart_worker_4():
    """Helper to restart worker_4"""
    print("\nRestarting worker_4...")
    try:
        subprocess.Popen([
            'python', 'worker/worker.py', 'worker_4', '6003'
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        print("✓ Worker restarted")
    except Exception as e:
        print(f"✗ Failed to restart: {e}")


def run_failure_tests():
    """Run all failure tests"""
    print("\n" + "="*70)
    print("🔥 FAILURE HANDLING TESTS")
    print("="*70)
    print("These tests will:")
    print("  1. Kill a worker")
    print("  2. Verify failure detection")
    print("  3. Check data availability")
    print("="*70)
    
    input("\nPress Enter to start tests...")
    
    results = []
    
    # Test 1: Heartbeat timeout
    results.append(("Heartbeat Timeout", test_heartbeat_timeout()))
    
    # Test 2: Data availability
    results.append(("Data Availability", test_data_availability_after_failure()))
    
    # Restart worker for cleanup
    restart_worker_4()
    
    # Summary
    print("\n" + "="*70)
    print("FAILURE TEST SUMMARY")
    print("="*70)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print("="*70)
    print(f"TOTAL: {passed}/{total} tests passed")
    print("="*70)


if __name__ == '__main__':
    run_failure_tests()