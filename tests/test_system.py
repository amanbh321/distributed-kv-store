import requests
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CONTROLLER_HOST, CONTROLLER_PORT

CONTROLLER_URL = f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}"
WORKER_URLS = [
    "http://localhost:6000",
    "http://localhost:6001",
    "http://localhost:6002",
    "http://localhost:6003"
]


def print_header(test_name):
    print("\n" + "="*70)
    print(f"TEST: {test_name}")
    print("="*70)


def print_result(passed, message=""):
    if passed:
        print(f"✓ PASSED {message}")
    else:
        print(f"✗ FAILED {message}")
    return passed


def test_1_controller_status():
    """Test 1: Check if controller is running"""
    print_header("Controller Status")
    try:
        response = requests.get(f"{CONTROLLER_URL}/status", timeout=5)
        data = response.json()
        print(f"Controller status: {data}")
        return print_result(
            response.status_code == 200 and data.get('status') == 'running',
            f"- {data.get('active_workers', 0)} active workers"
        )
    except Exception as e:
        return print_result(False, f"- Error: {str(e)}")


def test_2_workers_registered():
    """Test 2: Check if all workers are registered"""
    print_header("Workers Registration")
    try:
        response = requests.get(f"{CONTROLLER_URL}/workers", timeout=5)
        data = response.json()
        
        total = data.get('total', 0)
        active = data.get('active', 0)
        
        print(f"Total workers: {total}")
        print(f"Active workers: {active}")
        
        return print_result(total >= 4, f"- Expected 4, got {total}")
    except Exception as e:
        return print_result(False, f"- Error: {str(e)}")


def test_3_put_operation():
    """Test 3: PUT operation with replication"""
    print_header("PUT Operation")
    
    test_cases = [
        ("user:1", "Alice"),
        ("user:2", "Bob"),
        ("product:100", "Laptop"),
        ("product:101", "Phone")
    ]
    
    passed = 0
    for key, value in test_cases:
        try:
            # Query controller
            query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
            if query_resp.status_code != 200:
                print(f"  ✗ {key}: Failed to query controller")
                continue
            
            primary_worker = query_resp.json()['primary_worker']
            
            # PUT operation
            put_resp = requests.post(
                f"{primary_worker}/put",
                json={'key': key, 'value': value},
                timeout=10
            )
            
            if put_resp.status_code == 200:
                replicas = put_resp.json().get('replicas_written', 0)
                print(f"  ✓ {key} = {value} (replicas: {replicas})")
                passed += 1
            else:
                print(f"  ✗ {key}: PUT failed")
        except Exception as e:
            print(f"  ✗ {key}: Error - {str(e)}")
    
    return print_result(passed == len(test_cases), f"- {passed}/{len(test_cases)} passed")


def test_4_get_operation():
    """Test 4: GET operation"""
    print_header("GET Operation")
    
    test_cases = [
        ("user:1", "Alice"),
        ("user:2", "Bob"),
        ("product:100", "Laptop"),
        ("product:101", "Phone")
    ]
    
    passed = 0
    for key, expected_value in test_cases:
        try:
            # Query controller
            query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
            if query_resp.status_code != 200:
                print(f"  ✗ {key}: Failed to query controller")
                continue
            
            primary_worker = query_resp.json()['primary_worker']
            
            # GET operation
            get_resp = requests.get(f"{primary_worker}/get?key={key}", timeout=5)
            
            if get_resp.status_code == 200:
                actual_value = get_resp.json()['value']
                if actual_value == expected_value:
                    print(f"  ✓ {key} = {actual_value}")
                    passed += 1
                else:
                    print(f"  ✗ {key}: Expected {expected_value}, got {actual_value}")
            else:
                print(f"  ✗ {key}: GET failed")
        except Exception as e:
            print(f"  ✗ {key}: Error - {str(e)}")
    
    return print_result(passed == len(test_cases), f"- {passed}/{len(test_cases)} passed")


def test_5_replication_verification():
    """Test 5: Verify data is replicated"""
    print_header("Replication Verification")
    
    key = "replication_test"
    value = "replicated_data"
    
    try:
        # Query controller for replicas
        query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
        if query_resp.status_code != 200:
            return print_result(False, "- Failed to query controller")
        
        data = query_resp.json()
        primary_worker = data['primary_worker']
        replica_urls = data['replicas']
        
        print(f"Primary: {primary_worker}")
        print(f"Replicas: {replica_urls}")
        
        # PUT to primary
        put_resp = requests.post(
            f"{primary_worker}/put",
            json={'key': key, 'value': value},
            timeout=10
        )
        
        if put_resp.status_code != 200:
            return print_result(False, "- PUT failed")
        
        time.sleep(1)  # Wait for replication
        
        # Verify on all replicas
        found_count = 0
        for replica_url in replica_urls:
            try:
                get_resp = requests.get(f"{replica_url}/get?key={key}", timeout=5)
                if get_resp.status_code == 200:
                    actual = get_resp.json()['value']
                    if actual == value:
                        print(f"  ✓ Found on {replica_url}")
                        found_count += 1
                    else:
                        print(f"  ✗ Wrong value on {replica_url}")
                else:
                    print(f"  ✗ Not found on {replica_url}")
            except Exception as e:
                print(f"  ✗ Error checking {replica_url}: {str(e)}")
        
        return print_result(found_count >= 2, f"- Found on {found_count} replicas (need 2+)")
        
    except Exception as e:
        return print_result(False, f"- Error: {str(e)}")


def test_6_key_distribution():
    """Test 6: Check key distribution across workers"""
    print_header("Key Distribution")
    
    keys = [f"key_{i}" for i in range(20)]
    distribution = {}
    
    for key in keys:
        try:
            query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
            if query_resp.status_code == 200:
                primary = query_resp.json()['primary_worker']
                distribution[primary] = distribution.get(primary, 0) + 1
        except:
            pass
    
    print("\nKey distribution:")
    for worker, count in sorted(distribution.items()):
        print(f"  {worker}: {count} keys")
    
    # Check if keys are distributed (not all on one worker)
    return print_result(
        len(distribution) > 1,
        f"- Keys distributed across {len(distribution)} workers"
    )


def test_7_non_existent_key():
    """Test 7: GET non-existent key"""
    print_header("Non-Existent Key")
    
    key = "does_not_exist_12345"
    
    try:
        query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
        if query_resp.status_code != 200:
            return print_result(False, "- Failed to query controller")
        
        primary_worker = query_resp.json()['primary_worker']
        
        get_resp = requests.get(f"{primary_worker}/get?key={key}", timeout=5)
        
        return print_result(
            get_resp.status_code == 404,
            f"- Correctly returned 404"
        )
    except Exception as e:
        return print_result(False, f"- Error: {str(e)}")


def test_8_concurrent_operations():
    """Test 8: Multiple concurrent operations"""
    print_header("Concurrent Operations")
    
    import threading
    
    results = {'success': 0, 'failed': 0}
    lock = threading.Lock()
    
    def put_data(key, value):
        try:
            query_resp = requests.get(f"{CONTROLLER_URL}/query?key={key}", timeout=5)
            if query_resp.status_code == 200:
                primary = query_resp.json()['primary_worker']
                put_resp = requests.post(
                    f"{primary}/put",
                    json={'key': key, 'value': value},
                    timeout=10
                )
                with lock:
                    if put_resp.status_code == 200:
                        results['success'] += 1
                    else:
                        results['failed'] += 1
        except:
            with lock:
                results['failed'] += 1
    
    threads = []
    for i in range(10):
        t = threading.Thread(target=put_data, args=(f"concurrent_{i}", f"value_{i}"))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    print(f"Success: {results['success']}/10")
    print(f"Failed: {results['failed']}/10")
    
    return print_result(results['success'] >= 8, f"- {results['success']}/10 succeeded")


def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print("🧪 DISTRIBUTED KEY-VALUE STORE - SYSTEM TESTS")
    print("="*70)
    print("Make sure controller and all 4 workers are running!")
    print("="*70)
    
    input("\nPress Enter to start tests...")
    
    results = []
    
    # Run all tests
    results.append(("Controller Status", test_1_controller_status()))
    results.append(("Workers Registered", test_2_workers_registered()))
    results.append(("PUT Operation", test_3_put_operation()))
    results.append(("GET Operation", test_4_get_operation()))
    results.append(("Replication", test_5_replication_verification()))
    results.append(("Key Distribution", test_6_key_distribution()))
    results.append(("Non-Existent Key", test_7_non_existent_key()))
    results.append(("Concurrent Operations", test_8_concurrent_operations()))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print("="*70)
    print(f"TOTAL: {passed}/{total} tests passed ({passed*100//total}%)")
    print("="*70)
    
    return passed == total


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)