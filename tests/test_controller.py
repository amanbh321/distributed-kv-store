import requests
import json
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import CONTROLLER_HOST, CONTROLLER_PORT

BASE_URL = f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}"

def test_controller_status():
    """Test if controller is running"""
    print("\n" + "="*60)
    print("TEST 1: Controller Status")
    print("="*60)
    try:
        response = requests.get(f"{BASE_URL}/status")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        assert response.status_code == 200
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {str(e)}")

def test_register_worker():
    """Test worker registration"""
    print("\n" + "="*60)
    print("TEST 2: Register Worker")
    print("="*60)
    try:
        worker_data = {
            "worker_id": "test_worker_1",
            "host": "localhost",
            "port": 7000
        }
        response = requests.post(
            f"{BASE_URL}/register",
            json=worker_data
        )
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        assert response.status_code == 201
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {str(e)}")

def test_query_key():
    """Test key query"""
    print("\n" + "="*60)
    print("TEST 3: Query Key Location")
    print("="*60)
    try:
        response = requests.get(f"{BASE_URL}/query?key=test_key_123")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        assert response.status_code == 200
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {str(e)}")

def test_get_workers():
    """Test getting all workers"""
    print("\n" + "="*60)
    print("TEST 4: Get All Workers")
    print("="*60)
    try:
        response = requests.get(f"{BASE_URL}/workers")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        assert response.status_code == 200
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {str(e)}")

def test_heartbeat():
    """Test heartbeat"""
    print("\n" + "="*60)
    print("TEST 5: Send Heartbeat")
    print("="*60)
    try:
        heartbeat_data = {
            "worker_id": "test_worker_1"
        }
        response = requests.post(
            f"{BASE_URL}/heartbeat",
            json=heartbeat_data
        )
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        assert response.status_code == 200
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {str(e)}")

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🧪 CONTROLLER API TESTS")
    print("="*60)
    print(f"Testing Controller at: {BASE_URL}")
    print("Make sure the controller is running first!")
    print("="*60)
    
    input("\nPress Enter to start tests...")
    
    # Run all tests
    test_controller_status()
    test_register_worker()
    test_query_key()
    test_get_workers()
    test_heartbeat()
    
    print("\n" + "="*60)
    print("✓ ALL TESTS COMPLETED")
    print("="*60)