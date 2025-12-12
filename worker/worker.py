from flask import Flask, request, jsonify
import requests
import threading
import time
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *

app = Flask(__name__)

# Worker state
worker_id = None
worker_port = None
storage = {}  # Simple dictionary to store key-value pairs
lock = threading.Lock()


@app.route('/get', methods=['GET'])
def get_key():
    """
    GET operation - retrieve value for a key
    GET /get?key=<key>
    """
    try:
        key = request.args.get('key')
        
        if not key:
            return jsonify({
                'success': False,
                'error': 'Missing key parameter'
            }), 400
        
        with lock:
            if key in storage:
                value = storage[key]
                print(f"✓ GET: {key} = {value}")
                return jsonify({
                    'success': True,
                    'key': key,
                    'value': value
                }), 200
            else:
                print(f"✗ GET: {key} not found")
                return jsonify({
                    'success': False,
                    'error': 'Key not found'
                }), 404
                
    except Exception as e:
        print(f"✗ Error in GET: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/put', methods=['POST'])
def put_key():
    """
    PUT operation - store key-value pair
    POST /put
    Body: {"key": "mykey", "value": "myvalue"}
    """
    try:
        data = request.get_json()
        key = data.get('key')
        value = data.get('value')
        
        if not key or value is None:
            return jsonify({
                'success': False,
                'error': 'Missing key or value'
            }), 400
        
        # Store locally
        with lock:
            storage[key] = value
        
        print(f"✓ PUT: {key} = {value}")
        
        # Get replica workers from controller
        replicas_response = requests.get(
            f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}/query?key={key}",
            timeout=5
        )
        
        if replicas_response.status_code != 200:
            return jsonify({
                'success': True,
                'replicas_written': 1,
                'warning': 'Could not contact controller for replication'
            }), 200
        
        replica_data = replicas_response.json()
        replica_urls = replica_data.get('replicas', [])
        
        # Replicate to other workers (excluding self)
        my_url = f"http://localhost:{worker_port}"
        other_replicas = [url for url in replica_urls if url != my_url]
        
        replicas_written = 1  # Count self
        
        # Synchronous replication - write to first replica
        if len(other_replicas) >= 1:
            if replicate_to_worker(other_replicas[0], key, value):
                replicas_written += 1
        
        # Synchronous replication - write to second replica
        if len(other_replicas) >= 2:
            if replicate_to_worker(other_replicas[1], key, value):
                replicas_written += 1
        
        # Check if we have enough replicas
        if replicas_written >= SYNC_REPLICAS:
            print(f"✓ PUT successful: {replicas_written}/{REPLICATION_FACTOR} replicas written")
            return jsonify({
                'success': True,
                'key': key,
                'replicas_written': replicas_written
            }), 200
        else:
            print(f"⚠ PUT warning: Only {replicas_written}/{SYNC_REPLICAS} replicas written")
            return jsonify({
                'success': False,
                'error': f'Only {replicas_written} replicas written, need {SYNC_REPLICAS}',
                'replicas_written': replicas_written
            }), 500
            
    except Exception as e:
        print(f"✗ Error in PUT: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/replicate', methods=['POST'])
def replicate():
    """
    Replicate operation - receive data from primary worker
    POST /replicate
    Body: {"key": "mykey", "value": "myvalue"}
    """
    try:
        data = request.get_json()
        key = data.get('key')
        value = data.get('value')
        
        if not key or value is None:
            return jsonify({
                'success': False,
                'error': 'Missing key or value'
            }), 400
        
        # Store the replicated data
        with lock:
            storage[key] = value
        
        print(f"✓ REPLICATE: {key} = {value}")
        
        return jsonify({
            'success': True,
            'message': 'Replication successful'
        }), 200
        
    except Exception as e:
        print(f"✗ Error in REPLICATE: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/status', methods=['GET'])
def status():
    """Get worker status"""
    with lock:
        num_keys = len(storage)
    
    return jsonify({
        'success': True,
        'worker_id': worker_id,
        'status': 'active',
        'num_keys': num_keys
    }), 200


def replicate_to_worker(worker_url, key, value):
    """Helper function to replicate data to another worker"""
    try:
        response = requests.post(
            f"{worker_url}/replicate",
            json={'key': key, 'value': value},
            timeout=5
        )
        return response.status_code == 200
    except Exception as e:
        print(f"✗ Replication failed to {worker_url}: {str(e)}")
        return False


def send_heartbeat():
    """Send periodic heartbeat to controller"""
    while True:
        try:
            time.sleep(HEARTBEAT_INTERVAL)
            response = requests.post(
                f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}/heartbeat",
                json={'worker_id': worker_id},
                timeout=2
            )
            if response.status_code == 200:
                print(f"💓 Heartbeat sent")
            else:
                print(f"⚠ Heartbeat failed: {response.status_code}")
        except Exception as e:
            print(f"✗ Heartbeat error: {str(e)}")


def register_with_controller():
    """Register this worker with the controller"""
    try:
        response = requests.post(
            f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}/register",
            json={
                'worker_id': worker_id,
                'host': 'localhost',
                'port': worker_port
            },
            timeout=5
        )
        
        if response.status_code == 201:
            print(f"✓ Registered with controller")
            return True
        else:
            print(f"✗ Registration failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Registration error: {str(e)}")
        return False


def start_worker(w_id, port):
    """Start the worker server"""
    global worker_id, worker_port
    worker_id = w_id
    worker_port = port
    
    print("=" * 60)
    print(f"🚀 Starting Worker: {worker_id}")
    print("=" * 60)
    print(f"Worker URL: http://localhost:{worker_port}")
    print(f"Controller: http://{CONTROLLER_HOST}:{CONTROLLER_PORT}")
    print("=" * 60)
    
    # Register with controller
    if register_with_controller():
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
        heartbeat_thread.start()
        
        # Start Flask server
        app.run(host='localhost', port=worker_port, debug=False)
    else:
        print("✗ Failed to register with controller. Exiting.")
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python worker.py <worker_id> <port>")
        print("Example: python worker.py worker_1 6000")
        sys.exit(1)
    
    w_id = sys.argv[1]
    port = int(sys.argv[2])
    start_worker(w_id, port)