from flask import Flask, request, jsonify
import threading
import time
import sys
import os
import requests

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from utils import ConsistentHash, WorkerRegistry

app = Flask(__name__)

# Global state
consistent_hash = ConsistentHash(NUM_WORKERS, VIRTUAL_NODES)
worker_registry = WorkerRegistry(HEARTBEAT_TIMEOUT)
lock = threading.Lock()

# Track keys stored on each worker (for re-replication)
worker_keys = {}  # worker_id -> set of keys


@app.route('/register', methods=['POST'])
def register_worker():
    """
    Register a new worker node
    POST /register
    Body: {"worker_id": "worker_1", "host": "localhost", "port": 6000}
    """
    try:
        data = request.get_json()
        worker_id = data.get('worker_id')
        host = data.get('host')
        port = data.get('port')
        
        if not all([worker_id, host, port]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: worker_id, host, port'
            }), 400
        
        with lock:
            # Register worker in registry
            worker_registry.register_worker(worker_id, host, port)
            
            # Add worker to consistent hash ring
            consistent_hash.add_worker(worker_id)
            
            # Initialize key tracking for this worker
            if worker_id not in worker_keys:
                worker_keys[worker_id] = set()
        
        print(f"✓ Worker registered: {worker_id} at {host}:{port}")
        
        return jsonify({
            'success': True,
            'message': f'Worker {worker_id} registered successfully',
            'worker_id': worker_id
        }), 201
        
    except Exception as e:
        print(f"✗ Error registering worker: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    """
    Receive heartbeat from worker
    POST /heartbeat
    Body: {"worker_id": "worker_1"}
    """
    try:
        data = request.get_json()
        worker_id = data.get('worker_id')
        
        if not worker_id:
            return jsonify({
                'success': False,
                'error': 'Missing worker_id'
            }), 400
        
        with lock:
            success = worker_registry.update_heartbeat(worker_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Heartbeat received'
            }), 200
        else:
            return jsonify({
                'success': False,
                'error': f'Worker {worker_id} not registered'
            }), 404
            
    except Exception as e:
        print(f"✗ Error processing heartbeat: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/query', methods=['GET'])
def query_key():
    """
    Query which worker is responsible for a key
    GET /query?key=<key>
    """
    try:
        key = request.args.get('key')
        
        if not key:
            return jsonify({
                'success': False,
                'error': 'Missing key parameter'
            }), 400
        
        with lock:
            # Get primary worker and replicas
            replicas = consistent_hash.get_replicas(key, REPLICATION_FACTOR)
            
            if not replicas:
                return jsonify({
                    'success': False,
                    'error': 'No workers available'
                }), 503
            
            primary_worker_id = replicas[0]
            primary_worker = worker_registry.get_worker(primary_worker_id)
            
            if not primary_worker:
                return jsonify({
                    'success': False,
                    'error': f'Primary worker {primary_worker_id} not found'
                }), 500
            
            # Get URLs for all replicas
            replica_urls = []
            for replica_id in replicas:
                url = worker_registry.get_worker_url(replica_id)
                if url:
                    replica_urls.append(url)
            
            # Track that this key should be on these workers
            for replica_id in replicas:
                if replica_id in worker_keys:
                    worker_keys[replica_id].add(key)
        
        return jsonify({
            'success': True,
            'key': key,
            'primary_worker': primary_worker['url'],
            'primary_worker_id': primary_worker_id,
            'replicas': replica_urls,
            'replica_ids': replicas
        }), 200
        
    except Exception as e:
        print(f"✗ Error querying key: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/notify_put', methods=['POST'])
def notify_put():
    """
    Workers notify controller when they store a key
    POST /notify_put
    Body: {"worker_id": "worker_1", "key": "mykey", "replicas": ["worker_1", "worker_2"]}
    """
    try:
        data = request.get_json()
        worker_id = data.get('worker_id')
        key = data.get('key')
        replica_ids = data.get('replicas', [])
        
        if not all([worker_id, key]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields'
            }), 400
        
        with lock:
            # Track this key on all replicas
            for replica_id in replica_ids:
                if replica_id in worker_keys:
                    worker_keys[replica_id].add(key)
        
        return jsonify({
            'success': True,
            'message': 'Key tracking updated'
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/workers', methods=['GET'])
def get_workers():
    """
    Get list of all workers and their status
    GET /workers
    """
    try:
        with lock:
            workers = worker_registry.get_all_workers()
        
        return jsonify({
            'success': True,
            'workers': workers,
            'total': len(workers),
            'active': len([w for w in workers.values() if w['status'] == 'active'])
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/status', methods=['GET'])
def get_status():
    """
    Get controller status
    GET /status
    """
    try:
        with lock:
            workers = worker_registry.get_all_workers()
            active_workers = [w for w in workers.values() if w['status'] == 'active']
        
        return jsonify({
            'success': True,
            'status': 'running',
            'total_workers': len(workers),
            'active_workers': len(active_workers),
            'replication_factor': REPLICATION_FACTOR,
            'heartbeat_timeout': HEARTBEAT_TIMEOUT
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def get_key_from_worker(worker_url, key):
    """Fetch a key's value from a worker"""
    try:
        response = requests.get(f"{worker_url}/get?key={key}", timeout=5)
        if response.status_code == 200:
            return response.json().get('value')
    except:
        pass
    return None


def replicate_key_to_worker(worker_url, key, value):
    """Replicate a key to a worker"""
    try:
        response = requests.post(
            f"{worker_url}/replicate",
            json={'key': key, 'value': value},
            timeout=5
        )
        return response.status_code == 200
    except:
        return False


def handle_worker_failure(failed_worker_id):
    """Handle re-replication when a worker fails"""
    print(f"🔄 Starting re-replication for failed worker: {failed_worker_id}")
    
    with lock:
        # Get keys that were on the failed worker
        if failed_worker_id not in worker_keys:
            print(f"  No keys tracked for {failed_worker_id}")
            return
        
        keys_to_recover = list(worker_keys[failed_worker_id])
        active_workers = worker_registry.get_active_workers()
    
    if not keys_to_recover:
        print(f"  No keys to recover")
        return
    
    print(f"  Keys to recover: {len(keys_to_recover)}")
    recovered = 0
    failed = 0
    
    for key in keys_to_recover:
        try:
            # Find which workers should have this key
            with lock:
                target_replicas = consistent_hash.get_replicas(key, REPLICATION_FACTOR)
            
            # Find a surviving worker that has the data
            source_value = None
            source_worker = None
            
            for replica_id in target_replicas:
                if replica_id == failed_worker_id:
                    continue
                
                worker_url = worker_registry.get_worker_url(replica_id)
                if worker_url:
                    value = get_key_from_worker(worker_url, key)
                    if value is not None:
                        source_value = value
                        source_worker = replica_id
                        break
            
            if source_value is None:
                print(f"  ✗ Could not find data for key: {key}")
                failed += 1
                continue
            
            # Count how many replicas currently exist
            current_replicas = 0
            for replica_id in target_replicas:
                if replica_id == failed_worker_id:
                    continue
                worker_url = worker_registry.get_worker_url(replica_id)
                if worker_url:
                    value = get_key_from_worker(worker_url, key)
                    if value is not None:
                        current_replicas += 1
            
            # If we have enough replicas, no need to re-replicate
            if current_replicas >= REPLICATION_FACTOR - 1:
                recovered += 1
                continue
            
            # Need to create a new replica on a different worker
            # Find an active worker not in the current replica set
            with lock:
                available_workers = [w for w in active_workers 
                                   if w not in target_replicas]
            
            if not available_workers:
                print(f"  ⚠ No available workers for key: {key}")
                failed += 1
                continue
            
            # Replicate to the first available worker
            new_replica_id = available_workers[0]
            new_replica_url = worker_registry.get_worker_url(new_replica_id)
            
            if replicate_key_to_worker(new_replica_url, key, source_value):
                print(f"  ✓ Re-replicated {key} to {new_replica_id}")
                with lock:
                    worker_keys[new_replica_id].add(key)
                recovered += 1
            else:
                print(f"  ✗ Failed to re-replicate {key}")
                failed += 1
                
        except Exception as e:
            print(f"  ✗ Error recovering {key}: {str(e)}")
            failed += 1
    
    print(f"✓ Re-replication complete: {recovered} recovered, {failed} failed")


def monitor_workers():
    """
    Background thread to monitor worker health
    Runs continuously and checks for failed workers
    """
    print("✓ Worker health monitor started")
    
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        
        with lock:
            failed_workers = worker_registry.check_failed_workers()
        
        if failed_workers:
            for worker_id in failed_workers:
                print(f"⚠ Worker failed: {worker_id}")
                # Start re-replication in a separate thread
                replication_thread = threading.Thread(
                    target=handle_worker_failure,
                    args=(worker_id,),
                    daemon=True
                )
                replication_thread.start()


def start_controller():
    """Start the controller server"""
    print("=" * 60)
    print("🚀 Starting Distributed KV Store Controller")
    print("=" * 60)
    print(f"Controller URL: http://{CONTROLLER_HOST}:{CONTROLLER_PORT}")
    print(f"Replication Factor: {REPLICATION_FACTOR}")
    print(f"Expected Workers: {NUM_WORKERS}")
    print(f"Heartbeat Timeout: {HEARTBEAT_TIMEOUT}s")
    print("=" * 60)
    
    # Start health monitoring thread
    monitor_thread = threading.Thread(target=monitor_workers, daemon=True)
    monitor_thread.start()
    
    # Start Flask server
    app.run(host=CONTROLLER_HOST, port=CONTROLLER_PORT, debug=False)


if __name__ == '__main__':
    start_controller()