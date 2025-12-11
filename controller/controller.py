from flask import Flask, request, jsonify
import threading
import time
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
# from controller.utils import ConsistentHash, WorkerRegistry
from utils import ConsistentHash, WorkerRegistry

app = Flask(__name__)

# Global state
consistent_hash = ConsistentHash(NUM_WORKERS, VIRTUAL_NODES)
worker_registry = WorkerRegistry(HEARTBEAT_TIMEOUT)
lock = threading.Lock()  # For thread-safe operations


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
                # TODO: Implement re-replication logic in Step 5
                # For now, just log the failure


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