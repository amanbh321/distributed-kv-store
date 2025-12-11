import hashlib
import time
from typing import List, Dict, Optional

class ConsistentHash:
    """Consistent hashing implementation for key partitioning"""
    
    def __init__(self, num_workers: int, virtual_nodes: int = 150):
        self.num_workers = num_workers
        self.virtual_nodes = virtual_nodes
        self.ring = {}  # hash_value -> worker_id
        self.sorted_keys = []
        
    def _hash(self, key: str) -> int:
        """Generate hash value for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_worker(self, worker_id: str):
        """Add a worker to the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_key = f"{worker_id}:vnode{i}"
            hash_val = self._hash(virtual_key)
            self.ring[hash_val] = worker_id
        
        # Keep sorted list of hash values for binary search
        self.sorted_keys = sorted(self.ring.keys())
    
    def remove_worker(self, worker_id: str):
        """Remove a worker from the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_key = f"{worker_id}:vnode{i}"
            hash_val = self._hash(virtual_key)
            if hash_val in self.ring:
                del self.ring[hash_val]
        
        self.sorted_keys = sorted(self.ring.keys())
    
    def get_worker(self, key: str) -> Optional[str]:
        """Get the primary worker responsible for a key"""
        if not self.ring:
            return None
        
        key_hash = self._hash(key)
        
        # Find the first worker clockwise from the key's position
        for ring_hash in self.sorted_keys:
            if ring_hash >= key_hash:
                return self.ring[ring_hash]
        
        # Wrap around to the first worker
        return self.ring[self.sorted_keys[0]]
    
    def get_replicas(self, key: str, num_replicas: int) -> List[str]:
        """Get the list of workers for replicas (including primary)"""
        if not self.ring or num_replicas <= 0:
            return []
        
        key_hash = self._hash(key)
        replicas = []
        seen_workers = set()
        
        # Start from the key position and go clockwise
        start_idx = 0
        for i, ring_hash in enumerate(self.sorted_keys):
            if ring_hash >= key_hash:
                start_idx = i
                break
        
        # Collect unique workers
        idx = start_idx
        while len(replicas) < num_replicas and len(seen_workers) < len(set(self.ring.values())):
            worker_id = self.ring[self.sorted_keys[idx % len(self.sorted_keys)]]
            if worker_id not in seen_workers:
                replicas.append(worker_id)
                seen_workers.add(worker_id)
            idx += 1
        
        return replicas


class WorkerRegistry:
    """Manages worker information and health status"""
    
    def __init__(self, heartbeat_timeout: int = 15):
        self.workers = {}  # worker_id -> worker_info
        self.heartbeat_timeout = heartbeat_timeout
    
    def register_worker(self, worker_id: str, host: str, port: int):
        """Register a new worker"""
        self.workers[worker_id] = {
            'id': worker_id,
            'host': host,
            'port': port,
            'url': f"http://{host}:{port}",
            'status': 'active',
            'last_heartbeat': time.time(),
            'registered_at': time.time()
        }
    
    def update_heartbeat(self, worker_id: str) -> bool:
        """Update heartbeat timestamp for a worker"""
        if worker_id in self.workers:
            self.workers[worker_id]['last_heartbeat'] = time.time()
            if self.workers[worker_id]['status'] == 'failed':
                self.workers[worker_id]['status'] = 'active'
            return True
        return False
    
    def get_worker(self, worker_id: str) -> Optional[Dict]:
        """Get worker information"""
        return self.workers.get(worker_id)
    
    def get_all_workers(self) -> Dict:
        """Get all registered workers"""
        return self.workers
    
    def get_active_workers(self) -> List[str]:
        """Get list of active worker IDs"""
        return [wid for wid, info in self.workers.items() 
                if info['status'] == 'active']
    
    def check_failed_workers(self) -> List[str]:
        """Check for workers that haven't sent heartbeat within timeout"""
        current_time = time.time()
        failed = []
        
        for worker_id, info in self.workers.items():
            if info['status'] == 'active':
                time_since_heartbeat = current_time - info['last_heartbeat']
                if time_since_heartbeat > self.heartbeat_timeout:
                    info['status'] = 'failed'
                    info['failed_at'] = current_time
                    failed.append(worker_id)
        
        return failed
    
    def mark_worker_failed(self, worker_id: str):
        """Manually mark a worker as failed"""
        if worker_id in self.workers:
            self.workers[worker_id]['status'] = 'failed'
            self.workers[worker_id]['failed_at'] = time.time()
    
    def get_worker_url(self, worker_id: str) -> Optional[str]:
        """Get the URL for a worker"""
        worker = self.workers.get(worker_id)
        return worker['url'] if worker else None