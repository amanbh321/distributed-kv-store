import requests
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CONTROLLER_HOST, CONTROLLER_PORT

class KVStoreClient:
    def __init__(self):
        self.controller_url = f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}"
    
    def put(self, key, value):
        """PUT operation"""
        try:
            # Step 1: Query controller for key location
            response = requests.get(f"{self.controller_url}/query?key={key}")
            if response.status_code != 200:
                print(f"✗ Failed to query controller: {response.status_code}")
                return False
            
            data = response.json()
            primary_worker = data['primary_worker']
            
            print(f"→ Primary worker for '{key}': {primary_worker}")
            
            # Step 2: PUT to primary worker
            response = requests.post(
                f"{primary_worker}/put",
                json={'key': key, 'value': value},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✓ PUT successful: {key} = {value}")
                print(f"  Replicas written: {result.get('replicas_written', 0)}")
                return True
            else:
                print(f"✗ PUT failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            return False
    
    def get(self, key):
        """GET operation"""
        try:
            # Step 1: Query controller for key location
            response = requests.get(f"{self.controller_url}/query?key={key}")
            if response.status_code != 200:
                print(f"✗ Failed to query controller: {response.status_code}")
                return None
            
            data = response.json()
            primary_worker = data['primary_worker']
            
            print(f"→ Primary worker for '{key}': {primary_worker}")
            
            # Step 2: GET from primary worker
            response = requests.get(f"{primary_worker}/get?key={key}")
            
            if response.status_code == 200:
                result = response.json()
                value = result['value']
                print(f"✓ GET successful: {key} = {value}")
                return value
            elif response.status_code == 404:
                print(f"✗ Key not found: {key}")
                return None
            else:
                print(f"✗ GET failed: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            return None


def interactive_mode():
    """Interactive command-line interface"""
    client = KVStoreClient()
    
    print("=" * 60)
    print("📦 Distributed Key-Value Store Client")
    print("=" * 60)
    print("Commands:")
    print("  put <key> <value>  - Store a key-value pair")
    print("  get <key>          - Retrieve a value")
    print("  exit               - Exit client")
    print("=" * 60)
    
    while True:
        try:
            command = input("\n> ").strip()
            
            if not command:
                continue
            
            parts = command.split(maxsplit=2)
            cmd = parts[0].lower()
            
            if cmd == 'exit':
                print("Goodbye!")
                break
            
            elif cmd == 'put':
                if len(parts) < 3:
                    print("Usage: put <key> <value>")
                    continue
                key = parts[1]
                value = parts[2]
                client.put(key, value)
            
            elif cmd == 'get':
                if len(parts) < 2:
                    print("Usage: get <key>")
                    continue
                key = parts[1]
                client.get(key)
            
            else:
                print(f"Unknown command: {cmd}")
        
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {str(e)}")


if __name__ == '__main__':
    interactive_mode()