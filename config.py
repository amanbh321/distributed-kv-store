# config.py - Central configuration file

# Controller configuration
CONTROLLER_HOST = 'localhost'
CONTROLLER_PORT = 5000

# Worker configuration
WORKER_BASE_PORT = 6000  # Workers will use ports 6000, 6001, 6002, 6003
NUM_WORKERS = 4
REPLICATION_FACTOR = 3  # Total replicas per key
SYNC_REPLICAS = 2       # Replicas needed for PUT success

# Heartbeat configuration
HEARTBEAT_INTERVAL = 5  # seconds
HEARTBEAT_TIMEOUT = 15  # seconds - consider worker dead after this

# Partitioning configuration
PARTITION_METHOD = 'hash'  # or 'range'
VIRTUAL_NODES = 150  # For consistent hashing

# API endpoints
CONTROLLER_QUERY_ENDPOINT = '/query'
CONTROLLER_REGISTER_ENDPOINT = '/register'
CONTROLLER_HEARTBEAT_ENDPOINT = '/heartbeat'
WORKER_GET_ENDPOINT = '/get'
WORKER_PUT_ENDPOINT = '/put'
WORKER_REPLICATE_ENDPOINT = '/replicate'