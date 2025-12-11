# Distributed Key-Value Store Architecture

## System Components

### 1. Controller Node (Port 5000)
- Manages key partitioning and worker registry
- Monitors worker health via heartbeats
- Handles failure detection and recovery
- Provides key-to-worker mapping to clients

### 2. Worker Nodes (Ports 6000-6003)
- Store key-value pairs
- Handle GET/PUT operations
- Maintain replicas (primary + backups)
- Send periodic heartbeats to controller

### 3. Client
- Queries controller for key location
- Performs GET/PUT operations on workers
- Handles retries and errors

## Data Flow

### PUT Operation:
1. Client → Controller: "Where is key X?"
2. Controller → Client: "Primary worker is W1"
3. Client → W1: PUT(key, value)
4. W1 → W2, W3: Replicate synchronously (waits for 2/3)
5. W1 → Client: Success (after 2 replicas)
6. W1 → W4: Replicate asynchronously (background)

### GET Operation:
1. Client → Controller: "Where is key X?"
2. Controller → Client: "Primary worker is W1"
3. Client → W1: GET(key)
4. W1 → Client: Return value

## Key Partitioning Strategy
- Use consistent hashing (hash(key) mod NUM_WORKERS)
- Each worker responsible for a range of hash values
- Evenly distributes keys across workers

## Replication Strategy
- 3 total replicas per key
- Primary replica: responsible for handling requests
- 2 sync replicas: written before acknowledging PUT
- 1 async replica: written in background

## Failure Handling
- Heartbeat interval: 5 seconds
- Timeout: 15 seconds (3 missed heartbeats)
- On failure: Controller re-replicates from remaining replicas