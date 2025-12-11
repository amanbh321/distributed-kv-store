# REST API Specification

## Controller APIs

### 1. Query Key Location
**Endpoint:** `GET /query?key=<key>`  
**Description:** Returns the primary worker for a given key  
**Response:**
```json
{
  "key": "mykey",
  "primary_worker": "http://localhost:6001",
  "replicas": ["http://localhost:6002", "http://localhost:6003"]
}
```

### 2. Register Worker
**Endpoint:** `POST /register`  
**Body:**
```json
{
  "worker_id": "worker_1",
  "host": "localhost",
  "port": 6000
}
```

### 3. Heartbeat
**Endpoint:** `POST /heartbeat`  
**Body:**
```json
{
  "worker_id": "worker_1",
  "timestamp": 1234567890
}
```

## Worker APIs

### 1. GET Operation
**Endpoint:** `GET /get?key=<key>`  
**Response:**
```json
{
  "key": "mykey",
  "value": "myvalue",
  "success": true
}
```

### 2. PUT Operation
**Endpoint:** `POST /put`  
**Body:**
```json
{
  "key": "mykey",
  "value": "myvalue"
}
```
**Response:**
```json
{
  "success": true,
  "replicas_written": 2
}
```

### 3. Replicate Operation (Internal)
**Endpoint:** `POST /replicate`  
**Body:**
```json
{
  "key": "mykey",
  "value": "myvalue"
}
```