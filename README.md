# Distributed Key-Value Store

A fault-tolerant distributed key-value store implementation with replication and automatic failure recovery.

## 🎯 Project Overview

This project implements a distributed key-value store system with:
- **1 Controller Node**: Manages key partitioning and worker health
- **4 Worker Nodes**: Store and replicate key-value pairs
- **Replication**: 3 replicas per key with quorum-based writes
- **Fault Tolerance**: Automatic failure detection and recovery
- **REST API**: Clean HTTP-based interface

## 🏗️ Architecture

### Components
- **Controller**: Coordinates the system, tracks worker health, manages key partitioning
- **Workers**: Store data, handle GET/PUT operations, maintain replicas
- **Client**: Interacts with the system through REST APIs

### Key Features
- ✅ Consistent hashing for key partitioning
- ✅ Synchronous replication (2/3 replicas)
- ✅ Asynchronous replication (3rd replica)
- ✅ Heartbeat-based failure detection
- ✅ Automatic re-replication on failures

## 📋 Requirements

- Python 3.8+
- Flask
- Requests library

## 🚀 Installation

1. Clone the repository:
```bash
git clone https://github.com/amanbh321/distributed-kv-store.git
cd distributed-kv-store
```

2. Create and activate virtual environment:
```bash
python3 -m venv cloudvenv
source cloudvenv/bin/activate  
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## 📖 Usage

*Coming soon - Implementation in progress*

## 📁 Project Structure
```
distributed-kv-store/
├── controller/          # Controller node implementation
├── worker/             # Worker node implementation
├── client/             # Client interface
├── tests/              # Test cases
├── docs/               # Documentation
│   ├── architecture.md
│   └── api_spec.md
├── config.py           # Configuration settings
└── requirements.txt    # Python dependencies
```

## 🔧 Configuration

See `config.py` for system configuration including:
- Number of workers
- Replication factor
- Heartbeat intervals
- Port assignments

## 📚 Documentation

- [Architecture Design](docs/architecture.md)
- [API Specification](docs/api_spec.md)

## 🎓 Course Project

This project is part of my Cloud Computing course, demonstrating:
- Distributed systems design
- Fault tolerance mechanisms
- REST API development
- Replication strategies

## 👤 Team members

*Aman Bahuguna*
*Soumik Pal*

---

**Status**: 🚧 In Development - Step 2 Complete (controller node implementation)