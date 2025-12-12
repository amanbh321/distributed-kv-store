#!/bin/bash
# start_all.sh - Start controller and all workers

echo "Starting Distributed Key-Value Store..."

# Activate virtual environment
source cloudvenv/bin/activate

# Start controller in background
echo "Starting Controller..."
python controller/controller.py > logs/controller.log 2>&1 &
CONTROLLER_PID=$!
sleep 2

# Start 4 workers
echo "Starting Workers..."
python worker/worker.py worker_1 6000 > logs/worker_1.log 2>&1 &
python worker/worker.py worker_2 6001 > logs/worker_2.log 2>&1 &
python worker/worker.py worker_3 6002 > logs/worker_3.log 2>&1 &
python worker/worker.py worker_4 6003 > logs/worker_4.log 2>&1 &

echo ""
echo "✓ All services started!"
echo ""
echo "Controller: http://localhost:5000"
echo "Worker 1:   http://localhost:6000"
echo "Worker 2:   http://localhost:6001"
echo "Worker 3:   http://localhost:6002"
echo "Worker 4:   http://localhost:6003"
echo ""
echo "To stop all: ./stop_all.sh"
echo "To view logs: tail -f logs/*.log"