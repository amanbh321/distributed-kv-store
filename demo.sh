#!/bin/bash
# demo.sh - Demonstration Script

echo "=========================================="
echo "DISTRIBUTED KEY-VALUE STORE DEMONSTRATION"
echo "=========================================="

# Activate virtual environment
source cloudvenv/bin/activate

echo ""
echo "Step 1: Starting all services..."
./start_all.sh
sleep 3

echo ""
echo "Step 2: Checking system status..."
echo "Controller status:"
curl -s http://localhost:5000/status | python -m json.tool

echo ""
echo "Step 3: Demonstrating PUT operations..."
echo ""
echo "PUT: name = Alice"
curl -s -X POST http://localhost:6000/put \
  -H "Content-Type: application/json" \
  -d '{"key": "name", "value": "Alice"}' | python -m json.tool

echo ""
echo "PUT: age = 25"
curl -s -X POST http://localhost:6001/put \
  -H "Content-Type: application/json" \
  -d '{"key": "age", "value": "25"}' | python -m json.tool

echo ""
echo "PUT: city = Bangalore"
curl -s -X POST http://localhost:6002/put \
  -H "Content-Type: application/json" \
  -d '{"key": "city", "value": "Bangalore"}' | python -m json.tool

sleep 2

echo ""
echo "Step 4: Demonstrating GET operations..."
echo ""
echo "GET: name"
curl -s "http://localhost:6000/get?key=name" | python -m json.tool

echo ""
echo "GET: age"
curl -s "http://localhost:6001/get?key=age" | python -m json.tool

echo ""
echo "GET: city"
curl -s "http://localhost:6002/get?key=city" | python -m json.tool

echo ""
echo "Step 5: Demonstrating replication..."
echo "Querying controller for key location:"
curl -s "http://localhost:5000/query?key=name" | python -m json.tool

echo ""
echo "Step 6: Checking all workers..."
curl -s http://localhost:5000/workers | python -m json.tool

echo ""
echo "=========================================="
echo "✓ DEMONSTRATION COMPLETE"
echo "=========================================="
echo ""
echo "Next: Run 'python tests/test_system.py' for full tests"
echo "Or: Run 'python client/client.py' for interactive mode"