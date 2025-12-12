#!/bin/bash
# stop_all.sh - Stop all services

echo "Stopping all services..."

# Kill all Python processes running our scripts
pkill -f "controller.py"
pkill -f "worker.py"

echo "✓ All services stopped"