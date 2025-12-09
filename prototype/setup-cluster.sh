#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting Digital Twin Kubernetes Setup...${NC}"

# Function to build docker image
build_image() {
    local name=$1
    local tag=$2
    local path=$3
    local args=$4

    echo -e "${BLUE}Building $name ($tag)...${NC}"
    if [ -z "$args" ]; then
        docker build -t $tag $path
    else
        docker build $args -t $tag $path
    fi
    echo -e "${GREEN}Successfully built $name${NC}"
}

# 1. Build Images
echo -e "\n${BLUE}--- Step 1: Building Docker Images ---${NC}"

# State Manager
build_image "State Manager" "digital-twin/state-manager:latest" "./state-manager" ""

# Notification Manager
build_image "Notification Manager" "digital-twin/notification-manager:latest" "./notification-manager" ""

# Data Producer
build_image "Data Producer" "digital-twin/data-producer:latest" "./producer" ""

# Dashboard
echo -e "${BLUE}Building Dashboard (digital-twin/dashboard:latest)...${NC}"
docker build \
  --build-arg VITE_STATE_MANAGER_API_URL=http://localhost:3000 \
  --build-arg VITE_STATE_MANAGER_WS_URL=ws://localhost:3001 \
  --build-arg VITE_NOTIFICATION_MANAGER_API_URL=http://localhost:3002/api \
  -t digital-twin/dashboard:latest ./dashboard
echo -e "${GREEN}Successfully built Dashboard${NC}"

# 2. Apply Manifests
echo -e "\n${BLUE}--- Step 2: Deploying to Kubernetes ---${NC}"
kubectl apply -f kubernetes/

# 3. Verify
echo -e "\n${BLUE}--- Step 3: Deployment Status ---${NC}"
echo "Waiting a few seconds for resources to be created..."
sleep 5
kubectl get pods
kubectl get services

echo -e "\n${GREEN}Setup Complete!${NC}"
echo -e "Access the Dashboard at: http://localhost:8080"
echo -e "Access the State Manager API at: http://localhost:3000"
echo -e "Access the Notification Manager API at: http://localhost:3002/api"
