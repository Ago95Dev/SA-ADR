#!/usr/bin/env python3
"""
City Simulator - Emergency Management System

Main entry point for the L'Aquila Emergency Management System simulator.

Architecture:
- Loads city configuration from JSON (districts, edge ranges)
- Dynamically generates gateways and sensors based on edge IDs (E-00000 to E-03458)
- Creates EdgeManager (Gateway) for each assigned edge
- Each EdgeManager runs in a separate thread
- Supports horizontal scaling via INSTANCE_ID and TOTAL_INSTANCES env vars

Horizontal Scaling:
- INSTANCE_ID: Current instance (0, 1, 2, ..., N-1)
- TOTAL_INSTANCES: Total number of instances
- Each instance handles a partition of the total edge range

This simulator demonstrates:
- Edge computing with local processing
- Concurrent operations with threading
- Resilient message handling with buffering
- Dynamic configuration and horizontal scaling
"""

import json
import logging
import os
import random
import sys
import threading
import time
from typing import Any, Dict, List, Tuple

# Add parent directory to path to import common module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import common utilities
from common.kafka_utils import create_kafka_producer
# Import our modular components
from edge_manager import EdgeManager

# Configure logging with clear formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPICS = {
    'gateway': os.getenv('KAFKA_TOPIC_GATEWAY', 'city-gateway-data')
}
CITY_CONFIG_FILE = os.getenv('CITY_CONFIG_FILE', 
                              os.path.join(os.path.dirname(__file__), 'config', 'city_config.json'))

# Horizontal scaling configuration
INSTANCE_ID = int(os.getenv('INSTANCE_ID', '0'))
TOTAL_INSTANCES = int(os.getenv('TOTAL_INSTANCES', '1'))


def load_city_config() -> Dict[str, Any]:
    """
    Load city configuration from JSON file.
    
    The configuration includes:
    - City metadata (name, location)
    - Graph configuration (edge ranges)
    - Simulation parameters (sensors per gateway)
    - Districts with their edge ranges
    
    Returns:
        Dict with complete city configuration
        
    Raises:
        FileNotFoundError: If config file not found
        json.JSONDecodeError: If JSON is malformed
    """
    try:
        with open(CITY_CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"✓ Loaded city configuration: {config['city']['name']}")
        return config
    except FileNotFoundError:
        logger.error(f"✗ City configuration file not found: {CITY_CONFIG_FILE}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"✗ Invalid JSON in city configuration file: {e}")
        raise


def generate_edge_id(index: int) -> str:
    """Generate edge ID in format E-XXXXX."""
    return f"E-{index:05d}"


def generate_gateway_id(edge_id: str) -> str:
    """Generate gateway ID from edge ID."""
    return f"GW-{edge_id}"


def find_district_for_edge(districts: List[Dict], edge_index: int) -> Dict:
    """
    Find which district an edge belongs to based on edge ranges.
    
    Args:
        districts: List of district configurations
        edge_index: Edge index (0-3458)
        
    Returns:
        District configuration dict
    """
    for district in districts:
        edge_range = district.get('edge_range', {})
        start = edge_range.get('start', 0)
        end = edge_range.get('end', 0)
        if start <= edge_index <= end:
            return district
    # Default to first district if not found
    return districts[0] if districts else None


def generate_sensor_configs(edge_id: str, sensors_per_gateway: Dict[str, int]) -> Dict[str, List[Dict]]:
    """
    Dynamically generate sensor configurations for a gateway.
    
    Args:
        edge_id: The edge ID (E-XXXXX)
        sensors_per_gateway: Dict with count per sensor type {'speed': 2, 'weather': 1, 'camera': 2}
        
    Returns:
        Dict mapping sensor types to list of sensor configs
    """
    sensors = {}
    
    for sensor_type, count in sensors_per_gateway.items():
        sensors[sensor_type] = []
        for i in range(count):
            sensor_id = f"{sensor_type}-{edge_id}-{chr(97 + i)}"  # e.g., speed-E-00000-a
            
            # Generate small random offsets for sensor positions
            offset_lat = random.uniform(-0.0002, 0.0002)
            offset_lon = random.uniform(-0.0002, 0.0002)
            
            sensors[sensor_type].append({
                'sensor_id': sensor_id,
                'location': f"{sensor_type.capitalize()} sensor {i + 1}",
                'offset_lat': round(offset_lat, 6),
                'offset_lon': round(offset_lon, 6)
            })
    
    return sensors


def calculate_instance_edge_range(
    total_edges: int, 
    instance_id: int, 
    total_instances: int
) -> Tuple[int, int]:
    """
    Calculate the edge range for this instance.
    
    Divides the total edges evenly across instances.
    
    Args:
        total_edges: Total number of edges (e.g., 3459)
        instance_id: This instance's ID (0-indexed)
        total_instances: Total number of instances
        
    Returns:
        Tuple of (start_index, end_index) inclusive
    """
    edges_per_instance = total_edges // total_instances
    remainder = total_edges % total_instances
    
    # Distribute remainder across first instances
    if instance_id < remainder:
        start = instance_id * (edges_per_instance + 1)
        end = start + edges_per_instance
    else:
        start = instance_id * edges_per_instance + remainder
        end = start + edges_per_instance - 1
    
    return start, end


class CitySimulator:
    """
    Main City Simulator with Horizontal Scaling Support
    
    Coordinates all edge managers and handles system lifecycle.
    Supports splitting workload across multiple instances.
    
    Responsibilities:
    - Load configuration
    - Initialize Kafka connection
    - Dynamically generate gateways based on edge ranges
    - Create and start EdgeManagers
    - Handle graceful shutdown
    """
    
    def __init__(self):
        """Initialize the city simulator."""
        # Load configuration
        self.city_config = load_city_config()
        self.city_name = self.city_config['city']['name']
        
        # Get configuration parameters
        self.graph_config = self.city_config.get('graph', {})
        self.simulation_config = self.city_config.get('simulation', {})
        managed_resources = self.city_config.get('managed_resources', {})
        self.monitor_interval = managed_resources.get('monitor_interval_seconds', 3)
        
        # Get sensors per gateway configuration
        self.sensors_per_gateway = self.simulation_config.get('sensors_per_gateway', {
            'speed': 2,
            'weather': 1,
            'camera': 2
        })
        
        # Calculate this instance's edge range
        total_edges = self.graph_config.get('total_edges', 3459)
        self.edge_start, self.edge_end = calculate_instance_edge_range(
            total_edges, INSTANCE_ID, TOTAL_INSTANCES
        )
        
        # Initialize Kafka producer (shared across all edges)
        self.kafka_producer = self._init_kafka_producer()
        
        # Threading control
        self.stop_event = threading.Event()
        self.edge_managers = []
        self.edge_threads = []
        
        # Initialize edge managers dynamically
        self._initialize_edges()
        
        logger.info(f"✓ CitySimulator initialized for {self.city_name}")
        logger.info(f"  Instance: {INSTANCE_ID + 1} of {TOTAL_INSTANCES}")
        logger.info(f"  Edge range: E-{self.edge_start:05d} to E-{self.edge_end:05d}")
        logger.info(f"  Total gateways: {len(self.edge_managers)}")
    
    def _init_kafka_producer(self):
        """
        Initialize Kafka producer using common utilities.
        
        The producer is shared across all edge managers (thread-safe).
        Uses the shared kafka_utils module for consistent connection handling.
        
        Returns:
            Connected KafkaProducer instance
            
        Raises:
            Exception: If connection fails after all retries
        """
        producer = create_kafka_producer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            max_retries=10,
            retry_delay=5
        )
        
        logger.info(f"✓ City simulator Kafka producer ready (topics: {list(KAFKA_TOPICS.values())})")
        return producer
    
    def _initialize_edges(self):
        """
        Initialize EdgeManager for each edge in this instance's range.
        
        Dynamically generates gateway configurations based on edge IDs.
        Each edge gets one gateway with configured sensors.
        """
        districts = self.city_config.get('districts', [])
        
        for edge_index in range(self.edge_start, self.edge_end + 1):
            # Generate IDs
            edge_id = generate_edge_id(edge_index)
            gateway_id = generate_gateway_id(edge_id)
            
            # Find district for this edge
            district = find_district_for_edge(districts, edge_index)
            if not district:
                continue
            
            district_id = district['district_id']
            district_center = district.get('center', {'latitude': 42.35, 'longitude': 13.40})
            
            # Generate location with small random offset from district center
            location = {
                'latitude': district_center['latitude'] + random.uniform(-0.02, 0.02),
                'longitude': district_center['longitude'] + random.uniform(-0.02, 0.02)
            }
            
            # Generate sensor configurations
            sensors = generate_sensor_configs(edge_id, self.sensors_per_gateway)
            
            # Build edge configuration
            edge_config = {
                'gateway_id': gateway_id,
                'edge_id': edge_id,
                'name': f"Gateway at {edge_id}",
                'location': location,
                'sensors': sensors
            }
            
            # Create EdgeManager for this edge
            edge_manager = EdgeManager(
                district_id=district_id,
                edge_config=edge_config,
                kafka_producer=self.kafka_producer,
                kafka_topics=KAFKA_TOPICS,
                stop_event=self.stop_event
            )
            
            self.edge_managers.append(edge_manager)
        
        logger.info(f"  Initialized {len(self.edge_managers)} edge managers")
    
    def start_edges(self):
        """
        Start all edge managers in separate threads.
        
        Each edge runs independently in its own thread, allowing
        concurrent sensor data generation and transmission.
        """
        logger.info("Starting all edge managers...")
        
        for edge_manager in self.edge_managers:
            # Create thread for this edge
            thread = threading.Thread(
                target=edge_manager.run,
                name=f"GW-{edge_manager.gateway_id}",
                daemon=True  # Thread will stop when main program exits
            )
            thread.start()
            self.edge_threads.append(thread)
        
        logger.info(f"✓ Started {len(self.edge_threads)} edge threads")
    
    def run(self):
        """
        Run the city simulator.
        
        Main execution flow:
        1. Display startup information
        2. Start all edge manager threads
        3. Wait until interrupted
        4. Perform graceful shutdown
        """
        # Display startup banner
        logger.info("=" * 70)
        logger.info(f"🏙️  {self.city_name} Emergency Management System")
        logger.info("=" * 70)
        logger.info(f"📊 Configuration:")
        logger.info(f"   Instance: {INSTANCE_ID + 1} of {TOTAL_INSTANCES}")
        logger.info(f"   Edge Range: E-{self.edge_start:05d} to E-{self.edge_end:05d}")
        logger.info(f"   Gateways: {len(self.edge_managers)}")
        logger.info(f"   Sensors per Gateway: {self.sensors_per_gateway}")
        logger.info(f"   Kafka Topics: {KAFKA_TOPICS}")
        logger.info("=" * 70)
        
        try:
            # Start all edge managers
            self.start_edges()
            
            # Keep main thread alive
            logger.info("✓ System running. Press Ctrl+C to stop.")
            
            while not self.stop_event.is_set():
                time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("\n⚠ Shutdown requested...")
        finally:
            self.stop()
    
    def stop(self):
        """
        Stop all edge managers and cleanup resources.
        
        Performs graceful shutdown:
        1. Signal all threads to stop
        2. Wait for threads to finish
        3. Close Kafka connection
        """
        logger.info("Stopping all edge managers...")
        
        # Signal all threads to stop
        self.stop_event.set()
        
        # Wait for threads to finish (with timeout)
        for thread in self.edge_threads:
            thread.join(timeout=5)
        
        # Close Kafka producer
        self.kafka_producer.close()
        
        logger.info("✓ City simulator stopped cleanly")


if __name__ == '__main__':
    """
    Main entry point.
    
    Creates and runs the CitySimulator.
    Handles any startup errors gracefully.
    """
    try:
        simulator = CitySimulator()
        simulator.run()
    except Exception as e:
        logger.error(f"✗ Fatal error: {e}")
        sys.exit(1)
