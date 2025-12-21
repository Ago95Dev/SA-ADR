"""
Edge Manager Module (Gateway)

This module manages a single gateway location with its sensors.
Each gateway runs in a separate thread, simulating concurrent operations
at different physical locations in the city.

A gateway represents a specific location (e.g., a street intersection) that
hosts multiple sensors of different types. The gateway aggregates all sensor
data and sends it to Kafka as a unified payload.

Edge ID vs Gateway ID:
- edge_id: Property of individual sensors, refers to the graph edge (E-00000 to E-03458) 
  where each sensor is physically located. Sensors inherit this from their config.
- gateway_id: Unique identifier for the gateway device itself. The gateway is the 
  data collector that aggregates and forwards sensor data.
"""

import logging
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

from sensor_simulator import (CameraSensorSimulator, SpeedSensorSimulator,
                              WeatherSensorSimulator)

logger = logging.getLogger(__name__)

# Gateway metadata
GATEWAY_VERSION = "1.0.0"
GATEWAY_FIRMWARE = "EdgeOS 2.1.3"


class EdgeManager:
    """
    Manages a single gateway location with multiple sensors.
    
    This class acts as a Gateway that gathers sensor data from all sensors
    at a specific location and sends it to Kafka as a unified payload.
    
    Responsibilities:
    - Coordinate data generation from all sensors at this gateway
    - Aggregate all sensor data into a single gateway payload
    - Send gateway payload to Kafka message bus
    - Handle local buffering for resilience
    - Run in separate thread for concurrent operation
    
    Thread Safety:
    - Each EdgeManager has its own thread
    - Shares only the Kafka producer (which is thread-safe)
    - No shared state between gateways
    """
    
    def __init__(self, district_id: str, edge_config: Dict, 
                 kafka_producer: Any, kafka_topics: Dict[str, str], 
                 stop_event: threading.Event):
        """
        Initialize a Gateway Manager.
        
        Args:
            district_id: ID of the district this gateway belongs to
            edge_config: Configuration dict from city_config.json
            kafka_producer: Shared Kafka producer instance (thread-safe)
            kafka_topics: Dictionary mapping sensor types to topic names
            stop_event: Threading event to signal shutdown
        """
        # Gateway identification
        self.district_id = district_id
        self.gateway_id = edge_config.get('gateway_id', edge_config['edge_id'])  # Use gateway_id if available
        self.gateway_name = edge_config['name']
        self.location = edge_config['location']
        self.sensors_config = edge_config.get('sensors', {})
        
        # Edge ID for sensors - sensors inherit this to identify their graph edge location
        # This is passed to sensors but NOT included in the gateway payload
        self._sensors_edge_id = edge_config['edge_id']
        
        # Kafka configuration
        self.kafka_producer = kafka_producer
        self.kafka_topics = kafka_topics
        self.stop_event = stop_event
        
        # Initialize sensor simulators
        # Each simulator maintains its own state (windows, persistence, etc.)
        self.speed_simulator = SpeedSensorSimulator(self.location)
        self.weather_simulator = WeatherSensorSimulator(self.location)
        self.camera_simulator = CameraSensorSimulator(self.location)
        
        # Local buffer for resilience
        # If Kafka is temporarily unavailable, messages are buffered here
        self.local_buffer = deque(maxlen=1000)
        
        # Randomized sampling interval to avoid synchronization
        # (prevents all gateways from sending data at exactly the same time)
        import random
        self.sampling_interval = random.uniform(2.5, 4.5)
        
        # Log initialization
        speed_count = len(self.sensors_config.get('speed', []))
        weather_count = len(self.sensors_config.get('weather', []))
        camera_count = len(self.sensors_config.get('camera', []))
        
        logger.info(
            f"Gateway initialized: {district_id}/{self.gateway_id} (name: {self.gateway_name}) - "
            f"Sensors: {speed_count} speed, {weather_count} weather, {camera_count} camera"
        )
    
    def generate_sensor_data(self, sensor_type: str) -> Optional[Dict[str, Any]]:
        """
        Generate data for a specific sensor type.
        
        This method coordinates with the appropriate sensor simulator
        and returns the sensor-specific data with gateway_id attached.
        
        Args:
            sensor_type: One of 'speed', 'weather', 'camera'
            
        Returns:
            Sensor data dict with gateway_id, or None if no sensors
        """
        # Get sensor-specific data from appropriate simulator
        if sensor_type == 'speed':
            sensor_data = self.speed_simulator.generate_data(
                self.sensors_config.get('speed', []),
                self.gateway_id,
                self._sensors_edge_id
            )
        elif sensor_type == 'weather':
            sensor_data = self.weather_simulator.generate_data(
                self.sensors_config.get('weather', []),
                self.gateway_id,
                self._sensors_edge_id
            )
        elif sensor_type == 'camera':
            sensor_data = self.camera_simulator.generate_data(
                self.sensors_config.get('camera', []),
                self.gateway_id,
                self._sensors_edge_id
            )
        else:
            return None
        
        return sensor_data
    
    def generate_gateway_payload(self) -> Dict[str, Any]:
        """
        Generate the unified gateway payload containing all sensor data.
        
        This method aggregates data from all sensors at this gateway
        and returns a single payload ready for Kafka.
        
        Returns:
            Complete gateway payload with metadata and sensors array
        """
        # Collect sensor data from all sensor types
        sensors = []
        
        # Generate speed sensor data
        speed_data = self.generate_sensor_data('speed')
        if speed_data:
            sensors.extend(speed_data.get('readings', []))
        
        # Generate weather sensor data
        weather_data = self.generate_sensor_data('weather')
        if weather_data:
            sensors.extend(weather_data.get('readings', []))
        
        # Generate camera sensor data
        camera_data = self.generate_sensor_data('camera')
        if camera_data:
            sensors.extend(camera_data.get('readings', []))
        
        # Build the gateway payload
        # Note: edge_id is NOT included at gateway level - it's a property of individual sensors
        payload = {
            'gateway_id': self.gateway_id,
            'district_id': self.district_id,
            'location': {
                'latitude': self.location['latitude'],
                'longitude': self.location['longitude']
            },
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'metadata': {
                'name': self.gateway_name,
                'version': GATEWAY_VERSION,
                'firmware': GATEWAY_FIRMWARE,
                'sensor_counts': {
                    'speed': len(self.sensors_config.get('speed', [])),
                    'weather': len(self.sensors_config.get('weather', [])),
                    'camera': len(self.sensors_config.get('camera', []))
                }
            },
            'sensors': sensors
        }
        
        return payload
    
    def send_to_kafka(self, data: Dict[str, Any]) -> bool:
        """
        Send gateway data to Kafka with error handling.
        
        Implements resilience pattern:
        1. Try to send to Kafka
        2. On failure, buffer locally
        3. Retry buffered messages later
        
        Args:
            data: Gateway payload to send
            
        Returns:
            True if sent successfully, False if buffered
        """
        try:
            # Use the gateway topic for all gateway data
            topic = self.kafka_topics.get('gateway', 'city-gateway-data')
            
            logger.info(f"[{self.gateway_id}] 📤 Sending gateway data to {topic}...")
            
            # Async send with timeout and partition key
            # Using gateway_id as key ensures all data for this gateway goes to same partition
            future = self.kafka_producer.send(
                topic, 
                key=self.gateway_id,  # Partition key
                value=data
            )
            future.get(timeout=10)  # Wait max 10 seconds
            sensor_count = len(data.get('sensors', []))
            logger.info(f"[{self.gateway_id}] ✓ Sent gateway data ({sensor_count} sensors) to {topic}")
            return True
        except Exception as e:
            # Log error and buffer message for retry
            logger.error(f"[{self.gateway_id}] Kafka error: {e}. Buffering message.")
            self.local_buffer.append(data)
            return False
    
    def retry_buffered_messages(self):
        """
        Attempt to send buffered messages.
        
        Called periodically when Kafka might be available again.
        Implements simple retry logic without sophisticated backoff.
        """
        if not self.local_buffer:
            return
        
        logger.info(f"[{self.gateway_id}] Retrying {len(self.local_buffer)} buffered messages")
        
        # Try to send all buffered messages
        messages_to_retry = list(self.local_buffer)
        self.local_buffer.clear()
        
        for message in messages_to_retry:
            if not self.send_to_kafka(message):
                # Still failing, will be re-buffered
                break
    
    def run(self):
        """
        Main loop for this gateway: generate and send gateway data.
        
        Execution Flow:
        1. Generate unified gateway payload with all sensors
        2. Send to Kafka
        3. Wait for next interval
        4. Repeat until stop signal
        
        This method runs in a separate thread, one per gateway.
        """
        logger.info(f"[{self.gateway_id}] Starting gateway loop (interval: {self.sampling_interval:.1f}s)")
        
        iteration = 0
        while not self.stop_event.is_set():
            try:
                iteration += 1
                
                logger.info(f"[{self.gateway_id}] Iteration {iteration}: Generating gateway payload")
                
                # Generate unified gateway payload with all sensor data
                payload = self.generate_gateway_payload()
                
                # Send gateway payload to Kafka
                if payload.get('sensors'):
                    logger.info(f"[{self.gateway_id}] Payload generated with {len(payload['sensors'])} sensors, sending to Kafka...")
                    self.send_to_kafka(payload)
                else:
                    logger.info(f"[{self.gateway_id}] No sensors configured, skipping")
                
                # Periodically retry buffered messages
                if iteration % 10 == 0:
                    self.retry_buffered_messages()
                
                # Wait for next interval (interruptible sleep)
                self.stop_event.wait(timeout=self.sampling_interval)
                
            except Exception as e:
                # Log error but keep running (fault tolerance)
                logger.error(f"[{self.gateway_id}] Error in gateway loop: {e}")
                self.stop_event.wait(timeout=self.sampling_interval)
        
        logger.info(f"[{self.gateway_id}] Gateway loop stopped")
