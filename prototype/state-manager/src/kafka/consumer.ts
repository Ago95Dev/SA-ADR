import { Consumer, EachBatchPayload, Kafka } from 'kafkajs';
import { RedisStateManager } from '../state/redis-manager';
import { SnapshotManager } from '../state/snapshot-manager';
import { logger } from '../utils/logger';

/**
 * City-Simulator Sensor Message Types
 * These represent the data formats sent by the city-simulator edge managers
 */
interface SpeedSensorReading {
  sensor_id: string;
  speed_kmh: number;
  latitude: number;
  longitude: number;
}

interface SpeedSensorMessage {
  district_id: string;
  edge_id: string;
  sensor_type: 'speed';
  timestamp: string;
  latitude: number;
  longitude: number;
  speed_kmh: number;
  sensor_count: number;
  sensor_readings: SpeedSensorReading[];
  sample_count: number;
}

interface WeatherSensorReading {
  sensor_id: string;
  temperature_c: number;
  humidity: number;
  latitude: number;
  longitude: number;
}

interface WeatherSensorMessage {
  district_id: string;
  edge_id: string;
  sensor_type: 'weather';
  timestamp: string;
  latitude: number;
  longitude: number;
  temperature_c: number;
  humidity: number;
  weather_conditions: string;
  sensor_count: number;
  sensor_readings: WeatherSensorReading[];
  sample_count: number;
}

interface CameraSensorReading {
  sensor_id: string;
  road_condition: string;
  confidence: number;
  vehicle_count: number;
  latitude: number;
  longitude: number;
}

interface CameraSensorMessage {
  district_id: string;
  edge_id: string;
  sensor_type: 'camera';
  timestamp: string;
  latitude: number;
  longitude: number;
  road_condition: string;
  confidence: number;
  vehicle_count: number;
  sensor_count: number;
  sensor_readings: CameraSensorReading[];
}

// type CitySensorMessage = SpeedSensorMessage | WeatherSensorMessage | CameraSensorMessage;

/**
 * Buildings-Simulator Message Types
 */
interface AirQualitySensor {
  sensor_id: string;
  location: string;
  type: string;
  measurements: {
    pm25_ugm3?: number;
    pm10_ugm3?: number;
    no2_ugm3?: number;
    co_mgm3?: number;
    o3_ugm3?: number;
    voc_ugm3?: number;
    co2_ppm?: number;
  };
  last_reading: string;
  status: string;
}

interface AcousticSensor {
  sensor_id: string;
  location: string;
  type: string;
  measurements: {
    noise_level_db: number;
    peak_db?: number;
    average_db_1h?: number;
  };
  last_reading: string;
  status: string;
}

interface DisplaySensor {
  sensor_id: string;
  type: string;
  location: string;
  coordinates?: { latitude: number; longitude: number };
  current_message: string;
  operational: boolean;
  last_update: string;
}

interface EmergencyExit {
  exit_id: string;
  location: string;
  floor: number;
  status: string;
  operational: boolean;
  width_m: number;
  last_check: string;
}

interface Elevator {
  elevator_id: string;
  location: string;
  status: string;
  current_floor: number;
  capacity_persons: number;
  fault_description?: string;
}

interface BuildingMessage {
  building_id: string;
  building_name: string;
  building_type: string;
  timestamp: string;
  location: {
    latitude: number;
    longitude: number;
    altitude_m: number;
    address: string;
  };
  sensors: {
    air_quality: AirQualitySensor[];
    acoustic: AcousticSensor[];
    displays: DisplaySensor[];
  };
  managed_resources: {
    emergency_exits: EmergencyExit[];
    elevators: Elevator[];
  };
}

/**
 * Vehicles-Simulator Message Types
 */
interface VehicleMessage {
  vehicle_id: string;
  vehicle_type: string;
  timestamp: string;
  gps_position: {
    latitude: number;
    longitude: number;
    altitude_m: number;
  };
  movement: {
    speed_kmh: number;
    direction_degrees: number;
    heading: string;
  };
  managed_resources: {
    battery_level_percent: number;
    firmware_version: string;
  };
  sensors: {
    accelerometer: {
      sensor_id: string;
      incident_detected: boolean;
      threshold_g: number;
      last_reading_timestamp: string;
    };
  };
  route_planning: {
    current_destination?: {
      latitude: number;
      longitude: number;
      location_name: string;
    };
    predicted_destinations?: Array<{
      latitude: number;
      longitude: number;
      location_name: string;
      eta_minutes: number;
      probability: number;
    }>;
    route_priority: string;
  };
}

export class KafkaConsumerManager {
  private kafka: Kafka;
  private consumer: Consumer;
  private redisManager: RedisStateManager;
  private snapshotManager: SnapshotManager;
  private isRunning = false;

  // In-memory state cache for batching
  private stateCache = new Map<string, any>();
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private readonly FLUSH_INTERVAL_MS = 1000; // Flush every 1 second
  // private readonly _BATCH_SIZE = 1000; // Process 1000 messages per batch

  // Topics from environment variables (configured in k8s configmap)
  private readonly topics = [
    process.env.KAFKA_TOPIC_SPEED || 'city-speed-sensors',
    process.env.KAFKA_TOPIC_WEATHER || 'city-weather-sensors',
    process.env.KAFKA_TOPIC_CAMERA || 'city-camera-sensors',
    process.env.KAFKA_TOPIC_BUILDINGS || 'buildings-monitoring',
    process.env.KAFKA_TOPIC_VEHICLES || 'vehicles-telemetry',
  ];

  constructor(redisManager: RedisStateManager, snapshotManager: SnapshotManager) {
    this.redisManager = redisManager;
    this.snapshotManager = snapshotManager;

    const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
    const clientId = process.env.KAFKA_CLIENT_ID || 'state-manager';
    const groupId = process.env.KAFKA_GROUP_ID || 'state-manager-group';

    this.kafka = new Kafka({
      clientId,
      brokers,
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
    });

    this.consumer = this.kafka.consumer({ 
      groupId,
      maxBytesPerPartition: 1048576, // 1MB per partition
      sessionTimeout: 30000,
    });
  }

  async start(): Promise<void> {
    try {
      await this.consumer.connect();
      logger.info('Kafka consumer connected');

      for (const topic of this.topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
        logger.info(`Subscribed to topic: ${topic}`);
      }

      // Load initial state into cache
      await this.loadStateToCache();

      // Start periodic flush timer
      this.startFlushTimer();

      this.isRunning = true;

      // Use eachBatch for high-throughput processing
      await this.consumer.run({
        autoCommit: false, // Manual commit for better control
        partitionsConsumedConcurrently: 3, // Process 3 partitions in parallel
        eachBatch: async (payload: EachBatchPayload) => {
          await this.handleBatch(payload);
        },
      });

      logger.info('Kafka consumer started in batch mode');
    } catch (error) {
      logger.error('Error starting Kafka consumer:', error);
      throw error;
    }
  }

  /**
   * Load initial state from Redis into memory cache
   */
  private async loadStateToCache(): Promise<void> {
    try {
      const state = await this.redisManager.getCompleteState();
      
      // Cache districts
      for (const district of state.districts) {
        this.stateCache.set(`district:${district.districtId}`, district);
      }
      
      // Cache shared state
      this.stateCache.set('publicTransport', state.publicTransport);
      this.stateCache.set('emergencyServices', state.emergencyServices);
      this.stateCache.set('cityGraph', state.cityGraph);
      
      logger.info(`Loaded state to cache: ${this.stateCache.size} entries`);
    } catch (error) {
      logger.error('Error loading state to cache:', error);
    }
  }

  /**
   * Handle batch of messages with parallel processing per district
   */
  private async handleBatch(payload: EachBatchPayload): Promise<void> {
    const { batch, resolveOffset, heartbeat, commitOffsetsIfNecessary } = payload;
    
    try {
      const startTime = Date.now();
      
      // Group messages by district/category for sequential processing within group
      const messageGroups = this.groupMessages(batch.messages, batch.topic);
      
      // Process each group in parallel (safe because different districts)
      await Promise.all(
        Array.from(messageGroups.entries()).map(async ([groupKey, messages]) => {
          for (const message of messages) {
            try {
              const data = JSON.parse(message.value!.toString());
              await this.processMessageInMemory(batch.topic, data);
              
              // Resolve offset for each processed message
              resolveOffset(message.offset);
            } catch (error) {
              logger.error(`Error processing message in group ${groupKey}:`, error);
            }
          }
          
          // Send heartbeat periodically
          await heartbeat();
        })
      );
      
      // Commit offsets
      await commitOffsetsIfNecessary();
      
      const duration = Date.now() - startTime;
      const throughput = duration > 0 ? Math.round(batch.messages.length / (duration / 1000)) : batch.messages.length;
      
      // Only log if batch has more than 1 message or took significant time
      if (batch.messages.length > 1 || duration > 10) {
        logger.info(`Processed batch: ${batch.messages.length} messages in ${duration}ms (${throughput} msg/s)`);
      }
      
    } catch (error) {
      logger.error('Error handling batch:', error);
    }
  }

  /**
   * Group messages by district to avoid race conditions
   */
  private groupMessages(messages: any[], _topic: string): Map<string, any[]> {
    const groups = new Map<string, any[]>();
    
    for (const message of messages) {
      if (!message.value) continue;
      
      try {
        const data = JSON.parse(message.value.toString());
        const groupKey = data.districtId || 'shared'; // Shared for publicTransport, emergency
        
        if (!groups.has(groupKey)) {
          groups.set(groupKey, []);
        }
        groups.get(groupKey)!.push(message);
      } catch (error) {
        logger.error('Error parsing message:', error);
      }
    }
    
    return groups;
  }

  /**
   * Process message updates in memory cache (no Redis I/O)
   * Updated to handle city-simulator, buildings, and vehicles data formats
   */
  private async processMessageInMemory(topic: string, data: any): Promise<void> {
    const speedTopic = process.env.KAFKA_TOPIC_SPEED || 'city-speed-sensors';
    const weatherTopic = process.env.KAFKA_TOPIC_WEATHER || 'city-weather-sensors';
    const cameraTopic = process.env.KAFKA_TOPIC_CAMERA || 'city-camera-sensors';
    const buildingsTopic = process.env.KAFKA_TOPIC_BUILDINGS || 'buildings-monitoring';
    const vehiclesTopic = process.env.KAFKA_TOPIC_VEHICLES || 'vehicles-telemetry';

    switch (topic) {
      case speedTopic:
        this.updateSpeedSensorInCache(data as SpeedSensorMessage);
        break;

      case weatherTopic:
        this.updateWeatherSensorInCache(data as WeatherSensorMessage);
        break;

      case cameraTopic:
        this.updateCameraSensorInCache(data as CameraSensorMessage);
        break;

      case buildingsTopic:
        this.updateBuildingInCache(data as BuildingMessage);
        break;

      case vehiclesTopic:
        this.updateVehicleInCache(data as VehicleMessage);
        break;
    }
  }

  /**
   * Update speed sensor data in cache (from city-simulator)
   * Creates edge-level aggregated speed data
   */
  private updateSpeedSensorInCache(data: SpeedSensorMessage): void {
    const { district_id, edge_id, speed_kmh, sensor_count, sensor_readings, timestamp, latitude, longitude } = data;
    if (!district_id || !edge_id) return;

    const cacheKey = `district:${district_id}`;
    let district = this.stateCache.get(cacheKey);

    if (!district) {
      district = this.createEmptyDistrict(district_id, latitude, longitude);
      this.stateCache.set(cacheKey, district);
    }

    // Find or create edge sensor entry
    const sensorId = `speed-${edge_id}`;
    const sensorIndex = district.sensors.findIndex((s: any) => s.sensorId === sensorId);

    const sensorData = {
      sensorId,
      type: 'speed',
      edgeId: edge_id,
      value: speed_kmh,
      unit: 'km/h',
      status: 'active',
      lastUpdated: new Date(timestamp),
      location: { latitude, longitude },
      metadata: {
        avgSpeed: speed_kmh,
        sensorCount: sensor_count,
        readings: sensor_readings,
      },
    };

    if (sensorIndex !== -1) {
      district.sensors[sensorIndex] = sensorData;
    } else {
      district.sensors.push(sensorData);
    }
  }

  /**
   * Update weather sensor data in cache (from city-simulator)
   * Creates edge-level aggregated weather data
   */
  private updateWeatherSensorInCache(data: WeatherSensorMessage): void {
    const { district_id, edge_id, temperature_c, humidity, weather_conditions, sensor_count, sensor_readings, timestamp, latitude, longitude } = data;
    if (!district_id || !edge_id) return;

    const cacheKey = `district:${district_id}`;
    let district = this.stateCache.get(cacheKey);

    if (!district) {
      district = this.createEmptyDistrict(district_id, latitude, longitude);
      this.stateCache.set(cacheKey, district);
    }

    // Find or create weather station entry
    const stationId = `weather-${edge_id}`;
    const stationIndex = district.weatherStations.findIndex((ws: any) => ws.stationId === stationId);

    const stationData = {
      stationId,
      name: `Weather Station ${edge_id}`,
      edgeId: edge_id,
      location: { latitude, longitude, elevation: 0 },
      readings: {
        temperature: temperature_c,
        humidity,
        weatherConditions: weather_conditions,
        units: {
          temperature: '°C',
          humidity: '%',
        },
      },
      status: 'active',
      lastUpdated: new Date(timestamp),
      metadata: {
        sensorCount: sensor_count,
        readings: sensor_readings,
      },
    };

    if (stationIndex !== -1) {
      district.weatherStations[stationIndex] = stationData;
    } else {
      district.weatherStations.push(stationData);
    }
  }

  /**
   * Update camera sensor data in cache (from city-simulator)
   * Creates edge-level road condition analytics
   */
  private updateCameraSensorInCache(data: CameraSensorMessage): void {
    const { district_id, edge_id, road_condition, confidence, vehicle_count, sensor_count, sensor_readings, timestamp, latitude, longitude } = data;
    if (!district_id || !edge_id) return;

    const cacheKey = `district:${district_id}`;
    let district = this.stateCache.get(cacheKey);

    if (!district) {
      district = this.createEmptyDistrict(district_id, latitude, longitude);
      this.stateCache.set(cacheKey, district);
    }

    // Find or create camera sensor entry
    const sensorId = `camera-${edge_id}`;
    const sensorIndex = district.sensors.findIndex((s: any) => s.sensorId === sensorId);

    const sensorData = {
      sensorId,
      type: 'camera',
      edgeId: edge_id,
      value: vehicle_count,
      unit: 'vehicles',
      status: 'active',
      lastUpdated: new Date(timestamp),
      location: { latitude, longitude },
      metadata: {
        roadCondition: road_condition,
        confidence,
        vehicleCount: vehicle_count,
        sensorCount: sensor_count,
        readings: sensor_readings,
        congestionStatus: this.calculateCongestionStatus(road_condition),
      },
    };

    if (sensorIndex !== -1) {
      district.sensors[sensorIndex] = sensorData;
    } else {
      district.sensors.push(sensorData);
    }
  }

  /**
   * Create an empty district structure
   */
  private createEmptyDistrict(districtId: string, latitude: number, longitude: number): any {
    return {
      districtId,
      name: `District ${districtId}`,
      location: {
        centerLatitude: latitude,
        centerLongitude: longitude,
        boundaries: { north: 0, south: 0, east: 0, west: 0 },
      },
      sensors: [],
      buildings: [],
      weatherStations: [],
    };
  }

  /**
   * Update building data in cache (from buildings-simulator)
   * Stores comprehensive building monitoring data including sensors and managed resources
   */
  private updateBuildingInCache(data: BuildingMessage): void {
    const { building_id, building_name, building_type, timestamp, location, sensors, managed_resources } = data;
    if (!building_id) return;

    // Extract district from building_id or use default
    const districtId = this.extractDistrictFromBuildingId(building_id);
    const cacheKey = `district:${districtId}`;
    let district = this.stateCache.get(cacheKey);

    if (!district) {
      district = this.createEmptyDistrict(districtId, location.latitude, location.longitude);
      this.stateCache.set(cacheKey, district);
    }

    // Find or create building entry
    const buildingIndex = district.buildings.findIndex((b: any) => b.buildingId === building_id);

    const buildingData = {
      buildingId: building_id,
      name: building_name,
      type: building_type,
      location: {
        latitude: location.latitude,
        longitude: location.longitude,
        address: location.address,
        altitudeM: location.altitude_m,
      },
      status: 'operational',
      lastUpdated: new Date(timestamp),
      // Air quality sensors
      airQuality: sensors.air_quality.map(aq => ({
        sensorId: aq.sensor_id,
        location: aq.location,
        type: aq.type,
        measurements: {
          pm25: aq.measurements.pm25_ugm3,
          pm10: aq.measurements.pm10_ugm3,
          no2: aq.measurements.no2_ugm3,
          co: aq.measurements.co_mgm3,
          o3: aq.measurements.o3_ugm3,
          voc: aq.measurements.voc_ugm3,
          co2: aq.measurements.co2_ppm,
        },
        lastReading: aq.last_reading,
        status: aq.status,
      })),
      // Acoustic sensors
      acoustic: sensors.acoustic.map(ac => ({
        sensorId: ac.sensor_id,
        location: ac.location,
        type: ac.type,
        measurements: {
          noiseLevel: ac.measurements.noise_level_db,
          peakDb: ac.measurements.peak_db,
          averageDb1h: ac.measurements.average_db_1h,
        },
        lastReading: ac.last_reading,
        status: ac.status,
      })),
      // Display sensors
      displays: sensors.displays.map(d => ({
        sensorId: d.sensor_id,
        type: d.type,
        location: d.location,
        coordinates: d.coordinates,
        currentMessage: d.current_message,
        operational: d.operational,
        lastUpdate: d.last_update,
      })),
      // Managed resources
      managedResources: {
        emergencyExits: managed_resources.emergency_exits.map(exit => ({
          exitId: exit.exit_id,
          location: exit.location,
          floor: exit.floor,
          status: exit.status,
          operational: exit.operational,
          widthM: exit.width_m,
          lastCheck: exit.last_check,
        })),
        elevators: managed_resources.elevators.map(elev => ({
          elevatorId: elev.elevator_id,
          location: elev.location,
          status: elev.status,
          currentFloor: elev.current_floor,
          capacityPersons: elev.capacity_persons,
          faultDescription: elev.fault_description,
        })),
      },
    };

    if (buildingIndex !== -1) {
      district.buildings[buildingIndex] = buildingData;
    } else {
      district.buildings.push(buildingData);
    }
  }

  /**
   * Extract district ID from building ID
   */
  private extractDistrictFromBuildingId(buildingId: string): string {
    // Building IDs follow pattern: building-{type}-{number}
    // Map to districts based on type or use default
    if (buildingId.includes('hospital')) return 'district-centro';
    if (buildingId.includes('school')) return 'district-centro';
    if (buildingId.includes('university')) return 'district-centro';
    return 'district-centro'; // Default district
  }

  /**
   * Update vehicle data in cache (from vehicles-simulator)
   * Stores real-time vehicle telemetry including GPS, movement, and incident detection
   */
  private updateVehicleInCache(data: VehicleMessage): void {
    const { vehicle_id, vehicle_type, timestamp, gps_position, movement, managed_resources, sensors, route_planning } = data;
    if (!vehicle_id) return;

    // Store vehicles in a separate cache key
    const cacheKey = 'vehicles';
    let vehicles = this.stateCache.get(cacheKey) || [];

    // Find or create vehicle entry
    const vehicleIndex = vehicles.findIndex((v: any) => v.vehicleId === vehicle_id);

    const vehicleData = {
      vehicleId: vehicle_id,
      type: vehicle_type,
      lastUpdated: new Date(timestamp),
      gpsPosition: {
        latitude: gps_position.latitude,
        longitude: gps_position.longitude,
        altitudeM: gps_position.altitude_m,
      },
      movement: {
        speedKmh: movement.speed_kmh,
        directionDegrees: movement.direction_degrees,
        heading: movement.heading,
      },
      managedResources: {
        batteryLevelPercent: managed_resources.battery_level_percent,
        firmwareVersion: managed_resources.firmware_version,
      },
      sensors: {
        accelerometer: {
          sensorId: sensors.accelerometer.sensor_id,
          incidentDetected: sensors.accelerometer.incident_detected,
          thresholdG: sensors.accelerometer.threshold_g,
          lastReadingTimestamp: sensors.accelerometer.last_reading_timestamp,
        },
      },
      routePlanning: {
        currentDestination: route_planning.current_destination ? {
          latitude: route_planning.current_destination.latitude,
          longitude: route_planning.current_destination.longitude,
          locationName: route_planning.current_destination.location_name,
        } : null,
        predictedDestinations: (route_planning.predicted_destinations || []).map(dest => ({
          latitude: dest.latitude,
          longitude: dest.longitude,
          locationName: dest.location_name,
          etaMinutes: dest.eta_minutes,
          probability: dest.probability,
        })),
        routePriority: route_planning.route_priority,
      },
    };

    if (vehicleIndex !== -1) {
      vehicles[vehicleIndex] = vehicleData;
    } else {
      vehicles.push(vehicleData);
    }

    this.stateCache.set(cacheKey, vehicles);
  }

  /**
   * Calculate congestion status from road condition
   */
  private calculateCongestionStatus(roadCondition: string): string {
    const conditionMap: Record<string, string> = {
      'clear': 'free_flow',
      'congestion': 'congested',
      'accident': 'blocked',
      'obstacles': 'slow',
      'flooding': 'blocked',
    };
    return conditionMap[roadCondition] || 'unknown';
  }

  /**
   * Start periodic timer to flush cache to Redis
   */
  private startFlushTimer(): void {
    this.flushTimer = setInterval(async () => {
      await this.flushCacheToRedis();
    }, this.FLUSH_INTERVAL_MS);

    logger.info(`Started flush timer (interval: ${this.FLUSH_INTERVAL_MS}ms)`);
  }

  /**
   * Flush in-memory cache to Redis in bulk
   */
  private async flushCacheToRedis(): Promise<void> {
    if (this.stateCache.size === 0) return;

    const startTime = Date.now();

    try {
      // Use Redis pipeline for bulk writes
      const pipeline = this.redisManager.createPipeline();
      let operationCount = 0;

      for (const [key, value] of this.stateCache.entries()) {
        if (key.startsWith('district:')) {
          const districtId = key.replace('district:', '');
          pipeline.set(`district:${districtId}:state`, JSON.stringify(value));
          operationCount++;
        } else if (key === 'publicTransport') {
          pipeline.set('city:publicTransport', JSON.stringify(value));
          operationCount++;
        } else if (key === 'emergencyServices') {
          pipeline.set('city:emergencyServices', JSON.stringify(value));
          operationCount++;
        } else if (key === 'cityGraph') {
          pipeline.set('city:graph', JSON.stringify(value));
          operationCount++;
        } else if (key === 'vehicles') {
          pipeline.set('city:vehicles', JSON.stringify(value));
          operationCount++;
        }
      }

      await pipeline.exec();

      const duration = Date.now() - startTime;
      logger.info(`Flushed ${operationCount} keys to Redis in ${duration}ms`);

      // Trigger snapshot check
      await this.snapshotManager.incrementChangeCounter(
        () => this.redisManager.getCompleteState(),
        operationCount
      );

    } catch (error) {
      logger.error('Error flushing cache to Redis:', error);
    }
  }

  async stop(): Promise<void> {
    if (this.isRunning) {
      // Stop flush timer
      if (this.flushTimer) {
        clearInterval(this.flushTimer);
      }

      // Final flush
      await this.flushCacheToRedis();

      await this.consumer.disconnect();
      this.isRunning = false;
      logger.info('Kafka consumer stopped');
    }
  }

  isConnected(): boolean {
    return this.isRunning;
  }
}