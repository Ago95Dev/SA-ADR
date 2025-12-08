import { useEffect } from 'react';
import { ErrorMessage, LoadingSpinner } from '../components/common';
import { Layout } from '../components/layout';
import {
  BuildingMarkers,
  BusMarkers,
  Map,
  RoadSegmentLayer,
  SensorMarkers,
  WeatherStationMarkers
} from '../components/map';
import { useNotificationPolling } from '../services/useNotificationPolling';
import { webSocketService } from '../services/webSocketService';
import { useCityStateStore, useWebSocketStore } from '../stores';

export function DashboardPage() {
  const { cityState, isLoading, error, loadInitialState, applyIncrementalUpdate } =
    useCityStateStore();
  const { setConnectionState } = useWebSocketStore();

  // Start notification polling
  useNotificationPolling();

  // Load initial state on mount
  useEffect(() => {
    loadInitialState();
  }, [loadInitialState]);

  // Set up WebSocket connection
  useEffect(() => {
    // Connect to WebSocket
    webSocketService.connect();

    // Subscribe to connection state changes
    const unsubscribeConnectionState = webSocketService.onConnectionStateChange(
      (state, error) => {
        setConnectionState(state, error);
      }
    );

    // Subscribe to incremental state updates
    const unsubscribeStateUpdate = webSocketService.onStateUpdate((update) => {
      applyIncrementalUpdate(update.patch);
    });

    // Cleanup on unmount
    return () => {
      unsubscribeConnectionState();
      unsubscribeStateUpdate();
      webSocketService.disconnect();
    };
  }, [setConnectionState, applyIncrementalUpdate]);

  // Collect all entities from districts and city
  const allSensors = cityState?.districts.flatMap((district) => district.sensors) || [];
  const allBuildings = cityState?.districts.flatMap((district) => district.buildings) || [];
  const allWeatherStations = cityState?.districts.flatMap((district) => district.weatherStations) || [];
  const allRoadSegments = cityState?.cityGraph.edges || [];
  const allBuses = cityState?.publicTransport.buses || [];

  return (
    <Layout>
      <div className="w-full h-full relative">
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-gray-100 z-10">
            <LoadingSpinner size="lg" message="Loading city state..." />
          </div>
        )}

        {error && !cityState && (
          <div className="absolute inset-0 flex items-center justify-center bg-gray-100 z-10">
            <ErrorMessage
              message={error.message || 'Failed to load city state'}
              onRetry={loadInitialState}
            />
          </div>
        )}

        {cityState && (
          <Map>
            {/* Road segments as base layer */}
            <RoadSegmentLayer edges={allRoadSegments} />
            
            {/* Building markers */}
            <BuildingMarkers buildings={allBuildings} />
            
            {/* Sensor markers */}
            <SensorMarkers sensors={allSensors} />
            
            {/* Weather station markers */}
            <WeatherStationMarkers stations={allWeatherStations} />
            
            {/* Bus markers */}
            <BusMarkers buses={allBuses} />
          </Map>
        )}
      </div>
    </Layout>
  );
}
