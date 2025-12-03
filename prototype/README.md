# Emergency Management System - Prototype

Sistema prototipale di gestione emergenze stradali basato su architettura a microservizi con Docker Compose.

## Architettura

Il sistema è composto da:

- **3 Edge Simulators**: Simulano sensori di velocità, meteo e **camera** per 3 distretti (North, South, Center)
- **Apache Kafka**: Event bus per comunicazione asincrona
- **Monitor Python**: Consumer Kafka che valida e scrive su InfluxDB
- **InfluxDB**: Database time-series per metriche sensori

## Prerequisiti

- Docker
- Docker Compose

## Avvio Rapido

```bash
# Avvia tutti i servizi
docker-compose up -d

# Verifica lo stato dei servizi
docker-compose ps

# Visualizza i log
docker-compose logs -f

# Ferma tutti i servizi
docker-compose down

# Ferma e rimuove i volumi (cancella dati InfluxDB)
docker-compose down -v
```

## Verifica Funzionamento

### 1. Verifica Topic Kafka

```bash
# Lista topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consuma messaggi dal topic (CTRL+C per uscire)
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-data \
  --from-beginning
```

### 2. Accedi a InfluxDB UI

Apri browser su: http://localhost:8086

Credenziali:
- Username: `admin`
- Password: `adminpassword`

Organizzazione: `emergency-mgmt`
Bucket: `sensor_metrics`

### 3. Query Dati in InfluxDB

Nella UI di InfluxDB, vai su "Data Explorer" e usa le seguenti query:

```flux
// Dati velocità ultimi 1h
from(bucket: "sensor_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_speed")

// Dati meteo ultimi 1h
from(bucket: "sensor_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_weather")

// Dati camera - condizioni stradali ultimi 1h
from(bucket: "sensor_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_camera")

// Conteggio incidenti per distretto
from(bucket: "sensor_metrics")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "sensor_camera")
  |> filter(fn: (r) => r.road_condition == "accident")
  |> group(columns: ["district_id"])
  |> count()
```

## Struttura Progetto

```
prototype/
├── docker-compose.yml          # Orchestrazione servizi
├── edge-simulator/             # Edge device simulator
│   ├── Dockerfile
│   ├── edge_producer.py        # Producer Kafka
│   └── requirements.txt
├── monitor-python/             # Monitor consumer
│   ├── Dockerfile
│   ├── monitor_consumer.py     # Consumer Kafka + InfluxDB writer
│   └── requirements.txt
├── .env.example                # Variabili d'ambiente esempio
└── README.md
```

## Funzionalità Implementate

### Edge Simulators
- **Sensori Velocità**: Generazione dati velocità con pre-aggregazione (moving average)
- **Sensori Meteo**: Generazione dati temperatura, umidità e condizioni meteo
- **Sensori Camera**: Edge analytics per valutazione condizioni stradali
  - Analisi locale senza invio immagini/video
  - Categorie: `clear`, `congestion`, `accident`, `obstacles`, `flooding`
  - Confidence score (0.0-1.0) per affidabilità rilevamento
  - Conteggio veicoli opzionale per traffico
- Buffer locale per resilienza (fino a 1000 messaggi)
- Invio a Kafka ogni 3-5 secondi (configurabile)

### Monitor Python
- Consumo da Kafka con consumer group
- Validazione struttura messaggi per 3 tipi sensori (speed, weather, camera)
- Validazione condizioni stradali e confidence score per sensori camera
- Trasformazione in formato InfluxDB
- Scrittura batch su InfluxDB
- Statistiche di elaborazione

### InfluxDB
- Bucket con retention 30 giorni
- Precision nanoseconds per timestamp
- **Measurements**: `sensor_speed`, `sensor_weather`, `sensor_camera`
- **Tags** indicizzati: district_id, edge_id, sensor_type, road_condition (camera), weather_conditions (weather)
- **Fields**: metriche numeriche (speed_kmh, temperature_c, humidity, confidence_score, vehicle_count, **latitude**, **longitude**)

### GPS Coordinates
Ogni sensore include coordinate GPS precise per visualizzazione su mappa:
- **District North** (Flaminio): 41.9109°N, 12.4818°E
- **District South** (EUR): 41.8719°N, 12.4801°E
- **District Center** (Colosseum): 41.8902°N, 12.4922°E

Le coordinate permettono:
- Visualizzazione su OpenStreetMap per digital twin della città
- Analisi spaziale delle condizioni stradali
- Correlazione geografica tra sensori vicini

## Test Resilienza

### Simula Disconnessione Monitor

```bash
# Ferma il monitor
docker-compose stop monitor-python

# Gli edge continuano a produrre, Kafka bufferizza

# Riavvia il monitor dopo 30 secondi
docker-compose start monitor-python

# Verifica che tutti i messaggi vengano processati (nessuna perdita)
docker-compose logs monitor-python
```

## Troubleshooting

### Servizio non parte
```bash
# Controlla i log del servizio specifico
docker-compose logs <service-name>

# Ricostruisci le immagini
docker-compose build --no-cache
docker-compose up -d
```

### Kafka non riceve messaggi
```bash
# Verifica connettività Kafka
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### InfluxDB non salva dati
```bash
# Verifica health InfluxDB
docker-compose exec influxdb influx ping

# Controlla token e configurazione
docker-compose logs influxdb
```

## Performance

- Edge simulators: ~20-30 messaggi/minuto ciascuno
- Monitor consumer: >1000 messaggi/secondo
- InfluxDB: Write throughput dipendente da hardware

## Sviluppo Futuro

- Aggiungere più edge simulators
- Implementare dashboard Grafana
- Aggiungere alerting su anomalie
- Implementare analytics real-time con Kafka Streams
