import time
import json
import random
import sys
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    # We omit success print to avoid spamming the console at high rates

def main():
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    
    # Configure producer
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'bus-gps-producer',
        'linger.ms': 10,
        'acks': 'all'
    }
    
    producer = Producer(conf)
    
    gps_topic = 'urbanpulse.bus_gps'
    dlq_topic = 'urbanpulse.dlq'
    
    # Simulated bus metadata
    routes = ['R101', 'R102', 'R103', 'R104', 'R105', 'R106', 'R107', 'R108']
    buses = []
    for i in range(1, 51):  # Simulate 50 active buses
        buses.append({
            'bus_id': f'BUS-{1000 + i:04d}',
            'route_id': random.choice(routes),
            'lat': random.uniform(18.9, 19.2),
            'lon': random.uniform(72.8, 73.0),
            'speed_kmh': random.uniform(10, 60),
            'occupancy_pct': random.randint(10, 95)
        })
    
    print("Starting Bus GPS producer. Press Ctrl+C to stop.")
    
    try:
        while True:
            for bus in buses:
                # Update position and telemetry with small random walk
                bus['lat'] += random.uniform(-0.001, 0.001)
                bus['lon'] += random.uniform(-0.001, 0.001)
                bus['speed_kmh'] = max(0, min(100, bus['speed_kmh'] + random.uniform(-5, 5)))
                bus['occupancy_pct'] = max(0, min(100, bus['occupancy_pct'] + random.randint(-5, 5)))
                
                # Introduce a 2% chance of generating impossible coordinate/speed validation failures
                inject_error = (random.random() < 0.02)
                
                payload = {
                    'bus_id': bus['bus_id'],
                    'route_id': bus['route_id'],
                    'lat': bus['lat'] if not inject_error else 95.0,  # Invalid latitude (>90)
                    'lon': bus['lon'] if not inject_error else 185.0, # Invalid longitude (>180)
                    'speed_kmh': bus['speed_kmh'] if not inject_error else 150.0, # Invalid speed
                    'occupancy_pct': bus['occupancy_pct'],
                    'timestamp': int(time.time() * 1000)
                }
                
                # Validation rules
                validation_errors = []
                if not (-90.0 <= payload['lat'] <= 90.0) or not (18.8 <= payload['lat'] <= 19.3):
                    validation_errors.append(f"Latitude {payload['lat']} out of bounds [18.8, 19.3]")
                if not (-180.0 <= payload['lon'] <= 180.0) or not (72.7 <= payload['lon'] <= 73.1):
                    validation_errors.append(f"Longitude {payload['lon']} out of bounds [72.7, 73.1]")
                if not (0.0 <= payload['speed_kmh'] <= 120.0):
                    validation_errors.append(f"Speed {payload['speed_kmh']} km/h is impossible")
                
                if validation_errors:
                    # Message failed validation, route to DLQ
                    dlq_payload = {
                        'original_message': payload,
                        'error_reason': "; ".join(validation_errors),
                        'timestamp_failed': int(time.time() * 1000)
                    }
                    dlq_json = json.dumps(dlq_payload)
                    # Produce to DLQ topic
                    producer.produce(
                        dlq_topic, 
                        key=payload['bus_id'].encode('utf-8'), 
                        value=dlq_json.encode('utf-8'), 
                        callback=delivery_report
                    )
                    print(f"[DLQ TRIGGER] Bus {payload['bus_id']} validation failed: {dlq_payload['error_reason']}. Routed to DLQ.")
                else:
                    # Valid message, send to main topic
                    # Key by route_id to guarantee sequential ordering per route
                    key_str = payload['route_id']
                    val_json = json.dumps(payload)
                    producer.produce(
                        gps_topic, 
                        key=key_str.encode('utf-8'), 
                        value=val_json.encode('utf-8'), 
                        callback=delivery_report
                    )
                
                # Poll to handle delivery callbacks
                producer.poll(0)
                
            producer.flush()
            time.sleep(2.0)  # Wait 2 seconds before sending next batch
            
    except KeyboardInterrupt:
        print("\nStopping Bus GPS producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
