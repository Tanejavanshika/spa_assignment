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
    
    # Configure producer for at-least-once semantics
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'air-quality-producer',
        'acks': 'all',                  # Wait for leader and all replicas to acknowledge
        'enable.idempotence': True,     # Enable idempotency to prevent duplicates from retries
        'retries': 5,                   # Retry 5 times on transient network failures
        'retry.backoff.ms': 100,        # Wait 100ms before retrying
        'max.in.flight.requests.per.connection': 5
    }
    
    producer = Producer(conf)
    
    aq_topic = 'urbanpulse.air_quality'
    dlq_topic = 'urbanpulse.dlq'
    
    # Simulated Air Quality Sensors across zones
    zones = ['Zone-A', 'Zone-B', 'Zone-C', 'Zone-D', 'Zone-E', 'Zone-F']
    sensors = []
    for i in range(1, 11):  # Simulate 10 stations
        sensors.append({
            'sensor_id': f'AQ-SEN-{100 + i}',
            'zone': random.choice(zones),
            'pm25_base': random.uniform(20, 150),
            'pm10_base': random.uniform(30, 200),
            'no2_base': random.uniform(10, 80)
        })
    
    print("Starting Air Quality producer with At-Least-Once configs. Press Ctrl+C to stop.")
    
    try:
        while True:
            for sensor in sensors:
                # Add small variations to bases
                pm25 = max(5, sensor['pm25_base'] + random.uniform(-10, 10))
                pm10 = max(10, sensor['pm10_base'] + random.uniform(-15, 15))
                no2 = max(5, sensor['no2_base'] + random.uniform(-5, 5))
                
                # Simple AQI mapping calculation
                # Standard linear conversion approximation
                aqi = int(max(pm25 * 2.1, pm10 * 1.2, no2 * 1.5))
                
                # Simulating sensor failure: 5% chance of AQI arriving as null
                is_failed = (random.random() < 0.05)
                
                payload = {
                    'sensor_id': sensor['sensor_id'],
                    'zone': sensor['zone'],
                    'pm25': round(pm25, 2),
                    'pm10': round(pm10, 2),
                    'no2': round(no2, 2),
                    'aqi': aqi if not is_failed else None,  # Simulated null AQI
                    'timestamp': int(time.time() * 1000)
                }
                
                # Validation check
                validation_errors = []
                if payload['aqi'] is None:
                    validation_errors.append("AQI reading is null/missing due to sensor timeout")
                elif not (0 <= payload['aqi'] <= 500):
                    validation_errors.append(f"AQI reading {payload['aqi']} is out of range [0, 500]")
                
                if validation_errors:
                    # Log failure and route to DLQ
                    dlq_payload = {
                        'original_message': payload,
                        'error_reason': "; ".join(validation_errors),
                        'timestamp_failed': int(time.time() * 1000)
                    }
                    dlq_json = json.dumps(dlq_payload)
                    producer.produce(
                        dlq_topic,
                        key=payload['sensor_id'].encode('utf-8'),
                        value=dlq_json.encode('utf-8'),
                        callback=delivery_report
                    )
                    print(f"[DLQ TRIGGER] Sensor {payload['sensor_id']} failed validation: {dlq_payload['error_reason']}. Routed to DLQ.")
                else:
                    # Valid message, send to main topic
                    key_str = payload['sensor_id']
                    val_json = json.dumps(payload)
                    producer.produce(
                        aq_topic,
                        key=key_str.encode('utf-8'),
                        value=val_json.encode('utf-8'),
                        callback=delivery_report
                    )
                
                producer.poll(0)
            
            producer.flush()
            time.sleep(3.0)  # Emit every 3 seconds
            
    except KeyboardInterrupt:
        print("\nStopping Air Quality producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
