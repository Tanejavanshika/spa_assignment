import time
import json
import random
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def main():
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'meter-producer',
        'linger.ms': 10,
        'acks': 1
    }
    
    producer = Producer(conf)
    topic = 'urbanpulse.smart_meters'
    dlq_topic = 'urbanpulse.dlq'
    
    # MetroConnect Wards
    wards = ['Ward-1', 'Ward-2', 'Ward-3', 'Ward-4', 'Ward-5', 'Ward-6']
    meters = []
    for i in range(1, 31):  # Simulate 30 meters
        meters.append({
            'meter_id': f'MET-{1000 + i}',
            'ward_id': random.choice(wards),
            'kwh_reading': random.uniform(100.0, 1000.0),
            'voltage': 230.0,
            'power_factor': 0.95
        })
        
    print("Starting Smart Meter producer. Press Ctrl+C to stop.")
    
    try:
        while True:
            for meter in meters:
                # Increment consumption slightly
                meter['kwh_reading'] += random.uniform(0.01, 0.15)
                # Small fluctuations in voltage and power factor
                voltage = random.uniform(210.0, 245.0)
                power_factor = random.uniform(0.85, 0.99)
                
                # Introduce a 2% chance of generating invalid meter readings for DLQ
                inject_error = (random.random() < 0.02)
                
                payload = {
                    'meter_id': meter['meter_id'],
                    'ward_id': meter['ward_id'],
                    'kwh_reading': meter['kwh_reading'] if not inject_error else -10.0, # Invalid negative reading
                    'voltage': voltage if not inject_error else 300.0,                 # Invalid voltage (>260)
                    'power_factor': power_factor,
                    'timestamp': int(time.time() * 1000)
                }
                
                # Validation rules
                validation_errors = []
                if payload['kwh_reading'] < 0:
                    validation_errors.append(f"kWh reading {payload['kwh_reading']} cannot be negative")
                if not (180.0 <= payload['voltage'] <= 260.0):
                    validation_errors.append(f"Voltage {payload['voltage']} is out of bounds [180, 260]")
                
                if validation_errors:
                    dlq_payload = {
                        'original_message': payload,
                        'error_reason': "; ".join(validation_errors),
                        'timestamp_failed': int(time.time() * 1000)
                    }
                    dlq_json = json.dumps(dlq_payload)
                    producer.produce(
                        dlq_topic,
                        key=payload['meter_id'].encode('utf-8'),
                        value=dlq_json.encode('utf-8'),
                        callback=delivery_report
                    )
                    print(f"[DLQ TRIGGER] Meter {payload['meter_id']} failed validation: {dlq_payload['error_reason']}. Routed to DLQ.")
                else:
                    key_str = payload['meter_id']
                    val_json = json.dumps(payload)
                    producer.produce(
                        topic,
                        key=key_str.encode('utf-8'),
                        value=val_json.encode('utf-8'),
                        callback=delivery_report
                    )
                
                producer.poll(0)
                
            producer.flush()
            time.sleep(2.0)
            
    except KeyboardInterrupt:
        print("\nStopping Meter producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
