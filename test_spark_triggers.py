import json
import time
from confluent_kafka import Producer

def main():
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    
    print("=== INJECTING SMART METER TELEMETRY TO ADVANCE SPARK WATERMARK ===")
    
    # Let's define a base timestamp in milliseconds (e.g., representing 12:00 PM today)
    # We will use epoch time but subtracted by 2 hours as our starting point
    base_time = int(time.time() * 1000) - (3 * 3600 * 1000) # 3 hours ago
    
    # We will send 4 events for MET-TEST-001 in Ward-99:
    # 1. Event at t0 (starts a window)
    # 2. Event at t0 + 10 mins (inside the same window)
    # 3. Event at t0 + 20 mins (starts next window)
    # 4. Event at t0 + 70 mins (advances watermark past first two windows)
    
    events = [
        {
            'meter_id': 'MET-TEST-001',
            'ward_id': 'Ward-99',
            'kwh_reading': 150.0,
            'voltage': 230.0,
            'power_factor': 0.95,
            'timestamp': base_time  # t0
        },
        {
            'meter_id': 'MET-TEST-001',
            'ward_id': 'Ward-99',
            'kwh_reading': 155.5,
            'voltage': 228.0,
            'power_factor': 0.94,
            'timestamp': base_time + (10 * 60 * 1000)  # t0 + 10 mins
        },
        {
            'meter_id': 'MET-TEST-001',
            'ward_id': 'Ward-99',
            'kwh_reading': 162.0,
            'voltage': 231.0,
            'power_factor': 0.96,
            'timestamp': base_time + (20 * 60 * 1000)  # t0 + 20 mins (next window)
        },
        {
            'meter_id': 'MET-TEST-001',
            'ward_id': 'Ward-99',
            'kwh_reading': 180.0,
            'voltage': 230.0,
            'power_factor': 0.95,
            'timestamp': base_time + (70 * 60 * 1000)  # t0 + 70 mins
        }
    ]
    
    print("\nInjecting events...")
    for idx, event in enumerate(events):
        event_time_str = time.strftime('%H:%M:%S', time.localtime(event['timestamp']/1000))
        print(f" -> Event {idx+1}: Timestamp={event_time_str} ({event['timestamp']}) | kWh={event['kwh_reading']}")
        
        producer.produce(
            'urbanpulse.smart_meters',
            key=event['meter_id'].encode('utf-8'),
            value=json.dumps(event).encode('utf-8')
        )
        producer.flush()
        time.sleep(1.0)
        
    print("\nTrigger events injected! Watermark has been advanced.")
    print("Spark will now close the first window and write to Parquet. Waiting 10s for Spark to output...")
    time.sleep(10)
    print("Done!")

if __name__ == '__main__':
    main()
