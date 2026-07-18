import json
import time
from confluent_kafka import Producer, Consumer, KafkaError

def main():
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    
    print("=== URBANPULSE TASK C INTEGRATION TEST TRACE ===")
    
    # Base timestamp in milliseconds
    base_time = int(time.time() * 1000)
    
    # 1. Inject AQI Emergency Telemetry (AQI > 300)
    print("\n1. Injecting AQI Emergency event (AQI = 350 > 300)...")
    aqi_event = {
        'sensor_id': 'AQ-SEN-TEST-EMERGENCY',
        'zone': 'Zone-C',
        'pm25': 160.0,
        'pm10': 250.0,
        'no2': 90.0,
        'aqi': 350,  # Trigger value
        'timestamp': base_time
    }
    producer.produce(
        'urbanpulse.air_quality',
        key=aqi_event['sensor_id'].encode('utf-8'),
        value=json.dumps(aqi_event).encode('utf-8')
    )
    
    # 2. Inject Traffic Gridlock Telemetry (3 consecutive wait times > 180s)
    # We will send 3 events spaced in event-time (e.g. 3 consecutive cycles)
    print("\n2. Injecting Traffic Gridlock events (3 consecutive cycles > 180s)...")
    junction_id = 'JNC-GRIDLOCK-TEST'
    for cycle in range(1, 4):
        traffic_event = {
            'junction_id': junction_id,
            'zone': 'Zone-A',
            'vehicle_count': 95,
            'avg_wait_sec': 195,  # Exceeds 180s
            'signal_phase': 'RED',
            'timestamp': base_time + (cycle * 60 * 1000)  # Spaced 1 minute apart
        }
        print(f" -> Sending cycle {cycle} telemetry (wait={traffic_event['avg_wait_sec']}s)")
        producer.produce(
            'urbanpulse.traffic_signals',
            key=junction_id.encode('utf-8'),
            value=json.dumps(traffic_event).encode('utf-8')
        )
        # Flush each to guarantee sequential arrival in Kafka
        producer.flush()
        time.sleep(0.5)

    # 3. Inject Bus Bunching Telemetry
    # Two buses (BUS-9901, BUS-9902) on route R101, within 200m of each other for > 5 minutes
    # We will inject coordinates that are ~110m apart (0.001 latitude diff)
    # We send updates at t=0, t=2m, t=4m, t=6m to advance Flink's event-time state beyond 5 mins.
    print("\n3. Injecting Bus Bunching events (2 buses within 200m for > 5 minutes)...")
    bus_a = 'BUS-9901'
    bus_b = 'BUS-9902'
    route_id = 'R101'
    
    # Times: 0, 2 minutes, 4 minutes, 6 minutes
    intervals = [0, 2 * 60 * 1000, 4 * 60 * 1000, 6 * 60 * 1000]
    
    for idx, offset_ms in enumerate(intervals):
        event_time = base_time + offset_ms
        print(f" -> Sending GPS positions at t + {offset_ms/60000:.0f} mins (distance ~110m)")
        
        gps_a = {
            'bus_id': bus_a,
            'route_id': route_id,
            'lat': 19.000,
            'lon': 72.800,
            'speed_kmh': 15.0,
            'occupancy_pct': 45,
            'timestamp': event_time
        }
        
        gps_b = {
            'bus_id': bus_b,
            'route_id': route_id,
            'lat': 19.001, # ~111m north of lat 19.000
            'lon': 72.800,
            'speed_kmh': 12.0,
            'occupancy_pct': 60,
            'timestamp': event_time
        }
        
        # Send both coordinates
        producer.produce('urbanpulse.bus_gps', key=route_id.encode('utf-8'), value=json.dumps(gps_a).encode('utf-8'))
        producer.produce('urbanpulse.bus_gps', key=route_id.encode('utf-8'), value=json.dumps(gps_b).encode('utf-8'))
        producer.flush()
        time.sleep(0.5)

    print("\nAll trigger telemetries injected successfully! Flushing producer...")
    producer.flush()
    
    # 4. Consume and print alerts from the incidents topic
    print("\n4. Listening to 'urbanpulse.incidents' for alerts (waiting 15s for Flink output)...")
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': f'alert-verifier-group-{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['urbanpulse.incidents'])
    
    alerts_received = []
    listen_end_time = time.time() + 15  # Listen for 15 seconds
    
    try:
        while time.time() < listen_end_time:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer Error: {msg.error()}")
                    break
            
            alert = json.loads(msg.value().decode('utf-8'))
            alerts_received.append(alert)
            print(f"\n[ALERT RECEIVED] Type: {alert['incident_type']}")
            print(f" Severity: {alert['severity']}")
            print(f" Details: {alert['details']}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        
    print(f"\nIntegration test finished. Total alerts captured: {len(alerts_received)}/3 expected.")
    if len(alerts_received) >= 3:
        print("Success: All Flink incident patterns validated successfully!")
    else:
        print("Warning: Did not capture all 3 alerts. Check Flink container logs.")

if __name__ == '__main__':
    main()
