import json
import time
from confluent_kafka import Producer, Consumer

def main():
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    
    # 1. Produce high AQI event
    print("=== INJECTING TELEMETRY FOR SPARK SQL HEALTH ADVISORY ===")
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    
    current_time = int(time.time() * 1000)
    
    # Zone-A is selected because it exists in zone_profile.csv
    aq_data = {
        'sensor_id': 'AQ-SEN-SQL-TEST',
        'zone': 'Zone-A',
        'pm25': 80.0,
        'pm10': 120.0,
        'no2': 45.0,
        'aqi': 180,  # > 150 (Unhealthy)
        'timestamp': current_time
    }
    
    print(f" -> Injecting AQI event: Zone=Zone-A | AQI=180 | Timestamp={current_time}")
    producer.produce(
        'urbanpulse.air_quality',
        key=b'Zone-A',
        value=json.dumps(aq_data).encode('utf-8')
    )
    producer.flush()
    print("Event injected successfully!")
    
    # 2. Consume from health_advisories topic
    print("\nListening for Spark output on 'urbanpulse.health_advisories' (waiting 15s)...")
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': f'health-advisory-verifier-{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['urbanpulse.health_advisories'])
    
    start_time = time.time()
    advisories_received = []
    
    while time.time() - start_time < 15:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue
            
        payload = json.loads(msg.value().decode('utf-8'))
        advisories_received.append(payload)
        print("\n[HEALTH ADVISORY RECEIVED]")
        print(f" -> Zone ID: {payload.get('zone')}")
        print(f" -> Zone Name: {payload.get('zone_name')}")
        print(f" -> Population: {payload.get('population')}")
        print(f" -> Schools Count: {payload.get('number_schools')}")
        print(f" -> Rolling Average AQI: {payload.get('rolling_avg_aqi')}")
        print(f" -> Window: {payload.get('window_start')} to {payload.get('window_end')}")
        break  # We only need one message to verify correctness
        
    consumer.close()
    
    if advisories_received:
        print("\nSuccess: Spark SQL Health Advisory pipeline validated successfully!")
    else:
        print("\nFailure: Did not capture any advisories. Check Spark container logs.")

if __name__ == '__main__':
    main()
