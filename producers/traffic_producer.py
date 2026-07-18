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
        'client.id': 'traffic-producer',
        'linger.ms': 5,
        'acks': 1
    }
    
    producer = Producer(conf)
    topic = 'urbanpulse.traffic_signals'
    
    # MetroConnect Junctions
    zones = ['Zone-A', 'Zone-B', 'Zone-C', 'Zone-D', 'Zone-E', 'Zone-F']
    junctions = []
    for i in range(1, 13):  # 12 junctions
        junctions.append({
            'junction_id': f'JNC-{100 + i}',
            'zone': random.choice(zones),
            'signal_phase': 'RED'
        })
        
    phases = ['RED', 'GREEN', 'YELLOW']
    
    print("Starting Traffic Signal producer. Press Ctrl+C to stop.")
    
    try:
        while True:
            for jnc in junctions:
                # Update phase
                if random.random() < 0.3:
                    jnc['signal_phase'] = random.choice(phases)
                
                payload = {
                    'junction_id': jnc['junction_id'],
                    'zone': jnc['zone'],
                    'vehicle_count': random.randint(5, 120),
                    'avg_wait_sec': random.randint(10, 240),
                    'signal_phase': jnc['signal_phase'],
                    'timestamp': int(time.time() * 1000)
                }
                
                key_str = jnc['junction_id']
                val_json = json.dumps(payload)
                
                producer.produce(
                    topic,
                    key=key_str.encode('utf-8'),
                    value=val_json.encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                
            producer.flush()
            # Fast generation to test consumer lag differences
            time.sleep(0.1) 
            
    except KeyboardInterrupt:
        print("\nStopping Traffic producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
