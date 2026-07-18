import csv
import json
import os
import sys
from confluent_kafka import Consumer, Producer, KafkaError

def load_route_schedule(csv_path):
    """Loads route schedule CSV into a dictionary (acting as our KTable)."""
    schedule = {}
    if not os.path.exists(csv_path):
        print(f"Error: Static schedule CSV file not found at {csv_path}")
        sys.exit(1)
        
    with open(csv_path, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            schedule[row['route_id'].strip()] = {
                'route_name': row['route_name'].strip(),
                'terminal': row['terminal'].strip(),
                'scheduled_arrival_time': row['scheduled_arrival_time'].strip()
            }
    print(f"Loaded {len(schedule)} routes into memory KTable.")
    return schedule

def delivery_report(err, msg):
    if err is not None:
        print(f"Enriched message delivery failed: {err}")

def main():
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    csv_path = '../data/route_schedule.csv'
    
    # Resolve path relative to script directory
    script_dir = os.path.dirname(os.path.realpath(__file__))
    absolute_csv_path = os.path.join(script_dir, csv_path)
    
    # Load KTable reference data
    route_ktable = load_route_schedule(absolute_csv_path)
    
    # Consumer config
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'gps-enrichment-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    
    # Producer config
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'gps-enrichment-producer',
        'acks': 1
    }
    
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    
    input_topic = 'urbanpulse.bus_gps'
    output_topic = 'urbanpulse.bus_gps_enriched'
    
    consumer.subscribe([input_topic])
    print(f"Subscribed to {input_topic}. Processing stream-table join... Press Ctrl+C to stop.")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue
            
            # Parse message value
            try:
                gps_data = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                print(f"Error parsing GPS message JSON: {e}")
                continue
                
            route_id = gps_data.get('route_id')
            
            # Perform Stream-Table Join lookup (KTable join)
            route_meta = route_ktable.get(route_id)
            
            if route_meta:
                # Enrich payload
                gps_data['route_name'] = route_meta['route_name']
                gps_data['terminal'] = route_meta['terminal']
                gps_data['scheduled_arrival_time'] = route_meta['scheduled_arrival_time']
                gps_data['enrichment_status'] = 'SUCCESS'
            else:
                # Handle missing key
                gps_data['route_name'] = 'UNKNOWN_ROUTE'
                gps_data['terminal'] = 'UNKNOWN'
                gps_data['scheduled_arrival_time'] = '00:00'
                gps_data['enrichment_status'] = 'UNMAPPED_ROUTE'
                print(f"Warning: route_id '{route_id}' not found in KTable static schedule.")
                
            # Write to enriched stream
            enriched_json = json.dumps(gps_data)
            
            # Key by route_id to preserve partitions/ordering downstream
            producer.produce(
                output_topic,
                key=msg.key(),  # Keep the same route_id key
                value=enriched_json.encode('utf-8'),
                callback=delivery_report
            )
            
            producer.poll(0)
            
    except KeyboardInterrupt:
        print("\nStopping enrichment worker...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == '__main__':
    main()
