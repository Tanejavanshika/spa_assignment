import argparse
import time
import sys
from confluent_kafka import Consumer, KafkaError, TopicPartition

def main():
    parser = argparse.ArgumentParser(description="UrbanPulse Priority Consumers")
    parser.add_argument(
        '--mode', 
        choices=['high', 'standard'], 
        required=True,
        help="Consumer mode: 'high' (real-time signal control) or 'standard' (slow analytics)"
    )
    parser.add_argument(
        '--id',
        type=int,
        default=1,
        help="Consumer ID for standard priority instances (1, 2, or 3)"
    )
    
    args = parser.parse_args()
    
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    topic = 'urbanpulse.traffic_signals'
    
    if args.mode == 'high':
        group_id = 'HIGH_PRIORITY_GROUP'
        client_id = 'high-priority-consumer'
        delay_sec = 0.0
        print("[HIGH_PRIORITY] Starting real-time signal control consumer (reads all partitions, 0ms delay)...")
    else:
        group_id = 'STANDARD_PRIORITY_GROUP'
        client_id = f'standard-priority-consumer-{args.id}'
        delay_sec = 0.5  # Simulate processing bottleneck (500ms per message)
        print(f"[STANDARD_PRIORITY-{args.id}] Starting analytics consumer (sharing group, 500ms delay)...")

    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'client.id': client_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    msg_count = 0
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[{group_id}] Error: {msg.error()}")
                    continue
            
            # Simulated processing time
            if delay_sec > 0:
                time.sleep(delay_sec)
                
            msg_count += 1
            
            # Print status periodically to avoid console I/O bottlenecks
            if args.mode == 'high':
                should_print = (msg_count % 100 == 0)
            else:
                should_print = (msg_count % 5 == 0)
                
            if should_print:
                tp = TopicPartition(msg.topic(), msg.partition())
                try:
                    low_watermark, high_watermark = consumer.get_watermark_offsets(tp, timeout=0.5)
                    # Lag is the difference between the high watermark (latest message offset) and the current processed offset
                    lag = max(0, high_watermark - msg.offset() - 1)
                except Exception:
                    lag = "Unknown"
                    
                print(f"[{group_id}] Instance {client_id} | Partition {msg.partition()} | Msg Offset: {msg.offset()} | Lag: {lag}")


                
    except KeyboardInterrupt:
        print(f"\n[{group_id}] Stopping consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
