import sys
from confluent_kafka.admin import AdminClient, NewTopic

def create_urbanpulse_topics():
    # Bootstrap servers representing our 3 external ports
    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Topic definitions
    # Retention times are in milliseconds:
    # - bus_gps: 24 hours = 24 * 60 * 60 * 1000 = 86,400,000 ms (For replay during accident investigations)
    # - traffic_signals: 7 days = 7 * 24 * 60 * 60 * 1000 = 604,800,000 ms (Operational context window)
    # - air_quality: 90 days = 90 * 24 * 60 * 60 * 1000 = 7,776,000,000 ms (Pollution trend analysis)
    # - smart_meters: 365 days = 365 * 24 * 60 * 60 * 1000 = 31,536,000,000 ms (Regulatory energy audits)
    # - dlq: 14 days = 14 * 24 * 60 * 60 * 1000 = 1,209,600,000 ms (Dead Letter Queue troubleshooting)
    # - bus_gps_enriched: 6 partitions, 24 hours retention (For ETA routing)
    # - incidents: 2 partitions, 30 days retention (For tracking historical alerts)
    
    topic_configs = [
        {
            'name': 'urbanpulse.bus_gps',
            'partitions': 6,
            'retention_ms': 86400000,
            'reason': 'Retained for 24 hours to enable log replays for traffic accident investigation and route analysis.'
        },
        {
            'name': 'urbanpulse.bus_gps_enriched',
            'partitions': 6,
            'retention_ms': 86400000,
            'reason': 'Retained for 24 hours to sync with raw bus GPS logs and support real-time active routing.'
        },
        {
            'name': 'urbanpulse.traffic_signals',
            'partitions': 3,
            'retention_ms': 604800000,
            'reason': 'Retained for 7 days for short-term traffic flow analysis, congestion debugging, and validation.'
        },
        {
            'name': 'urbanpulse.air_quality',
            'partitions': 2,
            'retention_ms': 7776000000,
            'reason': 'Retained for 90 days to capture seasonal pollution changes and support environmental health trend analysis.'
        },
        {
            'name': 'urbanpulse.smart_meters',
            'partitions': 4,
            'retention_ms': 31536000000,
            'reason': 'Retained for 365 days (1 year) to comply with regulatory mandates for green audits and billing verification.'
        },
        {
            'name': 'urbanpulse.dlq',
            'partitions': 2,
            'retention_ms': 1209600000,
            'reason': 'Retained for 14 days to provide developers time to diagnose and re-inject failed/malformed telemetry.'
        },
        {
            'name': 'urbanpulse.incidents',
            'partitions': 2,
            'retention_ms': 2592000000, # 30 days
            'reason': 'Retained for 30 days to store emergency events (AQI breaches, bunching) for administrative review.'
        }
    ]

    new_topics = []
    print("Preparing topic creation requests...")
    for config in topic_configs:
        print(f" - {config['name']}: Partitions={config['partitions']}, Retention={config['retention_ms']} ms ({config['reason']})")
        new_topics.append(
            NewTopic(
                topic=config['name'],
                num_partitions=config['partitions'],
                replication_factor=3,  # Replicated across all 3 brokers
                config={
                    'retention.ms': str(config['retention_ms']),
                    'min.insync.replicas': '2'  # Guarantee write durability
                }
            )
        )

    # Call create_topics
    fs = admin_client.create_topics(new_topics)

    # Wait for each operation to finish
    success = True
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None if successful
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "TopicAlreadyExistsException" in str(e) or "already exists" in str(e).lower():
                print(f"Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic '{topic}': {e}")
                success = False

    if success:
        print("\nAll topics verified and configured successfully!")
    else:
        print("\nSome topic configurations failed. Please inspect logs.")
        sys.exit(1)

if __name__ == '__main__':
    create_urbanpulse_topics()
