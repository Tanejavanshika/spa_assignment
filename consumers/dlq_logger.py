import time
import json
import argparse
from confluent_kafka import Consumer, KafkaError

def main():
    parser = argparse.ArgumentParser(description="UrbanPulse DLQ Logger & Reporting Engine")
    parser.add_argument(
        '--duration', 
        type=int, 
        default=300, 
        help="Duration to run and gather stats in seconds (default: 300 seconds for 5-min report)"
    )
    args = parser.parse_args()

    bootstrap_servers = 'localhost:19092,localhost:29092,localhost:39092'
    topic = 'urbanpulse.dlq'
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'dlq-reporting-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    print(f"DLQ Logger started. Subscribed to '{topic}'.")
    print(f"Collecting validation errors for {args.duration} seconds ({args.duration/60:.1f} minutes)...")
    
    error_counts = {}
    total_errors = 0
    start_time = time.time()
    end_time = start_time + args.duration
    
    try:
        while time.time() < end_time:
            remaining_time = max(0.1, end_time - time.time())
            msg = consumer.poll(timeout=min(remaining_time, 1.0))
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"DLQ Consumer Error: {msg.error()}")
                    continue
                    
            try:
                dlq_record = json.loads(msg.value().decode('utf-8'))
                reason = dlq_record.get('error_reason', 'Unknown validation failure')
                original_msg = dlq_record.get('original_message', {})
                sensor_or_device = (
                    original_msg.get('bus_id') or 
                    original_msg.get('sensor_id') or 
                    original_msg.get('meter_id') or 
                    'Unknown'
                )
                
                # Classify the error type
                error_type = "Other Validation Error"
                if "null" in reason.lower() or "missing" in reason.lower():
                    error_type = "AQI Value Null/Sensor Timeout"
                elif "latitude" in reason.lower() or "longitude" in reason.lower():
                    error_type = "Geospatial Boundary Violation (GPS)"
                elif "speed" in reason.lower():
                    error_type = "Impossible Speed Telemetry (GPS)"
                elif "kwh" in reason.lower() or "negative" in reason.lower():
                    error_type = "Negative Power Reading (Smart Meter)"
                elif "voltage" in reason.lower():
                    error_type = "Voltage Spike/Out of Bounds (Smart Meter)"
                
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
                total_errors += 1
                
                print(f"[DLQ LOGGED] Type: {error_type} | Device: {sensor_or_device} | Reason: {reason}")
                
            except Exception as e:
                print(f"Failed to process DLQ payload: {e}")
                
    except KeyboardInterrupt:
        print("\nStopping DLQ logger early...")
        
    finally:
        consumer.close()
        
    # Generate and print the 5-minute report
    print("\n" + "="*70)
    print("                 URBANPULSE DEAD-LETTER QUEUE REPORT")
    print(f" Report Duration: {args.duration} seconds ({args.duration/60:.1f} minutes)")
    print(f" Total Logged Validation Failures: {total_errors}")
    print("="*70)
    
    if total_errors == 0:
        print(" No validation failures detected during this period.")
        print("="*70)
        return
        
    print(f"{'Error Category':<45} | {'Count':<8} | {'Percentage':<10}")
    print("-"*70)
    for err_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_errors) * 100
        print(f"{err_type:<45} | {count:<8} | {percentage:>8.2f}%")
    print("="*70)
    
    # Save the report as a markdown file for submission/documentation
    report_file_path = "dlq_report.md"
    try:
        with open(report_file_path, "w") as f:
            f.write("# UrbanPulse Dead-Letter Queue (DLQ) Analysis Report\n\n")
            f.write(f"- **Collection Duration:** {args.duration} seconds ({args.duration/60:.2f} minutes)\n")
            f.write(f"- **Total Validation Failures:** {total_errors}\n\n")
            f.write("## Error Distribution Table\n\n")
            f.write("| Error Category | Count | Percentage |\n")
            f.write("| :--- | :---: | :---: |\n")
            for err_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_errors) * 100
                f.write(f"| {err_type} | {count} | {percentage:.2f}% |\n")
            f.write("\n## Sample Validation Errors\n")
            f.write("- **AQI Sensor Failures:** Missing/Null values from ambient monitors.\n")
            f.write("- **Bus GPS Boundary Errors:** Latitudes/Longitudes generated outside the MetroConnect bounding box.\n")
            f.write("- **Smart Meter Anomalies:** High voltage levels (>260V) or negative consumption metrics.\n")
        print(f"DLQ report saved to '{report_file_path}'")
    except Exception as e:
        print(f"Failed to write DLQ report to file: {e}")

if __name__ == '__main__':
    main()
