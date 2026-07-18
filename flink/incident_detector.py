import json
import math
import os
from datetime import datetime
from pyflink.common import WatermarkStrategy, Types, SimpleStringSchema, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor


# --- Helper Functions ---
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculates the distance between two points in meters using the Haversine formula."""
    R = 6371000.0  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi / 2.0)**2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2.0)**2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

# --- Flink KeyedProcessFunctions ---

class TrafficGridlockDetector(KeyedProcessFunction):
    """
    Detects traffic gridlock:
    When a junction's average wait time exceeds 180 seconds for 3 consecutive cycles.
    """
    def __init__(self):
        self.consecutive_count_state = None

    def open(self, context):
        # State to store the number of consecutive cycles exceeding 180 seconds
        state_desc = ValueStateDescriptor("consecutive_count", Types.INT())
        self.consecutive_count_state = context.get_state(state_desc)

    def process_element(self, value_json_str, ctx):
        try:
            event = json.loads(value_json_str)
            junction_id = event['junction_id']
            zone = event['zone']
            avg_wait_sec = event['avg_wait_sec']
            timestamp = event['timestamp']
            
            current_count = self.consecutive_count_state.value()
            if current_count is None:
                current_count = 0
                
            if avg_wait_sec > 180:
                current_count += 1
            else:
                current_count = 0  # Reset on a normal cycle
                
            self.consecutive_count_state.update(current_count)
            
            if current_count >= 3:
                alert = {
                    'incident_type': 'TRAFFIC_GRIDLOCK',
                    'key_id': junction_id,
                    'details': f"Junction {junction_id} in {zone} is gridlocked. Avg wait time exceeded 180 seconds for {current_count} consecutive cycles (current: {avg_wait_sec}s).",
                    'timestamp': timestamp,
                    'severity': 'CRITICAL'
                }
                yield json.dumps(alert)
                # Reset counter after triggering to avoid continuous alerts every cycle after the 3rd,
                # or keep it depending on requirements. Let's reset to avoid alerting spam.
                self.consecutive_count_state.update(0)
        except Exception as e:
            print(f"Error in TrafficGridlockDetector: {e}")


class BusBunchingDetector(KeyedProcessFunction):
    """
    Detects bus bunching:
    When two buses on the same route_id are within 200 meters of each other for more than 5 minutes.
    Keyed by route_id.
    """
    def __init__(self):
        self.bus_positions_state = None
        self.bunching_start_state = None

    def open(self, context):
        # MapState to track latest position: bus_id -> (lat, lon, timestamp)
        pos_desc = MapStateDescriptor("bus_positions", Types.STRING(), Types.TUPLE([Types.DOUBLE(), Types.DOUBLE(), Types.LONG()]))
        self.bus_positions_state = context.get_map_state(pos_desc)
        
        # MapState to track when bunching started between pairs: "bus1_bus2" -> start_timestamp
        bunch_desc = MapStateDescriptor("bunching_start", Types.STRING(), Types.LONG())
        self.bunching_start_state = context.get_map_state(bunch_desc)


    def process_element(self, value_json_str, ctx):
        try:
            event = json.loads(value_json_str)
            bus_id = event['bus_id']
            route_id = event['route_id']
            lat = event['lat']
            lon = event['lon']
            timestamp = event['timestamp']
            
            # 1. Update this bus's position in the state
            self.bus_positions_state.put(bus_id, (lat, lon, timestamp))
            
            # 2. Compare this bus's position with all other buses on the same route
            all_buses = list(self.bus_positions_state.keys())
            
            for other_id in all_buses:
                if other_id == bus_id:
                    continue
                    
                other_pos = self.bus_positions_state.get(other_id)
                if other_pos is None:
                    continue
                    
                other_lat, other_lon, other_ts = other_pos
                
                # Check if the other bus position is fresh (e.g., within last 10 minutes)
                if abs(timestamp - other_ts) > 600000:
                    continue
                    
                # Calculate distance
                dist = haversine_distance(lat, lon, other_lat, other_lon)
                
                # Create a stable sorted key for the pair
                pair_key = "_".join(sorted([bus_id, other_id]))
                
                if dist < 200.0:
                    # Buses are bunched!
                    start_ts = self.bunching_start_state.get(pair_key)
                    if start_ts is None:
                        # Mark the start of bunching
                        self.bunching_start_state.put(pair_key, timestamp)
                    else:
                        duration_ms = timestamp - start_ts
                        if duration_ms >= 300000:  # 5 minutes (300,000 ms)
                            alert = {
                                'incident_type': 'BUS_BUNCHING',
                                'key_id': route_id,
                                'details': f"Bus bunching detected on route {route_id} between {bus_id} and {other_id}. They are {dist:.1f}m apart and have been close for {duration_ms / 60000:.1f} minutes.",
                                'timestamp': timestamp,
                                'severity': 'WARNING'
                            }
                            yield json.dumps(alert)
                            # Reset bunching start time to avoid spamming alerts every event
                            self.bunching_start_state.put(pair_key, timestamp)
                else:
                    # Buses are separated, remove bunching state if existed
                    self.bunching_start_state.remove(pair_key)

                        
        except Exception as e:
            print(f"Error in BusBunchingDetector: {e}")


# --- Flink DataStream Application ---

def main():
    # Initialize Stream Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Local development parallelism
    
    # Load Flink Kafka Connector Jar into JVM classpath
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")

    
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092,localhost:29092,localhost:39092')
    
    # 1. Define Kafka Sources
    
    # Source for Air Quality Stream
    aq_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics("urbanpulse.air_quality") \
        .set_group_id("flink-aq-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
        
    # Source for Traffic Signal Stream
    traffic_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics("urbanpulse.traffic_signals") \
        .set_group_id("flink-traffic-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
        
    # Source for Bus GPS Stream
    bus_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics("urbanpulse.bus_gps") \
        .set_group_id("flink-bus-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 2. Define Kafka Sink for Incident Alerts
    alert_sink = KafkaSink.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder() \
                .set_topic("urbanpulse.incidents") \
                .set_value_serialization_schema(SimpleStringSchema()) \
                .build()
        ) \
        .build()

    # 3. Create Streams and assign Watermark Strategy
    # We parse the timestamp from the JSON values to assign event time
    
    def extract_timestamp(json_str):
        try:
            return json.loads(json_str)['timestamp']
        except:
            return int(datetime.utcnow().timestamp() * 1000)
            
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
        .with_timestamp_assigner(lambda event, timestamp: extract_timestamp(event))

    aq_stream = env.from_source(aq_source, watermark_strategy, "AQI Source")
    traffic_stream = env.from_source(traffic_source, watermark_strategy, "Traffic Source")
    bus_stream = env.from_source(bus_source, watermark_strategy, "Bus GPS Source")

    # --- Processing logic ---
    
    # Incident (a): AQI Emergency (AQI > 300)
    # Filter and map directly
    def filter_aqi_emergencies(json_str):
        try:
            event = json.loads(json_str)
            return event.get('aqi') is not None and event['aqi'] > 300
        except:
            return False
            
    def map_aqi_alert(json_str):
        event = json.loads(json_str)
        alert = {
            'incident_type': 'AQI_EMERGENCY',
            'key_id': event['sensor_id'],
            'details': f"Air quality emergency detected in {event['zone']}. Sensor {event['sensor_id']} reports hazardous AQI: {event['aqi']}.",
            'timestamp': event['timestamp'],
            'severity': 'CRITICAL'
        }
        return json.dumps(alert)

    aqi_alerts = aq_stream \
        .filter(filter_aqi_emergencies) \
        .map(map_aqi_alert, output_type=Types.STRING())

    # Incident (b): Traffic Gridlock
    # Key by junction_id and apply stateful detector
    def get_junction_key(json_str):
        return json.loads(json_str)['junction_id']
        
    gridlock_alerts = traffic_stream \
        .key_by(get_junction_key) \
        .process(TrafficGridlockDetector(), output_type=Types.STRING())

    # Incident (c): Bus Bunching
    # Key by route_id and apply stateful detector
    def get_route_key(json_str):
        return json.loads(json_str)['route_id']
        
    bunching_alerts = bus_stream \
        .key_by(get_route_key) \
        .process(BusBunchingDetector(), output_type=Types.STRING())

    # Union all alert streams and send to Kafka incidents topic
    union_alerts = aqi_alerts \
        .union(gridlock_alerts) \
        .union(bunching_alerts)
        
    union_alerts.sink_to(alert_sink)
    union_alerts.print()  # Output to standard log

    print("Flink Incident Detector Stream Graph created. Starting execution...")
    env.execute("UrbanPulse Real-Time Incident Detector")

if __name__ == '__main__':
    main()
