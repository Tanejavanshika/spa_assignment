import subprocess
import time
import os
import sys

def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    
    print("=== STARTING ENRICHMENT & DLQ REPORT SIMULATION ===")
    print("This simulation will run for 5 minutes (300 seconds) to capture telemetry and generate the DLQ report.")
    
    # Paths to scripts
    bus_prod_path = os.path.join(script_dir, 'producers/bus_gps_producer.py')
    aq_prod_path = os.path.join(script_dir, 'producers/aq_producer.py')
    meter_prod_path = os.path.join(script_dir, 'producers/meter_producer.py')
    enrich_worker_path = os.path.join(script_dir, 'consumers/enrichment_worker.py')
    dlq_logger_path = os.path.join(script_dir, 'consumers/dlq_logger.py')
    
    background_processes = []
    
    try:
        # 1. Launch producers
        print("\nLaunching producers...")
        print(" -> Bus GPS producer...")
        background_processes.append(subprocess.Popen(['python3', bus_prod_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        
        print(" -> Air Quality producer...")
        background_processes.append(subprocess.Popen(['python3', aq_prod_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        
        print(" -> Smart Meter producer...")
        background_processes.append(subprocess.Popen(['python3', meter_prod_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        
        # 2. Launch Enrichment Worker
        print("\nLaunching Enrichment Stream-Table Join Worker...")
        background_processes.append(subprocess.Popen(['python3', enrich_worker_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        
        # Give them a couple of seconds to warm up
        time.sleep(3)
        
        # 3. Launch DLQ Logger (runs synchronously in the foreground for 5 minutes)
        print("\nLaunching DLQ Logger (monitoring for 5 minutes)...")
        # We run it synchronously so we wait for its completion
        dlq_proc = subprocess.run(
            ['python3', dlq_logger_path, '--duration', '300'],
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user.")
    finally:
        # 4. Clean up background processes
        print("\nCleaning up background producers and workers...")
        for p in background_processes:
            p.terminate()
            
        # Wait for termination
        for p in background_processes:
            p.wait()
            
        print("Cleanup complete. Simulation finished.")

if __name__ == '__main__':
    main()
