import subprocess
import time
import os
import sys

def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    
    print("=== STARTING PRIORITY CONSUMER LAG DEMONSTRATION ===")
    
    # Paths to scripts
    producer_path = os.path.join(script_dir, 'producers/traffic_producer.py')
    consumer_path = os.path.join(script_dir, 'consumers/priority_consumers.py')
    
    print("Launching traffic producer...")
    # Run with -u to avoid stdout buffering
    producer_proc = subprocess.Popen(
        ['python3', '-u', producer_path], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Give the producer a moment to start writing
    time.sleep(2)
    
    # Check if producer crashed early
    producer_poll = producer_proc.poll()
    if producer_poll is not None:
        _, err = producer_proc.communicate()
        print(f"ERROR: Traffic producer crashed immediately with exit code {producer_poll}:")
        print(err)
        sys.exit(1)
    
    # Launch HIGH_PRIORITY consumer (0ms delay)
    print("Launching HIGH_PRIORITY consumer (real-time signal control)...")
    high_proc = subprocess.Popen(
        ['python3', '-u', consumer_path, '--mode', 'high'], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Launch STANDARD_PRIORITY consumer instances (3 consumers, 500ms delay)
    print("Launching 3 STANDARD_PRIORITY consumers (analytics dashboard)...")
    std_procs = []
    for i in range(1, 4):
        p = subprocess.Popen(
            ['python3', '-u', consumer_path, '--mode', 'standard', '--id', str(i)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        std_procs.append(p)
        
    print("\nRunning test simulation for 30 seconds to capture lag differentials...\n")
    
    # Poll and print stdout of high and standard consumers dynamically during the 30 seconds
    start_time = time.time()
    high_logs = []
    std_logs = []
    
    # We do non-blocking reads on stdout of the processes
    # Set stdout pipes as non-blocking
    import fcntl
    for p in [high_proc] + std_procs + [producer_proc]:
        fd = p.stdout.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        
        # Do the same for stderr to inspect any error messages
        fd_err = p.stderr.fileno()
        fl_err = fcntl.fcntl(fd_err, fcntl.F_GETFL)
        fcntl.fcntl(fd_err, fcntl.F_SETFL, fl_err | os.O_NONBLOCK)

    try:
        while time.time() - start_time < 30:
            # Try to read high priority consumer output
            try:
                line = high_proc.stdout.readline()
                if line:
                    stripped = line.strip()
                    if "Lag:" in stripped:
                        high_logs.append(stripped)
                        print(f"[LIVE] {stripped}")
            except IOError:
                pass
                
            # Try to read standard priority consumer outputs
            for idx, p in enumerate(std_procs):
                try:
                    line = p.stdout.readline()
                    if line:
                        stripped = line.strip()
                        if "Lag:" in stripped:
                            std_logs.append(stripped)
                            print(f"[LIVE] {stripped}")
                except IOError:
                    pass
            
            # Check for stderr errors from any process
            for name, p in [("Producer", producer_proc), ("High Consumer", high_proc)] + [(f"Std Consumer {i+1}", proc) for i, proc in enumerate(std_procs)]:
                try:
                    err_line = p.stderr.readline()
                    if err_line:
                        print(f"[{name} STDERR] {err_line.strip()}", file=sys.stderr)
                except IOError:
                    pass
                    
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\nTest interrupted early.")
        
    # Clean up processes
    print("\nStopping all processes...")
    producer_proc.terminate()
    high_proc.terminate()
    for p in std_procs:
        p.terminate()
        
    # Wait for processes to exit
    producer_proc.wait()
    high_proc.wait()
    for p in std_procs:
        p.wait()
        
    print("\n=== FINAL ANALYSIS SUMMARY ===")
    print(f"Total High Priority Log Entries Captured: {len(high_logs)}")
    print(f"Total Standard Priority Log Entries Captured: {len(std_logs)}")
    
    print("\n--- Last 5 High Priority Logs ---")
    for log in high_logs[-5:]:
        print(log)
        
    print("\n--- Last 5 Standard Priority Logs ---")
    for log in std_logs[-5:]:
        print(log)
        
    print("\n=== DEMONSTRATION COMPLETE ===")
    print("Notice: The HIGH_PRIORITY group processes events immediately, keeping Lag = 0 or 1.")
    print("The STANDARD_PRIORITY group, with a 500ms artificial delay, quickly accumulates lag.")

if __name__ == '__main__':
    main()
