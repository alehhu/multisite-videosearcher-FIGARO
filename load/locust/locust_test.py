import json
import os
import subprocess
import numpy as np
import time
from locust import HttpUser, task, between, events
import gevent
from gevent import sleep
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# === Constants ===
HOST            = os.getenv("MINIKUBE_IP", "192.168.49.2")  # Minikube node IP
PORT            = int(os.getenv("GATEWAY_PORT", "30080"))   # Gateway NodePort
WORKFLOW_NAME   = "videosearcher_workflow"

# WORKFLOW_NAME   = "deepspeech_workflow"    # Name of the workflow to invoke

# List to store request timestamps
request_timestamps = []

_pod_logger_proc = None

# # === Precompute the RPS pattern ===
np.random.seed(22222121)  # for reproducibility

SECONDS = 60 * 60
BASE_RPS = 0
AMPLITUDE = 0.25
NOISE_STD   = 0.01

t = np.linspace(0, 3 * np.pi, SECONDS)
rps_pattern = BASE_RPS + AMPLITUDE * np.sin(t) + np.random.normal(0, NOISE_STD, SECONDS)
rps_pattern[rps_pattern < 0] = 0
print(len(rps_pattern))



# Save the workload to a CSV file
workload_file = 'workloadProfile.csv'
df = pd.DataFrame({'Time': range(SECONDS), 'RPS': (rps_pattern).astype(float)})
df.to_csv(workload_file, index=False, sep=';')

# Plot the RPS over time
plt.figure(figsize=(10, 6))
plt.plot(df['Time'], df['RPS'], label='RPS')
plt.title('RPS Over Time')
plt.xlabel('Time (seconds)')
plt.ylabel('Requests Per Second (RPS)')
plt.legend()
plt.grid(True)
plt.savefig("RPS_Over_Time.png")

# Function to log request with timestamp
def log_request(timestamp, response_time):
    """Log the request timestamp and response time."""
    request_timestamps.append((timestamp, response_time))

# Function to save request timestamps to CSV
def save_request_log():
    """Save the request timestamps to a CSV file."""
    timestamps_df = pd.DataFrame(request_timestamps, columns=['Timestamp', 'ResponseTime'])
    timestamps_df.to_csv('request_timestamps.csv', index=False)

# Register event handler to save timestamps when test ends
@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    save_request_log()
    if _pod_logger_proc is not None:
        _pod_logger_proc.terminate()

class SinusoidalUser(HttpUser):
    wait_time = between(0, 0)  # we control timing manually
    host = f"http://{HOST}:{PORT}"

    def on_start(self):
        self.second = 0

    @task
    def post_task(self):
        pass  # We override the run loop

    def run(self):
        self.environment.runner.greenlet.spawn(self.sinusoidal_loop)

    def sinusoidal_loop(self):
        headers = {'Content-Type': 'application/json'}
        body = {
            "params": {"dir": "example"},
            "CanDoOffloading": False,
            "Async": False
        }

        # Generate all request times upfront using thinning method for non-homogeneous Poisson process
        all_request_times = self.generate_nonhomogeneous_poisson_times()

        print(f"Generated {len(all_request_times)} total requests over {SECONDS} seconds")

        # Analyze the distribution of requests over time (for debugging)
        if len(all_request_times) > 0:
            self.analyze_request_distribution(all_request_times)

        # Schedule all requests
        start_time = time.time()
        global _pod_logger_proc
        _pod_logger_proc = subprocess.Popen(["python3", "pod_logger.py", "pod_counts.csv"])

        for request_time in all_request_times:
            if request_time < SECONDS:  # Only schedule requests within our time window
                gevent.spawn_later(
                    request_time,
                    self.make_request,
                    headers=headers,
                    body=body
                )

        # Wait for the full duration
        sleep(SECONDS)

        # Save the timestamps to CSV at the end
        save_request_log()
        self.environment.runner.quit()

    def analyze_request_distribution(self, request_times):
        """Analyze the distribution of requests to verify it matches the expected pattern."""
        # Check for clustering by analyzing inter-arrival times
        if len(request_times) > 1:
            sorted_times = np.sort(request_times)
            inter_arrivals = np.diff(sorted_times)

            print(f"\nInter-arrival time analysis:")
            print(f"  Min inter-arrival: {np.min(inter_arrivals):.4f}s")
            print(f"  Max inter-arrival: {np.max(inter_arrivals):.4f}s")
            print(f"  Mean inter-arrival: {np.mean(inter_arrivals):.4f}s")
            print(f"  Std inter-arrival: {np.std(inter_arrivals):.4f}s")

            # Check for very short intervals (potential clustering)
            short_intervals = inter_arrivals[inter_arrivals < 0.1]  # < 100ms
            if len(short_intervals) > 0:
                print(f"  WARNING: {len(short_intervals)} intervals < 0.1s detected (clustering!)")
                print(f"  Shortest intervals: {sorted(short_intervals)[:5]}")

        # Analyze with multiple bin sizes
        for bin_size in [10, 60]:  # 10-second and 60-second bins
            num_bins = int(SECONDS / bin_size)
            bins = np.linspace(0, SECONDS, num_bins + 1)
            hist, _ = np.histogram(request_times, bins=bins)
            rates = hist / bin_size

            print(f"\nRequest rate analysis (per {bin_size}s bins):")
            if bin_size == 10:
                # For 10s bins, show first 20 bins and find peaks
                for i, rate in enumerate(rates[:20]):
                    start_time = i * bin_size
                    end_time = (i + 1) * bin_size
                    expected_avg = np.mean(rps_pattern[start_time:end_time])
                    marker = " ⚠️" if rate > expected_avg * 1.5 else ""
                    print(f"  Bin {i:2d} ({start_time:3d}-{end_time:3d}s): {rate:.4f} RPS (expected: {expected_avg:.4f}){marker}")

                max_rate_10s = np.max(rates)
                print(f"  Max 10s rate: {max_rate_10s:.4f} RPS")
            else:
                # For 60s bins, show first 10
                for i, rate in enumerate(rates[:10]):
                    start_time = i * bin_size
                    end_time = (i + 1) * bin_size
                    expected_avg = np.mean(rps_pattern[start_time:end_time])
                    print(f"  Bin {i:2d} ({start_time:4d}-{end_time:4d}s): {rate:.4f} RPS (expected: {expected_avg:.4f})")

        max_expected_rate = np.max(rps_pattern)
        print(f"\nMax expected rate: {max_expected_rate:.4f} RPS")

    def generate_nonhomogeneous_poisson_times(self):
        """
        Generate request times for a non-homogeneous Poisson process using the thinning method.
        This properly handles variable RPS including values < 1.
        """
        # Find maximum rate for thinning
        max_rps = np.max(rps_pattern)
        print(f"Max RPS in pattern: {max_rps:.4f}")
        print(f"Min RPS in pattern: {np.min(rps_pattern):.4f}")
        print(f"Mean RPS in pattern: {np.mean(rps_pattern):.4f}")

        if max_rps <= 0:
            return []

        # Generate homogeneous Poisson process with rate = max_rps
        # Expected number of events in homogeneous process
        expected_events = max_rps * SECONDS
        print(f"Expected events in {SECONDS}s at max rate {max_rps:.4f}: {expected_events:.1f}")

        # Generate more events than expected to account for thinning
        safety_factor = 2.0  # Increased safety factor
        num_candidates = int(expected_events * safety_factor) + 100

        # Generate inter-arrival times for homogeneous process
        inter_arrivals = np.random.exponential(1.0 / max_rps, num_candidates)
        candidate_times = np.cumsum(inter_arrivals)

        # Keep only times within our duration
        candidate_times = candidate_times[candidate_times < SECONDS]
        print(f"Generated {len(candidate_times)} candidate events")

        # Thinning: accept each event with probability = rate(t) / max_rate
        accepted_times = []
        for t in candidate_times:
            # Get the rate at time t (interpolate from our RPS pattern)
            time_index = min(int(t), len(rps_pattern) - 1)
            current_rate = rps_pattern[time_index]

            # Accept with probability current_rate / max_rps
            acceptance_prob = current_rate / max_rps
            if np.random.random() < acceptance_prob:
                accepted_times.append(t)

        accepted_times = np.array(accepted_times)
        print(f"Accepted {len(accepted_times)} events after thinning")

        # Calculate actual rate to verify
        if len(accepted_times) > 0:
            actual_total_rate = len(accepted_times) / SECONDS
            theoretical_avg_rate = np.mean(rps_pattern)
            print(f"Actual average rate: {actual_total_rate:.4f} RPS")
            print(f"Theoretical average rate: {theoretical_avg_rate:.4f} RPS")
            print(f"Rate ratio (actual/theoretical): {actual_total_rate/theoretical_avg_rate:.4f}")

        return accepted_times

    def make_request(self, headers, body):
        # Log the timestamp before making the request
        timestamp = datetime.now().timestamp()
        if WORKFLOW_NAME == "videosearcher_workflow":
            body["params"]["dir"] = str(timestamp)
        with self.client.post(
            f"/workflow/invoke/{WORKFLOW_NAME}",
            data=json.dumps(body),
            headers=headers
        ) as response:
            log_request(timestamp, response.elapsed.total_seconds() * 1000)
