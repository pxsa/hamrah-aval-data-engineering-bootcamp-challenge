# Data Engineering Bootcamp Challenge
import time
import random
import socket
import json
import argparse
import threading
import sys
from datetime import datetime
from itertools import count
# ---------------------------------------------------------------------------
# Base configuration
# ---------------------------------------------------------------------------
HOST = 'localhost'
PORT = 9034
INTERVAL_SECONDS = 0.025

CORRUPTION_PROBABILITY_RANGE = (0.015, 0.03)

MODEL_NAMES = ["leak_detector", "pressure_drop_predictor", "demand_forecaster"]

VALID_LABELS = {
    "leak_detector": ["leak", "normal"],
    "pressure_drop_predictor": ["drop", "stable"],
    "demand_forecaster": None,  
}

DEMAND_RANGE = (1, 5000)  

STATION_IDS = [f"ST-{i:02d}" for i in range(1, 11)]  # ST-01 .. ST-10

MODEL_BASELINE = {
    "leak_detector": {"confidence_mean": 0.88, "confidence_std": 0.07, "latency_mean": 35, "latency_std": 7},
    "pressure_drop_predictor": {"confidence_mean": 0.78, "confidence_std": 0.09, "latency_mean": 55, "latency_std": 10},
    "demand_forecaster": {"confidence_mean": 0.70, "confidence_std": 0.10, "latency_mean": 80, "latency_std": 15},
}

BASE_LEAK_RATE = 0.08  

DRIFT_CYCLE_SECONDS = 300


def get_drift_state(elapsed_since_start, enable_drift):
    if not enable_drift:
        return {
            "leak_confidence_penalty": 0.0,
            "demand_latency_multiplier": 1.0,
            "leak_rate": BASE_LEAK_RATE,
        }

    phase_time = elapsed_since_start % DRIFT_CYCLE_SECONDS

    leak_confidence_penalty = 0.0
    demand_latency_multiplier = 1.0
    leak_rate = BASE_LEAK_RATE

    if phase_time < 75:
        pass  # Normal state
    elif phase_time < 160:
        progress = (phase_time - 75) / 85.0  
        leak_confidence_penalty = 0.35 * progress
    elif phase_time < 230:
        progress = (phase_time - 160) / 70.0
        demand_latency_multiplier = 1.0 + 2.5 * progress  
    else:
        progress = (phase_time - 230) / 70.0
        leak_rate = BASE_LEAK_RATE + 0.5 * progress  

    return {
        "leak_confidence_penalty": leak_confidence_penalty,
        "demand_latency_multiplier": demand_latency_multiplier,
        "leak_rate": leak_rate,
    }


def clamp(value, low, high):
    return max(low, min(high, value))


_reading_id_lock = threading.Lock()
_reading_id_counter = count(1)


def next_reading_id():
    with _reading_id_lock:
        return next(_reading_id_counter)


def generate_random_record(current_time, elapsed_since_start, enable_drift, corruption_probability):
    """Generate a random record that is valid or, with some probability, corrupted."""
    is_corrupted = random.random() < corruption_probability
    drift_state = get_drift_state(elapsed_since_start, enable_drift)

    if is_corrupted:
        if random.random() < 0.3:
            reading_id = random.choice([random.randint(-1000, 0), ""])
        else:
            reading_id = next_reading_id()

        station_id = random.choice(STATION_IDS + ["", "ST-99", "unknown"])
        model_name = random.choice(MODEL_NAMES + ["", "unknown_model"])
        timestamp = (
            current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            if random.random() > 0.2 else ""
        )
        predicted_label = random.choice([
            "invalid_label", "", 99999, -1, "leak", "drop", "stable", "normal"
        ])
        confidence_score = random.choice([
            round(random.uniform(-1, 0), 3),
            round(random.uniform(1.01, 2), 3),
            "",
        ])
        response_time_ms = random.choice([
            round(random.uniform(-100, -1), 1),
            "",
        ])

    else:
        reading_id = next_reading_id()
        station_id = random.choice(STATION_IDS)
        model_name = random.choice(MODEL_NAMES)
        timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        baseline = MODEL_BASELINE[model_name]

        if model_name == "demand_forecaster":
            predicted_label = random.randint(*DEMAND_RANGE)
            latency_mean = baseline["latency_mean"] * drift_state["demand_latency_multiplier"]
            confidence_score = round(
                clamp(random.gauss(baseline["confidence_mean"], baseline["confidence_std"]), 0, 1), 3
            )
            response_time_ms = round(max(1, random.gauss(latency_mean, baseline["latency_std"])), 1)

        elif model_name == "leak_detector":
            confidence_mean = clamp(
                baseline["confidence_mean"] - drift_state["leak_confidence_penalty"], 0.05, 1
            )
            predicted_label = "leak" if random.random() < drift_state["leak_rate"] else "normal"
            confidence_score = round(
                clamp(random.gauss(confidence_mean, baseline["confidence_std"]), 0, 1), 3
            )
            response_time_ms = round(max(1, random.gauss(baseline["latency_mean"], baseline["latency_std"])), 1)

        else:  # pressure_drop_predictor
            predicted_label = random.choice(VALID_LABELS[model_name])
            confidence_score = round(
                clamp(random.gauss(baseline["confidence_mean"], baseline["confidence_std"]), 0, 1), 3
            )
            response_time_ms = round(max(1, random.gauss(baseline["latency_mean"], baseline["latency_std"])), 1)

    return {
        "reading_id": reading_id,
        "station_id": station_id,
        "model_name": model_name,
        "predicted_label": predicted_label,
        "confidence_score": confidence_score,
        "response_time_ms": response_time_ms,
        "timestamp": timestamp,
    }


def handle_client(client_socket, address, enable_drift, verbose, log_every, corruption_probability):
    print(f"Client connected from {address}")
    client_start_time = time.time()
    sent_count = 0
    try:
        while True:
            current_time = datetime.now()
            elapsed_since_start = time.time() - client_start_time
            record = generate_random_record(
                current_time, elapsed_since_start, enable_drift, corruption_probability
            )
            record_json = json.dumps(record) + '\n'  
            client_socket.sendall(record_json.encode('utf-8'))
            sent_count += 1

            if verbose:
                sys.stderr.write(
                    f"[{current_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Sent 1 record to {address}\n"
                )
                sys.stderr.flush()
            elif sent_count % log_every == 0:
                sys.stderr.write(f"[{address}] Sent {sent_count} records so far\n")
                sys.stderr.flush()

            time.sleep(INTERVAL_SECONDS)
    except (ConnectionError, BrokenPipeError):
        print(f"Client {address} disconnected (total sent: {sent_count})")
    finally:
        client_socket.close()


def start_server(enable_drift, verbose, log_every, seed=None):
    if seed is not None:
        random.seed(seed)
    corruption_probability = random.uniform(*CORRUPTION_PROBABILITY_RANGE)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print(f"Server started. Listening on {HOST}:{PORT} for incoming connections...")
    print(f"Drift injection: {'ENABLED' if enable_drift else 'disabled'}")
    print(f"Corruption rate for this run: {corruption_probability * 100:.2f}%")
    if seed is not None:
        print(f"Random seed: {seed} (more reproducible for single-client runs)")
    sys.stderr.write("Waiting for client connections...\n")

    try:
        while True:
            client_socket, address = server_socket.accept()
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, address, enable_drift, verbose, log_every, corruption_probability),
            )
            client_thread.daemon = True
            client_thread.start()
    except KeyboardInterrupt:
        sys.stderr.write("\nServer stopped by user.\n")
    finally:
        server_socket.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart water network AI monitoring stream server")
    parser.add_argument("--disable-drift", action="store_true",
                        help="Disable the controlled model-behavior drift schedule (drift is ON by default).")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducible runs (useful for evaluation).")
    parser.add_argument("--verbose", action="store_true",
                        help="Print a line for every single record sent (very noisy).")
    parser.add_argument("--log-every", type=int, default=250,
                        help="When not verbose, print a summary line every N records per client.")
    args = parser.parse_args()

    if args.log_every <= 0:
        parser.error("--log-every must be a positive integer")

    print("Starting smart water network AI monitoring stream server.")
    print(f"Data will be streamed over TCP on port {PORT}.")
    print(f"Every {INTERVAL_SECONDS} seconds, 1 new record will be sent to connected clients.")
    print("A small, randomized share of records will be corrupted (invalid or missing values);")
    print("the exact rate for this run is printed once the server starts listening.")
    print("Note: model behavior gradually changes over the course of the run (drift is ON by default).")

    start_server(
        enable_drift=not args.disable_drift,
        verbose=args.verbose,
        log_every=args.log_every,
        seed=args.seed,
    )
