# Water Network AI Monitoring — Streaming Data Pipeline

A Python TCP client that connects to a simulated smart-water-network AI monitoring
server (`water_ai_stream_server.py`), consumes a live JSON record stream, validates
each reading, splits clean vs. bad records, and emits a rolling summary report.

This repo was built for the **Hamrah Aval Academy — Data Engineer Bootcamp 2**
entrance challenge.

## What it does

1. **Ingests a live stream** — opens a TCP connection to `localhost:9034`, where the
   provided server pushes one newline-delimited JSON record roughly every 25 ms
   (~40 records/sec under normal conditions).
2. **Handles TCP framing** — since TCP is a byte stream with no built-in message
   boundaries, incoming bytes are buffered and split on `\n` before being parsed
   as JSON.
3. **Validates every record** against the schema below and collects *all* errors
   for a record (a record can fail more than one rule at once).
4. **Splits output**:
   - `clean_readings.csv` — records that pass every validation rule.
   - `bad_readings.csv` — records that fail one or more rules (plus the list of
     errors, or `invalid_json` if the line couldn't even be parsed).
5. **Reports every 20 seconds** to `real_time_reports.csv`: total records seen,
   clean count, bad count, and number of distinct active stations in that window.

## Data schema

| Field | Rule |
|---|---|
| `reading_id` | positive integer |
| `station_id` | one of `ST-01` … `ST-10` |
| `model_name` | `leak_detector`, `pressure_drop_predictor`, or `demand_forecaster` |
| `predicted_label` | depends on `model_name` — `leak`/`normal`, `drop`/`stable`, or an integer `1–5000` |
| `confidence_score` | float in `[0, 1]` |
| `response_time_ms` | positive number |
| `timestamp` | a valid ISO-8601 timestamp |

## Repo contents

| File | Purpose |
|---|---|
| `water_ai_stream_server.py` | Provided simulator — generates the record stream, including a randomized corruption rate and (optional) model drift over time. Not part of the deliverable, just the data source. |
| `solution.ipynb` | The client: TCP framing (`stream_records`), validation (`validate`), and the ingest/report loop (`main`). |
| `clean_readings.csv` / `bad_readings.csv` / `real_time_reports.csv` | Output produced by a run (sample output included). |
| `README.md` | This file. |

## How to run

```bash
# Terminal 1 — start the simulated stream
python3 water_ai_stream_server.py

# Terminal 2 — run the consumer (open solution.ipynb and run all cells,
# or extract main() into a .py script and run it)
jupyter notebook solution.ipynb
```

Stop the server with `Ctrl+C`; stop the consumer notebook cell the same way (it
closes the socket in a `finally` block).

## Known limitations of the current implementation

Worth being upfront about, since the challenge questions below ask about exactly
these scaling/reliability gaps:

- **Single connection only.** `main()` calls `stream_records()` once — it does not
  accept multiple simultaneous producers.
- **Per-row, unbuffered CSV writes**, with an explicit `flush()` on every single
  bad record — this is a lot of syscalls per second and won't hold up at high
  throughput.
- **No durability**: if the process crashes between reading a record off the
  socket and writing it to disk, that record is gone. There's no queue, WAL, or
  acknowledgment step.
- **No reconnect logic**: if the TCP connection drops, `stream_records()` simply
  ends (its `finally` closes the socket) and the generator loop exits — production
  code would retry with backoff.
- **`buffer += ...`** does repeated string concatenation, which is O(n) per
  append; fine at ~40 records/sec, not fine at 50,000/sec (see Q1 below).
