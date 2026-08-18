<<<<<<< HEAD
# SkySecure-V3
=======
# SkySecure v2

**ADS-B Aviation Cybersecurity Platform — Spoofing Detection & Signal Authentication**

[![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-Backend-green?logo=fastapi)](https://fastapi.tiangolo.com)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active%20Development-orange)]()

---

## Overview

SkySecure v2 is a modular aviation cybersecurity platform targeting the growing threat of ADS-B signal spoofing. ADS-B (Automatic Dependent Surveillance–Broadcast) is the backbone of modern air traffic surveillance — but it transmits unauthenticated, unencrypted signals that any low-cost SDR can forge. SkySecure addresses this gap with a layered, real-time detection stack built on passive signal analysis.

The platform is validated against live aircraft data from OpenSky Network and designed to scale from a research prototype to a multi-receiver hardware deployment.

---

## Key Features

- **TDOA Spoofing Detection** — Time Difference of Arrival analysis flags position inconsistencies across receivers that a spoofed signal cannot physically satisfy
- **Simulation Environment** — Fully configurable spoofing and legitimate flight simulations for offline testing and algorithm development
- **FastAPI Backend** — Clean REST API exposing detection results, aircraft state, and alert streams
- **OpenSky Integration** — Live validation against real ADS-B traffic from the OpenSky Network
- **Modular Architecture** — Detection layers are independently versioned and pluggable; the platform is built to expand

---

## Detection Roadmap

| Layer | Method | Status |
|---|---|---|
| v1 | TDOA Position Consistency | ✅ Complete |
| v2 | ACARS Message Anomaly Detection | 🔧 In Development |
| v3 | ML-Based Trajectory Fingerprinting | 📋 Planned |
| v4 | Multi-Receiver Sensor Fusion | 📋 Planned |

Current Work:
   July 13th 2026 - L1 (TDOA) Created, tested, accuracy rate will be published after the full product is released.
   July 14th 2026 - L2 (Kinematic Anomaly Detection) In creation stage.
   Tentaive date of completion -  July 20th 2026

Action Plan:
      End Of August (August 31st) : Everything should get created, Layers 1 - 5. Should have 4 working recievers, and 3-4 working lifesize radars. 

---

## Architecture

```
SkySecure-v2/a# SkySecure v3

**Multi-layer ADS-B spoofing detection — built on live, independently-sourced data.**

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-blue?logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-backend-009688?logo=fastapi)](https://fastapi.tiangolo.com)
[![Status](https://img.shields.io/badge/status-research%20prototype-orange)]()
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

---

## The problem

ADS-B is the backbone of modern air traffic surveillance, and it is unauthenticated
by design. Every aircraft broadcasts its identity, position, altitude and velocity
in the clear on 1090 MHz, and any $30 software-defined radio can transmit a
well-formed message claiming to be an aircraft that does not exist, at a position
it is not at. Spoofing and GPS jamming have been observed operationally in conflict
airspace and near civil airports. There is no widely deployed receiver-level system
that detects it in real time.

SkySecure is an attempt at one.

---

## Detection layers

The design premise is that no single check is trustworthy on its own. Each layer
fails in a *different* way — L1 on network latency, L2 on unusual-but-legitimate
manoeuvres, L3 on avionics quirks — so agreement between layers carries far more
information than any one layer shouting.

| Layer | Method | Implementation | Live status |
|---|---|---|---|
| **L1** | Multi-source position cross-validation across independent ADS-B networks (OpenSky, adsb.lol, adsb.fi) | `processing/cross_source_validator.py` | **Active** |
| **L2** | Kinematic anomaly detection — flight-envelope violations and inter-report consistency | `anomaly/enhanced_detector.py` | **Active** |
| **L3** | NIC/NACp integrity metadata clustering | `anomaly/enhanced_detector.py` | **Degraded** — implemented, but the OpenSky feed carries no NIC/NACp, so it self-reports unavailable and is excluded from fusion (see below) |
| **L4** | RF fingerprinting | — | **Not implemented.** Requires owned receiver hardware |
| **L5** | Galileo OSNMA authentication | — | **Not implemented** |

Fusion across available layers lives in `EnhancedAnomalyDetector.assess()`.

### What L1 is, and is not

L1 is **not TDOA multilateration**, and the code says so at length in its own
docstring. True TDOA requires physically distributed, GPS-disciplined receivers
you own, so you can compare raw arrival timestamps of the *same* transmission at
each site. Without that hardware there is no honest way to produce TDOA — any
`receive_times` dictionary is fabricated.

What L1 does instead is compare the same aircraft's position as independently
computed by separate live aggregator networks. Each network runs its own receivers
and its own MLAT solver, so their position estimates are genuinely independent
measurements of the same physical aircraft. Reports are dead-reckoned to a common
timestamp before comparison, which removes most of the apparent disagreement caused
by networks polling seconds apart.

This is coarser than TDOA — positions arrive already fused by each network rather
than as raw timing, and there are 2–3 independent baselines rather than 4+. It is
described as "TDOA-equivalent, pending receiver hardware" and should be described
that way in any writeup. `processing/mlat_solver.py` holds the real ECEF/TDOA
solver for when receivers exist.

### On layers reporting "unavailable"

Every layer declares whether it actually had the data it needs. A layer without
usable input is dropped from the weighted sum and the remaining weights are
renormalized. **A layer never contributes a nonzero anomaly score on the basis of
absent data** — missing input is not evidence of spoofing.

This is not hypothetical: OpenSky's `/states/all` carries no NIC/NACp fields, so on
that feed L3 is unavailable for every aircraft. An earlier implementation returned
a 0.35 "missing metadata" penalty in that case, which would have applied a standing
spoofing score to the entire sky. Populating L3 properly means carrying integrity
metadata across from the adsb.lol/adsb.fi fetch that L1 already performs — that is
the next piece of work, and it is a real one.

---

## Repository layout

```
SkySecure-v3/
├── api/
│   └── main.py                        FastAPI app, live polling loop, WS broadcast,
│                                      L1 + L2/L3 fusion, /api/l1/* endpoints
├── processing/
│   ├── cross_source_validator.py      L1 — multi-source cross-validation (ACTIVE)
│   ├── mlat_solver.py                 True TDOA solver — awaiting receiver hardware
│   ├── tdoa_validator.py              Retired simulated-TDOA validator (tests only)
│   └── fusion_engine.py               Kalman smoothing / dup-ICAO — not yet wired
├── anomaly/
│   ├── enhanced_detector.py           L2 + L3 + fusion (ACTIVE)
│   └── detector.py                    v1 rule/statistical/LSTM stack — not wired
├── military/classifier.py             Military classification — not yet wired
├── ingestion/adsb_receiver.py         Receiver ingestion — not yet wired
├── frontend/                          React + Leaflet map UI
├── scripts/init.sql                   Postgres schema
├── docker-compose.yml                 Kafka, Zookeeper, Redis, Postgres
├── test_integration.py                Layer integration tests
├── test_live_aircraft.py              Live-traffic smoke tests
├── RUNNING.md                         How to run each layer, and the whole stack
└── INTEGRATION_GUIDE.md               Architecture and integration notes
```

Modules marked "not yet wired" contain working code that nothing currently imports.
They are kept deliberately, but they do not run, and no result in this repository
depends on them.

---

## Quickstart

```bash
git clone https://github.com/RoboticsIndustries/SkySecure-V3.git
cd SkySecure-V3
python3.12 -m venv venv          # 3.11 or 3.12 — some deps lack 3.14 wheels
source venv/bin/activate
pip install -r requirements.txt
```

### Run a layer on its own

No infrastructure needed — these hit live networks directly.

```bash
python processing/cross_source_validator.py   # L1
python anomaly/enhanced_detector.py           # L2 + L3 + fusion
```

### Run the API

```bash
export OPENSKY_USERNAME=... OPENSKY_PASSWORD=...   # anonymous works, rate-limited harder
uvicorn api.main:app --reload --port 8000

curl -s localhost:8000/healthz | python3 -m json.tool
curl -s localhost:8000/api/l1/sources | python3 -m json.tool
```

`/healthz` reports per-layer status honestly, including which layers are
`not_implemented`.

Full instructions, including the Kafka/Redis/Postgres stack, are in
[RUNNING.md](RUNNING.md).

---

## Validation status

Read this section before citing any number from this project.

**What has been tested:** that the mechanisms run against live traffic and that
they do not flag ordinary aircraft. In simulated-feed integration testing across
50 aircraft, the L2/L3 fusion path produced zero false positives on normal traffic
while catching both an injected position teleport and an L1-flagged disagreement.

**What has *not* been established:** detection *accuracy* against real spoofing.
There is no labeled ground-truth spoofing dataset behind any of this. Testing shows
the system catches the anomalies deliberately injected into it; it does not show
what fraction of real attacks it would catch, or its false-positive rate over long
runs of live traffic.

**Known open items:**

- L1 disagreement thresholds (1.5 km / 5 km) are reasoned estimates, not measured
  from a baseline distribution. A multi-hour baseline run over live traffic should
  precede trusting them.
- L3 is inert on the OpenSky feed until integrity metadata is carried over from the
  adsb.lol/adsb.fi fetch.
- L4 and L5 have no code path whatsoever, by design, pending hardware.
- The LSTM in `anomaly/detector.py` falls back to a heuristic — no trained weights
  ship with this repository, so no result here is ML-derived.

---

## Roadmap

1. Carry NIC/NACp from the L1 fetch so L3 becomes live rather than degraded.
2. Baseline L1 disagreement over live traffic; recalibrate thresholds from measured
   distribution.
3. Deploy 4 RTL-SDR receivers; retire L1 in favour of genuine TDOA via
   `processing/mlat_solver.py`.
4. L4 RF fingerprinting, once receivers exist to fingerprint with.
5. L5 Galileo OSNMA cross-check.

---

## License

MIT — see [LICENSE](LICENSE).

---

*Built by Aryan — Brandywine Cadet Squadron, Civil Air Patrol | JSHS 2026*
├── api/                   # FastAPI application & route handlers
│   └── main.py
├── detection/             # Detection layer modules
│   ├── tdoa.py            # TDOA spoofing detection (v1)
│   └── acars.py           # ACARS anomaly detection (v2, WIP)
├── simulation/            # Spoofing + legitimate flight simulators
│   ├── spoof_sim.py
│   └── flight_sim.py
├── data/                  # OpenSky integration & data pipeline
│   └── opensky_feed.py
├── tests/                 # Unit and integration tests
└── README.md
```

---

## Quickstart

### Prerequisites

- Python 3.10+
- An OpenSky Network account (free) for live data feeds

### Installation

```bash
git clone https://github.com/RoboticsIndustries/SkySecure-v2.git
cd SkySecure-v2
pip install -r requirements.txt
```

### Run the API

```bash
uvicorn api.main:app --reload
```

The API will be available at `http://localhost:8000`. Interactive docs at `/docs`.

### Run Simulations

```bash
# Simulate a spoofing scenario
python simulation/spoof_sim.py

# Simulate legitimate traffic
python simulation/flight_sim.py
```

---

## Live Validation

SkySecure v2 has been validated against real OpenSky Network aircraft data. The TDOA detection layer runs against live ADS-B feeds and flags statistically anomalous position reports in real time. Production metrics and detection performance benchmarks are documented in [`/results`](results/).

---

## Why ADS-B Security Matters

ADS-B mandates took effect in the US (2020) and are rolling out globally. Every commercial and private aircraft now broadcasts position, altitude, velocity, and identity — unencrypted and unauthenticated — on 1090 MHz. Spoofed ADS-B signals have been demonstrated in conflict zones (Ukraine, GPS jamming corridors near Iran/Iraq) and at civilian airports. Today there is no deployed, real-time system to detect these attacks at the receiver level.

SkySecure is designed to be that system.

---


## Contributing

This project is in active research and development. If you're working on ADS-B security, SDR signal processing, or aviation cybersecurity and want to collaborate, open an issue or reach out directly.


---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built by Aryan — CAP Chief Master Sergeant, Brandywine Cadet Squadron | JSHS 2026 Competitor*
>>>>>>> a33cf16 (populated files)
