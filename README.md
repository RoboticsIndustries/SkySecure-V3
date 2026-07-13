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
| v2 | ACARS Message Anomaly Detection | 🔧 In Development (Target: Aug 2026) |
| v3 | ML-Based Trajectory Fingerprinting | 📋 Planned |
| v4 | Multi-Receiver Sensor Fusion | 📋 Planned |

---

## Architecture

```
SkySecure-v2/
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
