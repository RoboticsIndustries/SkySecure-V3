# Running SkySecure v3

This covers three things: running L1 by itself right now, running each layer
independently as you build it out, and running the whole stack once it's
all wired together.

Status as of this doc: **L1 is real (multi-source cross-validation), L2/L3
exist as standalone modules but aren't wired into the live API path yet,
L4/L5 are stubs pending hardware.**

---

## 0. One-time setup

```bash
git clone <your-new-repo-url>
cd SkySecure-v3
python3.12 -m venv venv        # 3.11/3.12, not 3.14 — some deps lack wheels for it
source venv/bin/activate
pip install -r requirements.txt
```

You don't need Docker/Kafka/Postgres running for anything in Section 1 below —
those are only required once you're testing the fused pipeline in Section 3.

---

## 1. Running layers individually (do this now, while building)

### L1 — Multi-Source Cross-Validation

Standalone, no infra needed. Hits live OpenSky/adsb.lol/adsb.fi directly.

```bash
source venv/bin/activate
python processing/cross_source_validator.py
```

Edit the `icao`/`lat`/`lon` in `_smoke_test()` at the bottom of that file to a
real live aircraft first — grab one from:

```bash
curl -s "https://opensky-network.org/api/states/all" | python3 -m json.tool | head -30
```

Pick an `icao24` and its `lat`/`lon` from the output, drop them in, rerun.
You should get a dict back with `verdict`, `max_disagreement_m`, and which
sources responded. If `sources_used` only ever has 1-2 entries, that's the
point-radius query on adsb.lol/adsb.fi not finding the aircraft — try a
busier airspace (e.g. near a major airport) or double check their current
endpoint format hasn't changed.

**Baseline run** (do this before trusting any threshold): let it loop against
~50-100 real aircraft for an hour, log every `max_disagreement_m`, and look
at the distribution before you trust the 1.5km/5km cutoffs in the paper.

### L2 — Kinematic Anomaly Detection

```bash
python anomaly/detector.py
```

This runs its own `__main__` block — check what it prints against a few
known-legitimate tracks (steady cruise, normal climb/descent) to confirm it
doesn't flag ordinary flight as anomalous before testing it against anything
adversarial.

### L3 — NIC/NACp Integrity Clustering

```bash
python anomaly/enhanced_detector.py
```

Also has its own `__main__`. Note `EnhancedAnomalyDetector.__init__` takes an
optional `tdoa_validator` — right now that's the *old* simulated hardware-TDOA
validator (`processing/tdoa_validator.py`), not the new L1 cross-validator.
That's a real gap: L2/L3 and L1 aren't sharing data yet. Worth fixing before
you call this an integrated pipeline (see Section 2).

### L4 / L5

Nothing to run — these are architected but hardware-gated/stubbed. Don't
claim runtime results for these in the paper; describe them as designed,
pending deployment.

---

## 2. Combining layers

This is the part that isn't built yet, so treat this as the plan rather than
something you can run today:

1. **Wire L1 output into L2/L3's input.** Right now `EnhancedAnomalyDetector`
   expects a `tdoa_validator`-shaped object. Either adapt it to accept
   `CrossSourceValidator` results (`l1_result.to_dict()`), or write a thin
   adapter class that presents the same interface L2/L3 expect but is backed
   by real L1 data.
2. **Define a combined verdict.** Decide how L1's `SPOOFED`/`UNCERTAIN`/
   `LEGITIMATE` and L2/L3's anomaly scores merge into one risk number — e.g.
   an aircraft flagged `UNCERTAIN` by L1 *and* kinematically implausible by
   L2 is higher-confidence than either alone. Right now `api/main.py` only
   escalates risk when L1 says `SPOOFED`; L2/L3 flags aren't feeding into
   that same risk band yet.
3. **Test the combination against known-legitimate traffic first** — same
   principle as L1 alone: confirm the combined system doesn't flag normal
   aircraft before you look for whether it catches anything real.

I can build the actual adapter/fusion code for this whenever you're ready —
it's a real coding task, not just config.

---

## 3. Running the whole stack

Once layers are combined, this is how the full system comes up (infra is
already defined in `docker-compose.yml`):

```bash
# 1. Bring up infra (Kafka, Zookeeper, Redis, Postgres)
docker compose up -d zookeeper kafka kafka-init redis postgres

# 2. Confirm topics exist
docker compose logs kafka-init

# 3. Set OpenSky credentials (anonymous works but is rate-limited harder)
export OPENSKY_USERNAME=your_username
export OPENSKY_PASSWORD=your_password

# 4. Start the API (this is what runs L1 cross-validation live, per Section 1
#    of api/main.py's broadcast loop)
uvicorn api.main:app --reload --port 8000

# 5. Check it's alive and L1 is active
curl -s http://localhost:8000/healthz | python3 -m json.tool
curl -s http://localhost:8000/api/l1/sources | python3 -m json.tool

# 6. Watch live aircraft + L1 status
curl -s http://localhost:8000/api/aircraft?min_risk=0 | python3 -m json.tool | head -50
```

If you have a frontend (`frontend/`), that's a separate process:

```bash
cd frontend
npm install
npm run dev
```

### Sanity checks once it's all up

- `/healthz` should show `l1_enabled: true`
- Watch server logs for `L1 cross-validation failed for ...` warnings — if
  every request fails, it's almost always rate-limiting or a changed API
  format upstream, not your code
- Pull a known-real aircraft via `/api/l1/validate?icao=...&lat=...&lon=...`
  and confirm it verdicts `LEGITIMATE`
- Watch `/api/alerts` over time — if it's constantly firing on ordinary
  traffic, your thresholds are too tight and need recalibrating against the
  baseline data from Section 1

---

## What's honestly not done yet

- L1 thresholds are reasoned estimates, not measured from a baseline run
- L2/L3 aren't fused with L1 output
- No labeled spoofing ground truth to validate detection *accuracy* against —
  everything above tests that the mechanism runs, not that it catches real
  spoofing
- L4/L5 have no code path at all, by design, pending hardware