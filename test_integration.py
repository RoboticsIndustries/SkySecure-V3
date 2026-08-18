"""
Integration Test: TDOA Validator + Enhanced Anomaly Detector
=============================================================
Tests synthetic timing fixtures through the validator and anomaly detector.
This does not validate live receiver hardware or real TDOA.
"""

import sys
sys.path.append('.')

from processing.tdoa_validator import TDOAValidator, create_test_receivers, Position, Receiver
from anomaly.enhanced_detector import EnhancedAnomalyDetector
import json

def print_header(text):
    print("\n" + "=" * 70)
    print(text.center(70))
    print("=" * 70 + "\n")

def main():
    print_header("SkySecure V3 Synthetic TDOA Integration Test")
    
    # Initialize TDOA validator
    print("📡 Initializing receiver network...")
    receivers = create_test_receivers()
    tdoa_validator = TDOAValidator(
        receivers=receivers,
        detection_threshold_meters=500
    )
    
    print(f"✅ {len(receivers)} receivers online:")
    for r in receivers:
        print(f"   - {r.id}")
    
    # Initialize enhanced anomaly detector
    print("\n🔍 Initializing enhanced anomaly detector with TDOA...")
    detector = EnhancedAnomalyDetector(tdoa_validator=tdoa_validator)
    print("✅ Anomaly detector ready (4-layer detection)")
    
    # Test Case 1: Legitimate Aircraft
    print_header("TEST 1: Legitimate Aircraft")
    
    legit_position = Position.from_lat_lon_alt(39.8717, -75.2411, 3000)
    legit_t0 = 1000.0
    legit_receive_times = {
        rid: legit_t0 + legit_position.distance_to(receiver.position) / 299792458
        for rid, receiver in tdoa_validator.receivers.items()
    }

    legit_track = {
        "icao": "AAL123",
        "lat": 39.8717,
        "lon": -75.2411,
        "alt_baro": 3000,
        "alt_geo": 3020,
        "velocity": 250,
        "vertical_rate": 500,
        "heading": 90,
        "receive_times": legit_receive_times,
    }
    
    print(f"✈️  Aircraft: {legit_track['icao']}")
    print(f"   Position: {legit_track['lat']:.4f}°N, {legit_track['lon']:.4f}°W")
    print(f"   Altitude: {legit_track['alt_baro']} ft")
    print(f"   Speed: {legit_track['velocity']} kt")
    
    # Run TDOA validation
    print("\n🔍 Running TDOA validation...")
    tdoa_result = tdoa_validator.validate_position(
        icao=legit_track["icao"],
        claimed_lat=legit_track["lat"],
        claimed_lon=legit_track["lon"],
        claimed_alt=legit_track["alt_baro"],
        receive_times=legit_track["receive_times"]
    )
    
    print(f"   Verdict: {tdoa_result.verdict}")
    print(f"   Max position error: {tdoa_result.max_error_meters:.1f} m")
    print(f"   Confidence: {tdoa_result.confidence:.2%}")
    
    # Run full anomaly detection
    print("\n🔍 Running full anomaly detection...")
    anomaly_score = detector.calculate_overall_score(
        icao=legit_track["icao"],
        lat=legit_track["lat"],
        lon=legit_track["lon"],
        alt_baro=legit_track["alt_baro"],
        alt_geo=legit_track["alt_geo"],
        velocity=legit_track["velocity"],
        vertical_rate=legit_track["vertical_rate"],
        heading=legit_track["heading"],
        receive_times=legit_track["receive_times"]
    )
    
    print(f"\n📊 Multi-Layer Anomaly Scores:")
    print(f"   Layer 1 (Physics):")
    print(f"      - Impossible speed: {anomaly_score.impossible_speed:.2f}")
    print(f"      - Impossible climb: {anomaly_score.impossible_climb:.2f}")
    print(f"      - Altitude conflict: {anomaly_score.altitude_conflict:.2f}")
    print(f"   Layer 2 (Behavioral):")
    print(f"      - Unusual trajectory: {anomaly_score.unusual_trajectory:.2f}")
    print(f"   Layer 3 (Integrity):")
    print(f"      - NIC/NACp integrity: {anomaly_score.integrity_metadata:.2f}")
    print(f"   Layer 4 (TDOA):")
    print(f"      - TDOA spoofing score: {anomaly_score.tdoa_spoofing:.2f}")
    print(f"      - Position error: {anomaly_score.tdoa_position_error:.1f} m")
    print(f"\n   🎯 OVERALL SCORE: {anomaly_score.overall_score:.2f}")
    print(f"   🚦 THREAT LEVEL: {anomaly_score.threat_level}")

    assert tdoa_result.verdict == "LEGITIMATE", (
        f"legitimate fixture was classified {tdoa_result.verdict} "
        f"with {tdoa_result.max_error_meters:.1f}m error"
    )
    assert anomaly_score.threat_level == "LOW", (
        f"legitimate fixture received {anomaly_score.threat_level} threat level"
    )
    
    if anomaly_score.threat_level == "LOW":
        print("\n   ✅ Aircraft verified as LEGITIMATE")
    
    # Test Case 2: Spoofed Aircraft
    print_header("TEST 2: Spoofed Aircraft (Position Spoofing)")
    
    # Spoofer claims to be at position A, but signal timing reveals position B
    actual_position = Position.from_lat_lon_alt(39.9000, -75.1500, 500)
    claimed_position = (40.0500, -75.4000, 8000)
    
    # Calculate what the receive times WOULD be from actual position
    import time
    t0 = time.time()
    spoofed_receive_times = {}
    for rid, receiver in tdoa_validator.receivers.items():
        distance = actual_position.distance_to(receiver.position)
        propagation_time = distance / 299792458  # speed of light
        spoofed_receive_times[rid] = t0 + propagation_time
    
    spoofed_track = {
        "icao": "SPOOF1",
        "lat": claimed_position[0],  # Claimed position
        "lon": claimed_position[1],
        "alt_baro": claimed_position[2],
        "alt_geo": claimed_position[2],
        "velocity": 800,  # High but possible
        "vertical_rate": 5000,
        "heading": 180,
        "receive_times": spoofed_receive_times  # Times from ACTUAL position
    }
    
    print(f"✈️  Aircraft: {spoofed_track['icao']}")
    print(f"   CLAIMED Position: {spoofed_track['lat']:.4f}°N, {spoofed_track['lon']:.4f}°W, {spoofed_track['alt_baro']} ft")
    print(f"   ACTUAL Position: 39.9000°N, 75.1500°W, 500 ft")
    print(f"   Speed: {spoofed_track['velocity']} kt")
    
    # Run TDOA validation
    print("\n🔍 Running TDOA validation...")
    tdoa_result = tdoa_validator.validate_position(
        icao=spoofed_track["icao"],
        claimed_lat=spoofed_track["lat"],
        claimed_lon=spoofed_track["lon"],
        claimed_alt=spoofed_track["alt_baro"],
        receive_times=spoofed_track["receive_times"]
    )
    
    print(f"   Verdict: {tdoa_result.verdict}")
    print(f"   Max position error: {tdoa_result.max_error_meters:.1f} m ({tdoa_result.max_error_meters/1000:.1f} km)")
    print(f"   Confidence: {tdoa_result.confidence:.2%}")
    
    # Run full anomaly detection
    print("\n🔍 Running full anomaly detection...")
    anomaly_score = detector.calculate_overall_score(
        icao=spoofed_track["icao"],
        lat=spoofed_track["lat"],
        lon=spoofed_track["lon"],
        alt_baro=spoofed_track["alt_baro"],
        alt_geo=spoofed_track["alt_geo"],
        velocity=spoofed_track["velocity"],
        vertical_rate=spoofed_track["vertical_rate"],
        heading=spoofed_track["heading"],
        receive_times=spoofed_track["receive_times"]
    )
    
    print(f"\n📊 Multi-Layer Anomaly Scores:")
    print(f"   Layer 1 (Physics):")
    print(f"      - Impossible speed: {anomaly_score.impossible_speed:.2f}")
    print(f"      - Impossible climb: {anomaly_score.impossible_climb:.2f}")
    print(f"      - Altitude conflict: {anomaly_score.altitude_conflict:.2f}")
    print(f"   Layer 2 (Behavioral):")
    print(f"      - Unusual trajectory: {anomaly_score.unusual_trajectory:.2f}")
    print(f"   Layer 3 (Integrity):")
    print(f"      - NIC/NACp integrity: {anomaly_score.integrity_metadata:.2f}")
    print(f"   Layer 4 (TDOA):")
    print(f"      - TDOA spoofing score: {anomaly_score.tdoa_spoofing:.2f}")
    print(f"      - Position error: {anomaly_score.tdoa_position_error:.1f} m")
    print(f"\n   🎯 OVERALL SCORE: {anomaly_score.overall_score:.2f}")
    print(f"   🚦 THREAT LEVEL: {anomaly_score.threat_level}")

    assert tdoa_result.verdict == "SPOOFED", (
        f"spoof fixture was classified {tdoa_result.verdict}"
    )
    assert anomaly_score.threat_level in ["MEDIUM", "HIGH", "CRITICAL"], (
        f"spoof fixture received {anomaly_score.threat_level} threat level"
    )
    
    if anomaly_score.threat_level in ["HIGH", "CRITICAL"]:
        print("\n   🚨 SPOOFING DETECTED - Aircraft marked as THREAT")
    
    # Summary
    print_header("Integration Test Summary")
    
    print("✅ Synthetic timing validation passed")
    print("✅ Enhanced anomaly scoring integration passed")
    
    print("\n📋 Integration Points Validated:")
    print("   ✓ TDOA validator can be imported")
    print("   ✓ Enhanced detector accepts TDOA validator")
    print("   ✓ TDOA scores integrate with anomaly scoring")
    print("   ✓ Legitimate aircraft verified correctly")
    print("   ✓ Synthetic spoof fixture detected correctly")
    print("\n⚠ This test is not evidence of real TDOA capability; physical,")
    print("  synchronized receivers are still required for that claim.")
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    main()
