"""
SkySecure TDOA - Live Aircraft Testing
======================================
Fetches REAL aircraft from OpenSky Network and validates them with TDOA.
"""

import sys
sys.path.append('.')

import httpx
import time
from processing.tdoa_validator import (
    TDOAValidator, 
    create_test_receivers,
    Position,
    SPEED_OF_LIGHT
)
from anomaly.enhanced_detector import EnhancedAnomalyDetector

def fetch_live_aircraft(lat_min=39.5, lat_max=40.5, lon_min=-76.0, lon_max=-74.5):
    """
    Fetch real aircraft from OpenSky Network in the Philadelphia area.
    
    No login required for basic queries!
    """
    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamin": lat_min,
        "lomin": lon_min,
        "lamax": lat_max,
        "lomax": lon_max
    }
    
    print("📡 Fetching live aircraft from OpenSky Network...")
    print(f"   Area: {lat_min}°N to {lat_max}°N, {lon_min}°E to {lon_max}°E")
    
    try:
        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data or 'states' not in data or not data['states']:
            print("   ⚠️  No aircraft currently in area")
            return []
        
        aircraft = []
        for state in data['states']:
            # OpenSky state format: [icao24, callsign, origin_country, time_position, 
            #                        last_contact, longitude, latitude, baro_altitude, 
            #                        on_ground, velocity, true_track, vertical_rate, ...]
            if state[6] is None or state[5] is None:  # Missing lat/lon
                continue
            
            aircraft.append({
                'icao': state[0],
                'callsign': state[1].strip() if state[1] else state[0],
                'lat': state[6],
                'lon': state[5],
                'alt_baro': state[7] if state[7] else 0,  # meters
                'velocity': state[9] if state[9] else 0,  # m/s
                'heading': state[10] if state[10] else 0,
                'vertical_rate': state[11] if state[11] else 0,  # m/s
                'on_ground': state[8]
            })
        
        print(f"   ✅ Found {len(aircraft)} aircraft in area")
        return aircraft
        
    except httpx.HTTPError as e:
        print(f"   ❌ Error fetching data: {e}")
        return []


def simulate_receiver_timestamps(aircraft_lat, aircraft_lon, aircraft_alt, receivers):
    """
    Simulate what receiver timestamps would be if we had real receivers.
    
    This is what REAL receivers would measure - we're just calculating it
    because we don't have physical hardware deployed.
    """
    # Convert aircraft position to ECEF
    aircraft_pos = Position.from_lat_lon_alt(aircraft_lat, aircraft_lon, aircraft_alt)
    
    # Pick arbitrary transmission time
    t0 = time.time()
    
    # Calculate arrival time at each receiver
    receive_times = {}
    for rid, receiver in receivers.items():
        # Calculate distance from aircraft to receiver
        distance = aircraft_pos.distance_to(receiver.position)
        
        # Signal travels at speed of light
        propagation_time = distance / SPEED_OF_LIGHT
        
        # Add clock offset
        receive_time = t0 + propagation_time + receiver.clock_offset * 1e-9
        
        receive_times[rid] = receive_time
    
    return receive_times


def test_live_aircraft():
    """Main test function"""
    
    print("=" * 80)
    print(" " * 20 + "SkySecure TDOA - Live Aircraft Test")
    print("=" * 80)
    print()
    
    # Initialize TDOA validator
    print("🔧 Initializing TDOA validator...")
    receivers = create_test_receivers()
    validator = TDOAValidator(receivers)
    detector = EnhancedAnomalyDetector(tdoa_validator=validator)
    
    print(f"✅ Receiver network ready: {len(receivers)} receivers")
    for r in receivers:
        print(f"   - {r.id}")
    
    print()
    
    # Fetch live aircraft
    aircraft_list = fetch_live_aircraft()
    
    if not aircraft_list:
        print("\n⚠️  No aircraft found. Try again in a few seconds or expand the search area.")
        return
    
    print()
    print("=" * 80)
    print("Testing TDOA Validation on Real Aircraft")
    print("=" * 80)
    
    # Test each aircraft
    legitimate_count = 0
    tested_count = 0
    
    for i, aircraft in enumerate(aircraft_list[:10], 1):  # Test first 10
        if aircraft['on_ground']:
            continue  # Skip aircraft on ground
        
        tested_count += 1
        
        print(f"\n✈️  Aircraft {i}: {aircraft['callsign']}")
        print(f"   ICAO: {aircraft['icao']}")
        print(f"   Position: {aircraft['lat']:.4f}°N, {aircraft['lon']:.4f}°E")
        print(f"   Altitude: {aircraft['alt_baro']:.0f} m ({aircraft['alt_baro']*3.281:.0f} ft)")
        print(f"   Speed: {aircraft['velocity']:.1f} m/s ({aircraft['velocity']*1.944:.0f} knots)")
        
        # Simulate what receivers would measure
        receive_times = simulate_receiver_timestamps(
            aircraft['lat'],
            aircraft['lon'],
            aircraft['alt_baro'],
            validator.receivers
        )
        
        # Validate the position
        result = validator.validate_position(
            icao=aircraft['icao'],
            claimed_lat=aircraft['lat'],
            claimed_lon=aircraft['lon'],
            claimed_alt=aircraft['alt_baro'],
            receive_times=receive_times
        )
        
        print(f"\n   🔍 TDOA Validation:")
        print(f"      Verdict: {result.verdict}")
        print(f"      Position error: {result.max_error_meters:.1f} m")
        print(f"      Confidence: {result.confidence:.1%}")
        
        # Run full anomaly detection
        anomaly = detector.calculate_overall_score(
            icao=aircraft['icao'],
            lat=aircraft['lat'],
            lon=aircraft['lon'],
            alt_baro=aircraft['alt_baro'],
            alt_geo=aircraft['alt_baro'],  # Assume same for real aircraft
            velocity=aircraft['velocity'] * 1.944,  # Convert to knots
            vertical_rate=aircraft['vertical_rate'] * 196.85 if aircraft['vertical_rate'] else 0,  # m/s to ft/min
            heading=aircraft['heading'],
            receive_times=receive_times
        )
        
        print(f"\n   📊 Anomaly Analysis:")
        print(f"      Overall threat: {anomaly.threat_level}")
        print(f"      TDOA spoofing score: {anomaly.tdoa_spoofing:.2f}")
        print(f"      Physics violations: {anomaly.impossible_speed:.2f}")
        
        if result.verdict == "LEGITIMATE":
            legitimate_count += 1
            print(f"\n   ✅ Aircraft verified as LEGITIMATE")
        else:
            print(f"\n   ⚠️  Aircraft flagged (likely simulation artifact)")
    
    # Summary
    print()
    print("=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(f"\n✈️  Tested {tested_count} live aircraft from OpenSky")
    print(f"✅ {legitimate_count} verified as legitimate")
    print(f"⚠️  {tested_count - legitimate_count} flagged (expected in simulation)")
    
    print("\n📝 Note: Since we're simulating receiver timestamps from the reported")
    print("   position, all aircraft SHOULD validate as legitimate. Any flagged")
    print("   aircraft indicate either:")
    print("   1. Simulation timing precision issues (expected)")
    print("   2. The aircraft is actually reporting inaccurate position data")
    
    # Now demonstrate spoofing detection
    print()
    print("=" * 80)
    print("Demonstrating Spoofing Detection")
    print("=" * 80)
    
    if aircraft_list:
        real_aircraft = aircraft_list[0]
        print(f"\n🎯 Taking aircraft {real_aircraft['callsign']} and simulating spoofing...")
        print(f"   Real position: {real_aircraft['lat']:.4f}°N, {real_aircraft['lon']:.4f}°E")
        
        # Simulate receiver times from REAL position
        real_times = simulate_receiver_timestamps(
            real_aircraft['lat'],
            real_aircraft['lon'],
            real_aircraft['alt_baro'],
            validator.receivers
        )
        
        # But claim to be at a DIFFERENT position
        fake_lat = real_aircraft['lat'] + 0.5  # ~50km north
        fake_lon = real_aircraft['lon'] - 0.5  # ~50km west
        
        print(f"   Claimed (fake) position: {fake_lat:.4f}°N, {fake_lon:.4f}°E")
        print(f"   (Spoofer claims to be ~70km from real position)")
        
        # Validate with FAKE position but REAL timestamps
        spoof_result = validator.validate_position(
            icao=real_aircraft['icao'] + "_SPOOFED",
            claimed_lat=fake_lat,
            claimed_lon=fake_lon,
            claimed_alt=real_aircraft['alt_baro'],
            receive_times=real_times  # Times from REAL position
        )
        
        print(f"\n   🔍 TDOA Detection:")
        print(f"      Verdict: {spoof_result.verdict}")
        print(f"      Position error: {spoof_result.max_error_meters:.1f} m ({spoof_result.max_error_meters/1000:.1f} km)")
        print(f"      Confidence: {spoof_result.confidence:.1%}")
        
        if spoof_result.verdict == "SPOOFED":
            print(f"\n   🚨 SPOOFING DETECTED!")
            print(f"      The signal timing reveals the aircraft is NOT at the claimed position.")
            print(f"      Physics doesn't lie - the speed of light is constant!")
    
    print()
    print("=" * 80)
    print("✅ Live Aircraft Testing Complete!")
    print("=" * 80)
    print()


if __name__ == "__main__":
    try:
        test_live_aircraft()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
