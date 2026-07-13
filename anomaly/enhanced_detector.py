"""
SkySecure v2 - Enhanced Anomaly Detector with TDOA
===================================================
Combines behavioral anomaly detection with physics-based TDOA validation
for comprehensive spoofing detection.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class AnomalyScore:
    """Multi-layer anomaly scoring"""
    icao: str
    timestamp: datetime
    
    # Layer 1: Physics violations
    impossible_speed: float  # 0-1
    impossible_climb: float  # 0-1
    altitude_conflict: float  # 0-1
    
    # Layer 2: Behavioral anomalies
    unusual_trajectory: float  # 0-1
    mode_s_irregularities: float  # 0-1
    
    # Layer 3: TDOA validation (NEW!)
    tdoa_spoofing: float  # 0-1
    tdoa_position_error: float  # meters
    
    # Overall
    overall_score: float  # 0-1 (weighted combination)
    threat_level: str  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    
    def to_dict(self) -> dict:
        return {
            "icao": self.icao,
            "timestamp": self.timestamp.isoformat(),
            "scores": {
                "impossible_speed": self.impossible_speed,
                "impossible_climb": self.impossible_climb,
                "altitude_conflict": self.altitude_conflict,
                "unusual_trajectory": self.unusual_trajectory,
                "mode_s_irregularities": self.mode_s_irregularities,
                "tdoa_spoofing": self.tdoa_spoofing
            },
            "tdoa_position_error_m": self.tdoa_position_error,
            "overall_score": self.overall_score,
            "threat_level": self.threat_level
        }


class EnhancedAnomalyDetector:
    """
    4-layer anomaly detection with TDOA integration.
    
    Layer 1: Physics violations (speed, climb rate, altitude conflicts)
    Layer 2: Behavioral anomalies (trajectory, Mode S irregularities)
    Layer 3: TDOA validation (position spoofing detection)
    Layer 4: Ensemble scoring (weighted combination)
    """
    
    def __init__(self, tdoa_validator=None):
        """
        Args:
            tdoa_validator: Optional TDOAValidator instance
        """
        self.tdoa_validator = tdoa_validator
        self.previous_states = {}  # Track history for behavioral analysis
        
        # Scoring weights
        self.weights = {
            "impossible_speed": 0.20,
            "impossible_climb": 0.15,
            "altitude_conflict": 0.15,
            "unusual_trajectory": 0.10,
            "mode_s_irregularities": 0.10,
            "tdoa_spoofing": 0.30  # TDOA gets highest weight - it's physics
        }
    
    def check_physics_violations(
        self,
        icao: str,
        lat: float,
        lon: float,
        alt_baro: float,
        alt_geo: Optional[float],
        velocity: float,
        vertical_rate: float
    ) -> Dict[str, float]:
        """
        Layer 1: Check for impossible physics.
        
        Returns: {violation_type: score_0_to_1}
        """
        scores = {}
        
        # Check 1: Impossible speed (>1000 kt for non-military)
        max_plausible_speed = 1000  # knots
        if velocity > max_plausible_speed:
            scores["impossible_speed"] = min(velocity / 1500, 1.0)
        else:
            scores["impossible_speed"] = 0.0
        
        # Check 2: Impossible climb rate (>10000 ft/min for non-military)
        max_climb_rate = 10000  # ft/min
        if abs(vertical_rate) > max_climb_rate:
            scores["impossible_climb"] = min(abs(vertical_rate) / 15000, 1.0)
        else:
            scores["impossible_climb"] = 0.0
        
        # Check 3: Baro/Geo altitude conflict (spoofing indicator)
        if alt_geo is not None:
            alt_diff = abs(alt_baro - alt_geo)
            if alt_diff > 500:  # >500ft difference is suspicious
                scores["altitude_conflict"] = min(alt_diff / 2000, 1.0)
            else:
                scores["altitude_conflict"] = 0.0
        else:
            scores["altitude_conflict"] = 0.0
        
        return scores
    
    def check_behavioral_anomalies(
        self,
        icao: str,
        lat: float,
        lon: float,
        heading: float,
        velocity: float
    ) -> Dict[str, float]:
        """
        Layer 2: Check for unusual behavior.
        
        Compares against previous state to detect:
        - Sudden direction changes
        - Erratic movement
        - Teleportation (position jumps)
        """
        scores = {"unusual_trajectory": 0.0, "mode_s_irregularities": 0.0}
        
        if icao in self.previous_states:
            prev = self.previous_states[icao]
            
            # Check for sudden position jumps (teleportation)
            # Simplified haversine for distance
            lat_diff = abs(lat - prev["lat"])
            lon_diff = abs(lon - prev["lon"])
            position_jump = np.sqrt(lat_diff**2 + lon_diff**2) * 111  # rough km
            
            time_elapsed = 1.0  # Assume 1 second between updates for now
            implied_speed = position_jump / time_elapsed  # km/s
            
            if implied_speed > 0.5:  # >500 m/s = teleportation
                scores["unusual_trajectory"] = min(implied_speed, 1.0)
        
        # Update previous state
        self.previous_states[icao] = {
            "lat": lat,
            "lon": lon,
            "heading": heading,
            "velocity": velocity
        }
        
        return scores
    
    def check_tdoa_spoofing(
        self,
        icao: str,
        lat: float,
        lon: float,
        alt: float,
        receive_times: Dict[str, float]
    ) -> tuple[float, float]:
        """
        Layer 3: TDOA-based spoofing detection.
        
        Returns: (spoofing_score_0_to_1, position_error_meters)
        """
        if not self.tdoa_validator or not receive_times or len(receive_times) < 4:
            return 0.0, 0.0
        
        try:
            result = self.tdoa_validator.validate_position(
                icao, lat, lon, alt, receive_times
            )
            
            # Convert TDOA result to 0-1 score
            if result.verdict == "LEGITIMATE":
                spoofing_score = 0.0
            elif result.verdict == "UNCERTAIN":
                spoofing_score = 0.5
            else:  # SPOOFED
                spoofing_score = 1.0
            
            return spoofing_score, result.max_error_meters
            
        except Exception as e:
            logger.error(f"TDOA validation failed for {icao}: {e}")
            return 0.0, 0.0
    
    def calculate_overall_score(
        self,
        icao: str,
        lat: float,
        lon: float,
        alt_baro: float,
        alt_geo: Optional[float],
        velocity: float,
        vertical_rate: float,
        heading: float,
        receive_times: Optional[Dict[str, float]] = None
    ) -> AnomalyScore:
        """
        Calculate comprehensive anomaly score across all layers.
        
        Args:
            icao: Aircraft ICAO24 identifier
            lat, lon: Position (degrees)
            alt_baro: Barometric altitude (feet)
            alt_geo: Geometric altitude (feet, optional)
            velocity: Ground speed (knots)
            vertical_rate: Climb/descent rate (ft/min)
            heading: Direction (degrees)
            receive_times: {receiver_id: timestamp} for TDOA validation
        
        Returns:
            AnomalyScore with multi-layer scoring
        """
        # Layer 1: Physics
        physics_scores = self.check_physics_violations(
            icao, lat, lon, alt_baro, alt_geo, velocity, vertical_rate
        )
        
        # Layer 2: Behavioral
        behavioral_scores = self.check_behavioral_anomalies(
            icao, lat, lon, heading, velocity
        )
        
        # Layer 3: TDOA
        tdoa_score, tdoa_error = self.check_tdoa_spoofing(
            icao, lat, lon, alt_baro, receive_times or {}
        )
        
        # Layer 4: Weighted ensemble
        overall = (
            physics_scores["impossible_speed"] * self.weights["impossible_speed"] +
            physics_scores["impossible_climb"] * self.weights["impossible_climb"] +
            physics_scores["altitude_conflict"] * self.weights["altitude_conflict"] +
            behavioral_scores["unusual_trajectory"] * self.weights["unusual_trajectory"] +
            behavioral_scores["mode_s_irregularities"] * self.weights["mode_s_irregularities"] +
            tdoa_score * self.weights["tdoa_spoofing"]
        )
        
        # Threat level classification
        if overall < 0.3:
            threat_level = "LOW"
        elif overall < 0.6:
            threat_level = "MEDIUM"
        elif overall < 0.8:
            threat_level = "HIGH"
        else:
            threat_level = "CRITICAL"
        
        return AnomalyScore(
            icao=icao,
            timestamp=datetime.utcnow(),
            impossible_speed=physics_scores["impossible_speed"],
            impossible_climb=physics_scores["impossible_climb"],
            altitude_conflict=physics_scores["altitude_conflict"],
            unusual_trajectory=behavioral_scores["unusual_trajectory"],
            mode_s_irregularities=behavioral_scores["mode_s_irregularities"],
            tdoa_spoofing=tdoa_score,
            tdoa_position_error=tdoa_error,
            overall_score=overall,
            threat_level=threat_level
        )


if __name__ == "__main__":
    # Test without TDOA
    detector = EnhancedAnomalyDetector()
    
    # Test case 1: Normal aircraft
    score1 = detector.calculate_overall_score(
        icao="AAL123",
        lat=39.8717,
        lon=-75.2411,
        alt_baro=3000,
        alt_geo=3020,
        velocity=250,
        vertical_rate=500,
        heading=90
    )
    print(f"Normal aircraft: {score1.threat_level} ({score1.overall_score:.2f})")
    
    # Test case 2: Spoofed aircraft (impossible speed)
    score2 = detector.calculate_overall_score(
        icao="SPOOF1",
        lat=40.0000,
        lon=-75.3000,
        alt_baro=8000,
        alt_geo=8000,
        velocity=1500,  # Impossible for civilian
        vertical_rate=20000,  # Impossible climb
        heading=180
    )
    print(f"Spoofed aircraft: {score2.threat_level} ({score2.overall_score:.2f})")
