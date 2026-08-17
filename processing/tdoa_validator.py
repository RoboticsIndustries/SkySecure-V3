"""
SkySecure v2 - TDOA Validator Module
=====================================
Integrates Time Difference of Arrival validation into the existing SkySecure
processing pipeline for physics-based spoofing detection.

This module plugs into the fusion engine to validate aircraft positions
using multi-receiver timing analysis.
"""

import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import numpy as np

# Kafka imports (compatible with SkySecure v2)
try:
    from kafka import KafkaConsumer, KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning("Kafka not available - running in standalone mode")

# Constants
SPEED_OF_LIGHT = 299792458  # meters per second
EARTH_RADIUS = 6371000  # meters

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """3D position in ECEF coordinates"""
    x: float  # meters
    y: float  # meters
    z: float  # meters
    
    @classmethod
    def from_lat_lon_alt(cls, lat: float, lon: float, alt: float) -> 'Position':
        """Convert WGS84 to ECEF"""
        lat_rad = np.radians(lat)
        lon_rad = np.radians(lon)
        
        a = 6378137.0
        e2 = 0.00669437999014
        
        N = a / np.sqrt(1 - e2 * np.sin(lat_rad)**2)
        
        x = (N + alt) * np.cos(lat_rad) * np.cos(lon_rad)
        y = (N + alt) * np.cos(lat_rad) * np.sin(lon_rad)
        z = (N * (1 - e2) + alt) * np.sin(lat_rad)
        
        return cls(x, y, z)
    
    def distance_to(self, other: 'Position') -> float:
        """Euclidean distance"""
        return np.sqrt(
            (self.x - other.x)**2 + 
            (self.y - other.y)**2 + 
            (self.z - other.z)**2
        )
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Receiver:
    """ADS-B receiver with GPS timing"""
    id: str
    position: Position
    clock_offset: float = 0.0  # nanoseconds
    last_sync: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "position": self.position.to_dict(),
            "clock_offset_ns": self.clock_offset,
            "last_sync": self.last_sync.isoformat() if self.last_sync else None
        }


@dataclass
class TDOAValidationResult:
    """Result of TDOA validation"""
    icao: str
    is_valid: bool
    max_error_meters: float
    confidence: float  # 0.0-1.0
    timestamp: datetime
    errors_by_receiver: Dict[str, float]
    verdict: str  # "LEGITIMATE", "SPOOFED", "UNCERTAIN"
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat()
        }


class TDOAValidator:
    """
    TDOA-based position validator for SkySecure v2.
    
    Integrates with:
    - processing/mlat_solver.py (receives MLAT positions)
    - processing/fusion_engine.py (validates fused tracks)
    - anomaly/detector.py (provides spoofing scores)
    """
    
    def __init__(
        self,
        receivers: List[Receiver],
        kafka_bootstrap_servers: Optional[str] = None,
        detection_threshold_meters: float = 500
    ):
        if len(receivers) < 4:
            raise ValueError("Need at least 4 receivers for TDOA validation")
        
        self.receivers = {r.id: r for r in receivers}
        self.detection_threshold = detection_threshold_meters
        self.kafka_servers = kafka_bootstrap_servers
        
        # Kafka integration
        if KAFKA_AVAILABLE and kafka_bootstrap_servers:
            self.consumer = KafkaConsumer(
                'fused.tracks',  # Listen to fused tracks from fusion engine
                bootstrap_servers=kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("TDOA validator connected to Kafka")
        else:
            self.consumer = None
            self.producer = None
            logger.warning("Running in standalone mode without Kafka")
    
    def calculate_expected_tdoa(
        self,
        position: Position,
        ref_receiver_id: str
    ) -> Dict[str, float]:
        """Calculate expected time differences"""
        ref_receiver = self.receivers[ref_receiver_id]
        ref_distance = position.distance_to(ref_receiver.position)
        
        tdoas = {}
        for rid, receiver in self.receivers.items():
            if rid == ref_receiver_id:
                tdoas[rid] = 0.0
            else:
                distance = position.distance_to(receiver.position)
                tdoa = (distance - ref_distance) / SPEED_OF_LIGHT
                tdoa += (receiver.clock_offset - ref_receiver.clock_offset) * 1e-9
                tdoas[rid] = tdoa
        
        return tdoas
    
    def calculate_observed_tdoa(
        self,
        receive_times: Dict[str, float],
        ref_receiver_id: str
    ) -> Dict[str, float]:
        """Calculate observed time differences"""
        ref_time = receive_times[ref_receiver_id]
        return {rid: time - ref_time for rid, time in receive_times.items()}
    
    def validate_position(
        self,
        icao: str,
        claimed_lat: float,
        claimed_lon: float,
        claimed_alt: float,
        receive_times: Dict[str, float]
    ) -> TDOAValidationResult:
        """
        Validate aircraft position using TDOA.
        
        Args:
            icao: Aircraft ICAO24 identifier
            claimed_lat: Latitude from ADS-B message
            claimed_lon: Longitude from ADS-B message
            claimed_alt: Altitude from ADS-B message
            receive_times: {receiver_id: timestamp} of message reception
        
        Returns:
            TDOAValidationResult with spoofing verdict
        """
        claimed_position = Position.from_lat_lon_alt(
            claimed_lat, claimed_lon, claimed_alt
        )
        
        # Use first receiver as reference
        ref_id = list(self.receivers.keys())[0]
        
        # Calculate expected vs observed TDOA
        expected_tdoa = self.calculate_expected_tdoa(claimed_position, ref_id)
        observed_tdoa = self.calculate_observed_tdoa(receive_times, ref_id)
        
        # Calculate errors
        errors = {}
        max_error_meters = 0
        
        for rid in expected_tdoa.keys():
            if rid == ref_id:
                continue
            
            time_error = abs(expected_tdoa[rid] - observed_tdoa[rid])
            distance_error = time_error * SPEED_OF_LIGHT
            
            errors[rid] = distance_error
            max_error_meters = max(max_error_meters, distance_error)
        
   # Determine verdict
        is_valid = max_error_meters < self.detection_threshold
        
        if max_error_meters < 100:
            verdict = "LEGITIMATE"
            # Confidence varies with error magnitude within this range
            confidence = max(0.95, 1.0 - (max_error_meters / 100.0) * 0.05)
        elif max_error_meters < self.detection_threshold:
            verdict = "LEGITIMATE"
            # Confidence decreases as error increases toward threshold
            confidence = max(0.90, 1.0 - (max_error_meters / self.detection_threshold) * 0.10)
        elif max_error_meters < 2000:
            verdict = "UNCERTAIN"
            confidence = max(0.50, 1.0 - (max_error_meters / 2000.0) * 0.50)
        else:
            verdict = "SPOOFED"
            confidence = max(0.0, 1.0 - (500 / max_error_meters))
        
        return TDOAValidationResult(
            icao=icao,
            is_valid=is_valid,
            max_error_meters=max_error_meters,
            confidence=confidence,
            timestamp=datetime.utcnow(),
            errors_by_receiver=errors,
            verdict=verdict
        )
    
    def process_track(self, track_data: dict) -> Optional[TDOAValidationResult]:
        """
        Process a fused track from Kafka and validate it.
        
        Expected track format (from fusion_engine):
        {
            "icao": "A1B2C3",
            "lat": 39.8717,
            "lon": -75.2411,
            "alt": 3000,
            "receive_times": {"RX1": 1000.123, "RX2": 1000.156, ...},
            "timestamp": "2026-05-25T12:34:56Z"
        }
        """
        try:
            # Extract position data
            icao = track_data.get("icao")
            lat = track_data.get("lat")
            lon = track_data.get("lon")
            alt = track_data.get("alt")
            receive_times = track_data.get("receive_times", {})
            
            if not all([icao, lat, lon, alt]) or len(receive_times) < 4:
                logger.debug(f"Insufficient data for TDOA validation: {icao}")
                return None
            
            # Validate
            result = self.validate_position(icao, lat, lon, alt, receive_times)
            
            # Publish to Kafka if spoofed
            if self.producer and result.verdict == "SPOOFED":
                self.producer.send('tdoa.spoofed', value=result.to_dict())
                logger.warning(
                    f"🚨 Spoofing detected: {icao} - "
                    f"{result.max_error_meters:.0f}m error"
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating track: {e}")
            return None
    
    async def run(self):
        """Main processing loop - consumes from Kafka and validates"""
        if not self.consumer:
            logger.error("Cannot run without Kafka consumer")
            return
        
        logger.info("TDOA validator started, listening for tracks...")
        
        for message in self.consumer:
            track_data = message.value
            result = self.process_track(track_data)
            
            if result and result.verdict == "SPOOFED":
                # Could trigger alerts here
                pass
    
    def get_receiver_status(self) -> List[dict]:
        """Get status of all receivers"""
        return [r.to_dict() for r in self.receivers.values()]


# Standalone mode for testing
def create_test_receivers() -> List[Receiver]:
    """Create test receiver network around Philadelphia"""
    return [
        Receiver(
            id="RX1_Brandywine",
            position=Position.from_lat_lon_alt(39.9526, -75.1652, 100),
            clock_offset=5.0
        ),
        Receiver(
            id="RX2_WestChester",
            position=Position.from_lat_lon_alt(39.9606, -75.6080, 120),
            clock_offset=-3.0
        ),
        Receiver(
            id="RX3_Wilmington",
            position=Position.from_lat_lon_alt(39.7391, -75.5398, 90),
            clock_offset=2.0
        ),
        Receiver(
            id="RX4_Camden",
            position=Position.from_lat_lon_alt(39.9259, -75.1196, 85),
            clock_offset=-1.0
        ),
    ]


if __name__ == "__main__":
    # Standalone test
    logging.basicConfig(level=logging.INFO)
    
    receivers = create_test_receivers()
    validator = TDOAValidator(receivers)
    
    # Test legitimate track
    test_track = {
        "icao": "AAL123",
        "lat": 39.8717,
        "lon": -75.2411,
        "alt": 3000,
        "receive_times": {
            "RX1_Brandywine": 1000.000523,
            "RX2_WestChester": 1000.000589,
            "RX3_Wilmington": 1000.000612,
            "RX4_Camden": 1000.000498
        }
    }
    
    result = validator.process_track(test_track)
    print(f"\nValidation result: {result.verdict}")
    print(f"Max error: {result.max_error_meters:.1f}m")
    print(f"Confidence: {result.confidence:.2f}")
