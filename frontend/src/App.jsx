// frontend/src/App.jsx
import { useEffect, useRef, useState } from "react";
import { useWebSocket } from "./hooks/useWebSocket";
import { useAircraftStore, BAND_COLORS, CLASS_COLORS } from "./store/aircraftStore";

// ─── Aircraft Icon SVG ─────────────────────────────────────────────────────

function AircraftIcon({ heading = 0, color = "#22d3ee", size = 18, pulsing = false }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      style={{
        transform: `rotate(${heading}deg)`,
        filter: pulsing ? `drop-shadow(0 0 4px ${color})` : "none",
        transition: "transform 1s linear",
      }}
    >
      <path
        d="M12 2 L8 10 L2 10 L6 14 L4 22 L12 18 L20 22 L18 14 L22 10 L16 10 Z"
        fill={color}
        stroke={color}
        strokeWidth="0.5"
        opacity="0.95"
      />
    </svg>
  );
}

// ─── Status Bar ────────────────────────────────────────────────────────────

function StatusBar({ stats, wsStatus }) {
  const statusColor = wsStatus === "connected" ? "#22d3ee" : wsStatus === "connecting" ? "#facc15" : "#ef4444";

  return (
    <div style={{
      position: "fixed", top: 0, left: 0, right: 0, height: 40,
      background: "rgba(2,6,16,0.97)",
      borderBottom: "1px solid rgba(34,211,238,0.2)",
      display: "flex", alignItems: "center",
      padding: "0 16px", gap: 24, zIndex: 100,
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 11, letterSpacing: "0.08em",
      color: "rgba(226,232,240,0.8)"
    }}>
      {/* Logo */}
      <div style={{ color: "#22d3ee", fontWeight: 700, fontSize: 13, letterSpacing: "0.15em" }}>
        ◈ SKYSECURE V2
      </div>

      <div style={{ width: 1, height: 20, background: "rgba(34,211,238,0.2)" }} />

      {/* WS status */}
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <div style={{
          width: 7, height: 7, borderRadius: "50%",
          background: statusColor,
          boxShadow: wsStatus === "connected" ? `0 0 8px ${statusColor}` : "none",
          animation: wsStatus === "connected" ? "pulse 2s infinite" : "none",
        }} />
        <span style={{ color: statusColor }}>
          {wsStatus.toUpperCase()}
        </span>
      </div>

      <div style={{ width: 1, height: 20, background: "rgba(34,211,238,0.2)" }} />

      {/* Stats */}
      <StatChip label="TOTAL" value={stats.total_tracks} color="#22d3ee" />
      <StatChip label="CIVIL" value={stats.classifications?.CIVILIAN || 0} color="#22d3ee" />
      <StatChip label="MIL" value={(stats.classifications?.CONFIRMED_MILITARY || 0) + (stats.classifications?.LIKELY_MILITARY || 0)} color="#ef4444" />
      <StatChip label="DARK" value={stats.classifications?.DARK_AIRCRAFT || 0} color="#a855f7" />
      <StatChip label="ALERTS" value={(stats.risk_bands?.ALERT || 0) + (stats.risk_bands?.CRITICAL || 0)} color="#f97316" />

      <div style={{ flex: 1 }} />

      {/* UTC Clock */}
      <UTCClock />
    </div>
  );
}

function StatChip({ label, value, color }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
      <span style={{ color: "rgba(148,163,184,0.6)", fontSize: 9 }}>{label}</span>
      <span style={{ color, fontWeight: 700, minWidth: 32, textAlign: "right" }}>{value}</span>
    </div>
  );
}

function UTCClock() {
  const [time, setTime] = useState(new Date().toUTCString().split(" ")[4]);
  useEffect(() => {
    const t = setInterval(() => setTime(new Date().toUTCString().split(" ")[4]), 1000);
    return () => clearInterval(t);
  }, []);
  return <div style={{ color: "#22d3ee", fontSize: 12, fontWeight: 600 }}>{time} UTC</div>;
}

// ─── Layer Controls ────────────────────────────────────────────────────────

function LayerControls({ layers, toggle }) {
  const LAYER_DEFS = [
    { key: "civilian",  label: "CIVILIAN",  color: "#22d3ee" },
    { key: "military",  label: "MILITARY",  color: "#ef4444" },
    { key: "unknown",   label: "DARK",      color: "#a855f7" },
    { key: "alerts",    label: "ALERTS",    color: "#f97316" },
    { key: "trails",    label: "TRAILS",    color: "rgba(34,211,238,0.4)" },
  ];

  return (
    <div style={{
      position: "fixed", top: 56, left: 12, zIndex: 90,
      background: "rgba(2,6,16,0.92)",
      border: "1px solid rgba(34,211,238,0.15)",
      borderRadius: 6, padding: "10px 12px",
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 10, letterSpacing: "0.1em",
    }}>
      <div style={{ color: "rgba(148,163,184,0.5)", marginBottom: 8, fontSize: 9 }}>
        LAYER VISIBILITY
      </div>
      {LAYER_DEFS.map(({ key, label, color }) => (
        <div
          key={key}
          onClick={() => toggle(key)}
          style={{
            display: "flex", alignItems: "center", gap: 8,
            marginBottom: 6, cursor: "pointer", userSelect: "none",
            opacity: layers[key] ? 1 : 0.35,
            transition: "opacity 0.2s",
          }}
        >
          <div style={{
            width: 10, height: 10, borderRadius: 2,
            background: layers[key] ? color : "rgba(255,255,255,0.1)",
            border: `1px solid ${color}`,
            transition: "background 0.2s",
          }} />
          <span style={{ color: layers[key] ? color : "rgba(148,163,184,0.4)" }}>
            {label}
          </span>
        </div>
      ))}
    </div>
  );
}

// ─── Alert Feed ────────────────────────────────────────────────────────────

function AlertFeed({ alerts, onSelectAircraft }) {
  if (!alerts.length) return null;

  return (
    <div style={{
      position: "fixed", top: 56, right: 12, zIndex: 90,
      width: 280,
      background: "rgba(2,6,16,0.92)",
      border: "1px solid rgba(239,68,68,0.3)",
      borderRadius: 6,
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 10, maxHeight: "40vh", overflow: "hidden",
    }}>
      <div style={{
        padding: "8px 12px",
        borderBottom: "1px solid rgba(239,68,68,0.2)",
        color: "#ef4444", fontWeight: 700, letterSpacing: "0.1em", fontSize: 10,
        display: "flex", alignItems: "center", gap: 6,
      }}>
        <span style={{ animation: "blink 1s infinite" }}>⚠</span> ACTIVE ALERTS ({alerts.length})
      </div>
      <div style={{ overflow: "auto", maxHeight: "calc(40vh - 36px)" }}>
        {alerts.map((alert, i) => (
          <AlertItem key={i} alert={alert} onClick={() => onSelectAircraft(alert.aircraft?.icao)} />
        ))}
      </div>
    </div>
  );
}

function AlertItem({ alert, onClick }) {
  const ac = alert.aircraft || {};
  const color = BAND_COLORS[ac.band] || "#ef4444";
  const topAnomaly = alert.anomalies?.[0];

  return (
    <div
      onClick={onClick}
      style={{
        padding: "8px 12px",
        borderBottom: "1px solid rgba(255,255,255,0.05)",
        cursor: "pointer",
        transition: "background 0.15s",
      }}
      onMouseEnter={e => e.currentTarget.style.background = "rgba(239,68,68,0.08)"}
      onMouseLeave={e => e.currentTarget.style.background = "transparent"}
    >
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
        <span style={{ color, fontWeight: 700 }}>{ac.icao || "??????"}</span>
        <span style={{
          background: color, color: "#000",
          padding: "1px 5px", borderRadius: 3, fontSize: 9, fontWeight: 700,
        }}>
          RISK {ac.risk}
        </span>
      </div>
      {topAnomaly && (
        <div style={{ color: "rgba(148,163,184,0.7)", fontSize: 9 }}>
          {topAnomaly.type.replace(/_/g, " ")}
        </div>
      )}
    </div>
  );
}

// ─── Aircraft Detail Panel ─────────────────────────────────────────────────

function DetailPanel({ aircraft, onClose }) {
  if (!aircraft) return null;

  const color = CLASS_COLORS[aircraft.cls] || "#22d3ee";
  const bandColor = BAND_COLORS[aircraft.band] || "#22d3ee";

  const fields = [
    ["ICAO24",    aircraft.icao],
    ["CALLSIGN",  aircraft.cs || "—"],
    ["ALT",       aircraft.alt ? `${aircraft.alt.toLocaleString()} ft` : "—"],
    ["SPEED",     aircraft.vel ? `${Math.round(aircraft.vel)} kts` : "—"],
    ["HEADING",   aircraft.hdg ? `${Math.round(aircraft.hdg)}°` : "—"],
    ["V-RATE",    aircraft.vr ? `${aircraft.vr > 0 ? "+" : ""}${aircraft.vr} fpm` : "—"],
    ["SOURCE",    aircraft.src],
    ["CONFIDENCE", aircraft.conf ? `${(aircraft.conf * 100).toFixed(0)}%` : "—"],
    ["MILITARY P", aircraft.mil ? `${(aircraft.mil * 100).toFixed(0)}%` : "0%"],
    ["LAT/LON",   aircraft.lat && aircraft.lon ? `${aircraft.lat.toFixed(4)}, ${aircraft.lon.toFixed(4)}` : "—"],
  ];

  return (
    <div style={{
      position: "fixed", bottom: 16, left: 16, zIndex: 90,
      width: 260,
      background: "rgba(2,6,16,0.97)",
      border: `1px solid ${color}30`,
      borderLeft: `3px solid ${color}`,
      borderRadius: "0 6px 6px 0",
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 10,
    }}>
      {/* Header */}
      <div style={{
        padding: "8px 12px",
        borderBottom: `1px solid ${color}20`,
        display: "flex", justifyContent: "space-between", alignItems: "center",
      }}>
        <div>
          <span style={{ color, fontWeight: 700, fontSize: 13 }}>
            {aircraft.icao}
          </span>
          {aircraft.cs && (
            <span style={{ color: "rgba(148,163,184,0.6)", marginLeft: 8 }}>
              {aircraft.cs}
            </span>
          )}
        </div>
        <button
          onClick={onClose}
          style={{
            background: "none", border: "none",
            color: "rgba(148,163,184,0.5)", cursor: "pointer", fontSize: 16,
          }}
        >×</button>
      </div>

      {/* Classification badge */}
      <div style={{ padding: "6px 12px", borderBottom: `1px solid rgba(255,255,255,0.05)` }}>
        <span style={{
          background: `${color}20`, color,
          padding: "2px 8px", borderRadius: 3, fontSize: 9, fontWeight: 700,
          border: `1px solid ${color}40`,
        }}>
          {aircraft.cls.replace(/_/g, " ")}
        </span>
        <span style={{
          marginLeft: 6,
          background: `${bandColor}20`, color: bandColor,
          padding: "2px 8px", borderRadius: 3, fontSize: 9, fontWeight: 700,
          border: `1px solid ${bandColor}40`,
        }}>
          RISK {aircraft.risk} · {aircraft.band}
        </span>
      </div>

      {/* Fields */}
      <div style={{ padding: "8px 12px" }}>
        {fields.map(([label, value]) => (
          <div key={label} style={{
            display: "flex", justifyContent: "space-between",
            marginBottom: 5, lineHeight: 1.4,
          }}>
            <span style={{ color: "rgba(148,163,184,0.5)", letterSpacing: "0.08em" }}>
              {label}
            </span>
            <span style={{ color: "rgba(226,232,240,0.85)", fontWeight: 500 }}>
              {value}
            </span>
          </div>
        ))}
      </div>

      {/* Active anomalies */}
      {aircraft.anoms && aircraft.anoms.length > 0 && (
        <div style={{
          padding: "6px 12px 10px",
          borderTop: "1px solid rgba(255,255,255,0.05)",
        }}>
          <div style={{ color: "rgba(148,163,184,0.4)", fontSize: 9, marginBottom: 5 }}>
            ACTIVE ANOMALIES
          </div>
          {aircraft.anoms.map((a, i) => (
            <div key={i} style={{
              color: "#f97316", fontSize: 9, marginBottom: 3,
              display: "flex", alignItems: "center", gap: 5,
            }}>
              <span>▸</span>
              <span>{a.replace(/_/g, " ")}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Map Layer (Canvas-based, no Mapbox dependency) ────────────────────────

function RadarCanvas({ tracks, layers, onSelectAircraft }) {
  const canvasRef = useRef(null);
  const animRef = useRef(null);
  const tracksRef = useRef(tracks);
  const layersRef = useRef(layers);
  const [transform, setTransform] = useState({ offsetX: 0, offsetY: 0, scale: 1 });
  const transformRef = useRef(transform);
  const dragRef = useRef({ dragging: false, lastX: 0, lastY: 0 });

  useEffect(() => { tracksRef.current = tracks; }, [tracks]);
  useEffect(() => { layersRef.current = layers; }, [layers]);
  useEffect(() => { transformRef.current = transform; }, [transform]);

  // Project lat/lon to canvas x/y using Mercator
  function project(lat, lon, width, height, tx, ty, scale) {
    const x0 = width / 2;
    const y0 = height / 2;
    const baseScale = width / (2 * Math.PI);

    const x = baseScale * scale * ((lon + 180) * Math.PI / 180) - baseScale * scale * Math.PI + x0 + tx;
    const latRad = lat * Math.PI / 180;
    const mercY = Math.log(Math.tan(Math.PI / 4 + latRad / 2));
    const y = y0 - baseScale * scale * mercY + ty;

    return [x, y];
  }

  function draw() {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    const W = canvas.width;
    const H = canvas.height;
    const { offsetX, offsetY, scale } = transformRef.current;
    const tracks = tracksRef.current;
    const layers = layersRef.current;

    // Background
    ctx.fillStyle = "#020610";
    ctx.fillRect(0, 0, W, H);

    // Grid lines (latitude/longitude)
    ctx.strokeStyle = "rgba(34,211,238,0.04)";
    ctx.lineWidth = 1;

    for (let lon = -180; lon <= 180; lon += 30) {
      ctx.beginPath();
      const [x] = project(0, lon, W, H, offsetX, offsetY, scale);
      ctx.moveTo(x, 0); ctx.lineTo(x, H);
      ctx.stroke();
    }
    for (let lat = -60; lat <= 60; lat += 30) {
      ctx.beginPath();
      const [, y] = project(lat, 0, W, H, offsetX, offsetY, scale);
      ctx.moveTo(0, y); ctx.lineTo(W, y);
      ctx.stroke();
    }

    // Draw aircraft
    const now = Date.now() / 1000;

    for (const ac of tracks) {
      if (!ac.lat || !ac.lon) continue;

      // Filter by layer
      const cls = ac.cls;
      if (!layers.civilian && cls === "CIVILIAN") continue;
      if (!layers.military && (cls === "LIKELY_MILITARY" || cls === "CONFIRMED_MILITARY")) continue;
      if (!layers.unknown && (cls === "UNKNOWN" || cls === "DARK_AIRCRAFT")) continue;

      const [x, y] = project(ac.lat, ac.lon, W, H, offsetX, offsetY, scale);

      // Cull off-screen
      if (x < -20 || x > W + 20 || y < -20 || y > H + 20) continue;

      const color = CLASS_COLORS[cls] || "#22d3ee";
      const bandColor = BAND_COLORS[ac.band] || color;
      const isCritical = ac.band === "CRITICAL" || ac.band === "ALERT";

      // Trail
      if (layers.trails && ac.trail && ac.trail.length > 1) {
        ctx.beginPath();
        let first = true;
        for (const pt of ac.trail) {
          const [tx, ty] = project(pt.lat, pt.lon, W, H, offsetX, offsetY, scale);
          if (first) { ctx.moveTo(tx, ty); first = false; }
          else ctx.lineTo(tx, ty);
        }
        ctx.strokeStyle = `${color}30`;
        ctx.lineWidth = 1;
        ctx.stroke();
      }

      // Pulse ring for alerts
      if (isCritical && layers.alerts) {
        const pulseR = 10 + 6 * Math.sin(now * 4);
        ctx.beginPath();
        ctx.arc(x, y, pulseR, 0, 2 * Math.PI);
        ctx.strokeStyle = `${bandColor}60`;
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }

      // Aircraft marker
      const heading = ac.hdg || 0;
      ctx.save();
      ctx.translate(x, y);
      ctx.rotate((heading * Math.PI) / 180);

      // Draw arrow/plane shape
      ctx.beginPath();
      ctx.moveTo(0, -6);
      ctx.lineTo(-4, 4);
      ctx.lineTo(0, 2);
      ctx.lineTo(4, 4);
      ctx.closePath();
      ctx.fillStyle = color;
      ctx.fill();

      ctx.restore();

      // Callsign label
      const label = ac.cs || ac.icao;
      if (scale > 1.5) {
        ctx.font = "9px 'JetBrains Mono', monospace";
        ctx.fillStyle = `${color}aa`;
        ctx.fillText(label, x + 7, y - 5);
      }
    }

    // Aircraft count
    ctx.font = "10px 'JetBrains Mono', monospace";
    ctx.fillStyle = "rgba(34,211,238,0.3)";
    ctx.fillText(`${tracks.length} TRACKS`, 16, H - 16);
  }

  useEffect(() => {
    function resize() {
      const canvas = canvasRef.current;
      if (!canvas) return;
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    }
    resize();
    window.addEventListener("resize", resize);
    return () => window.removeEventListener("resize", resize);
  }, []);

  useEffect(() => {
    function loop() {
      draw();
      animRef.current = requestAnimationFrame(loop);
    }
    animRef.current = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(animRef.current);
  }, []);

  // Pan
  function onMouseDown(e) {
    dragRef.current = { dragging: true, lastX: e.clientX, lastY: e.clientY };
  }

  function onMouseMove(e) {
    const d = dragRef.current;
    if (!d.dragging) return;
    const dx = e.clientX - d.lastX;
    const dy = e.clientY - d.lastY;
    dragRef.current.lastX = e.clientX;
    dragRef.current.lastY = e.clientY;
    setTransform(t => ({ ...t, offsetX: t.offsetX + dx, offsetY: t.offsetY + dy }));
  }

  function onMouseUp() {
    dragRef.current.dragging = false;
  }

  // Zoom
  function onWheel(e) {
    e.preventDefault();
    const factor = e.deltaY < 0 ? 1.15 : 0.87;
    setTransform(t => ({ ...t, scale: Math.max(0.5, Math.min(20, t.scale * factor)) }));
  }

  // Click to select
  function onClick(e) {
    if (dragRef.current.moved) return;
    const canvas = canvasRef.current;
    const rect = canvas.getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const { offsetX, offsetY, scale } = transformRef.current;
    const W = canvas.width, H = canvas.height;

    let closest = null;
    let minDist = 16;

    for (const ac of tracksRef.current) {
      if (!ac.lat || !ac.lon) continue;
      const [x, y] = project(ac.lat, ac.lon, W, H, offsetX, offsetY, scale);
      const d = Math.sqrt((x - cx) ** 2 + (y - cy) ** 2);
      if (d < minDist) {
        minDist = d;
        closest = ac.icao;
      }
    }
    onSelectAircraft(closest);
  }

  return (
    <canvas
      ref={canvasRef}
      style={{ position: "fixed", top: 0, left: 0, cursor: "crosshair" }}
      onMouseDown={onMouseDown}
      onMouseMove={onMouseMove}
      onMouseUp={onMouseUp}
      onWheel={onWheel}
      onClick={onClick}
    />
  );
}

// ─── Root App ──────────────────────────────────────────────────────────────

export default function App() {
  useWebSocket();

  const {
    wsStatus,
    layers,
    toggleLayer,
    alerts,
    selectedIcao,
    setSelected,
    getFilteredTracks,
    getSelected,
  } = useAircraftStore();

  const [stats, setStats] = useState({});

  // Fetch stats periodically
  useEffect(() => {
    async function fetchStats() {
      try {
        const res = await fetch(
          (import.meta.env.VITE_API_URL || "http://localhost:8000") + "/api/stats"
        );
        const data = await res.json();
        setStats(data);
      } catch (e) {}
    }
    fetchStats();
    const t = setInterval(fetchStats, 5000);
    return () => clearInterval(t);
  }, []);

  const tracks = getFilteredTracks();
  const selected = getSelected();

  return (
    <div style={{
      background: "#020610",
      width: "100vw",
      height: "100vh",
      overflow: "hidden",
      fontFamily: "'JetBrains Mono', monospace",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: rgba(2,6,16,0.9); }
        ::-webkit-scrollbar-thumb { background: rgba(34,211,238,0.3); border-radius: 2px; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
        @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }
      `}</style>

      {/* Radar canvas */}
      <RadarCanvas
        tracks={tracks}
        layers={layers}
        onSelectAircraft={setSelected}
      />

      {/* Overlay scanline effect */}
      <div style={{
        position: "fixed", inset: 0,
        backgroundImage: "repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,0.03) 2px, rgba(0,0,0,0.03) 4px)",
        pointerEvents: "none", zIndex: 1,
      }} />

      {/* Top status bar */}
      <StatusBar stats={stats} wsStatus={wsStatus} />

      {/* Layer controls */}
      <LayerControls layers={layers} toggle={toggleLayer} />

      {/* Alert feed */}
      <AlertFeed alerts={alerts} onSelectAircraft={setSelected} />

      {/* Aircraft detail panel */}
      <DetailPanel aircraft={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
