// frontend/src/store/aircraftStore.js
import { create } from "zustand";

// Risk band color palette
export const BAND_COLORS = {
  NORMAL:   "#22d3ee",   // cyan
  MONITOR:  "#facc15",   // yellow
  ALERT:    "#f97316",   // orange
  CRITICAL: "#ef4444",   // red
};

export const CLASS_COLORS = {
  CIVILIAN:           "#22d3ee",
  LIKELY_MILITARY:    "#f97316",
  CONFIRMED_MILITARY: "#ef4444",
  DARK_AIRCRAFT:      "#a855f7",
  UNKNOWN:            "#6b7280",
  SPOOFED:            "#ec4899",
};

export const useAircraftStore = create((set, get) => ({
  // All live aircraft tracks keyed by ICAO24
  tracks: {},

  // Currently selected aircraft for detail panel
  selectedIcao: null,

  // Layer visibility toggles
  layers: {
    civilian:    true,
    military:    true,
    unknown:     true,
    alerts:      true,
    trails:      true,
  },

  // Alert queue (last 50 alerts)
  alerts: [],

  // System stats
  stats: {
    total_tracks: 0,
    civilian: 0,
    military: 0,
    unknown: 0,
    ws_clients: 0,
  },

  // Connection state
  wsStatus: "connecting",   // connecting | connected | disconnected

  // Actions
  applySnapshot: (aircraftList) => {
    const next = {};
    for (const ac of aircraftList) {
      next[ac.icao] = ac;
    }
    set({ tracks: next });
  },

  applyAlert: (alertMsg) => {
    set((state) => {
      const alerts = [alertMsg, ...state.alerts].slice(0, 50);
      return { alerts };
    });
  },

  setSelected: (icao) => set({ selectedIcao: icao }),

  toggleLayer: (layer) =>
    set((state) => ({
      layers: { ...state.layers, [layer]: !state.layers[layer] },
    })),

  setWsStatus: (status) => set({ wsStatus: status }),

  setStats: (stats) => set({ stats }),

  getSelected: () => {
    const { tracks, selectedIcao } = get();
    return selectedIcao ? tracks[selectedIcao] : null;
  },

  getFilteredTracks: () => {
    const { tracks, layers } = get();
    return Object.values(tracks).filter((ac) => {
      if (!ac.lat || !ac.lon) return false;
      const cls = ac.cls;
      if (!layers.civilian && cls === "CIVILIAN") return false;
      if (!layers.military && (cls === "LIKELY_MILITARY" || cls === "CONFIRMED_MILITARY")) return false;
      if (!layers.unknown && (cls === "UNKNOWN" || cls === "DARK_AIRCRAFT")) return false;
      return true;
    });
  },
}));
