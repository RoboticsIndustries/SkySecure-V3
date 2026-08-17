// frontend/src/hooks/useWebSocket.js
import { useEffect, useRef } from "react";
import { useAircraftStore } from "../store/aircraftStore";

const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws/tracks";
const RECONNECT_DELAY = 3000;

export function useWebSocket() {
  const wsRef = useRef(null);
  const retryRef = useRef(null);
  const { applySnapshot, applyAlert, setWsStatus } = useAircraftStore();

  useEffect(() => {
    function connect() {
      setWsStatus("connecting");
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.binaryType = "arraybuffer";

      ws.onopen = () => {
        setWsStatus("connected");
        console.log("[SkySecure] WebSocket connected");

        // Keep-alive ping
        const ping = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send("ping");
          }
        }, 20_000);
        ws._ping = ping;
      };

      ws.onmessage = (event) => {
        try {
          const text =
            event.data instanceof ArrayBuffer
              ? new TextDecoder().decode(event.data)
              : event.data;

          if (text === "ping") {
            ws.send("pong");
            return;
          }
          if (text === "pong") return;

          const msg = JSON.parse(text);

          if (msg.type === "snapshot") {
            applySnapshot(msg.aircraft || []);
          } else if (msg.type === "alert") {
            applyAlert(msg);
          }
        } catch (e) {
          console.error("[SkySecure] WebSocket parse error", e);
        }
      };

      ws.onerror = (e) => {
        console.error("[SkySecure] WebSocket error", e);
      };

      ws.onclose = () => {
        clearInterval(ws._ping);
        setWsStatus("disconnected");
        console.log("[SkySecure] WebSocket closed. Reconnecting in 3s...");
        retryRef.current = setTimeout(connect, RECONNECT_DELAY);
      };
    }

    connect();

    return () => {
      clearTimeout(retryRef.current);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.close();
      }
    };
  }, []);
}
