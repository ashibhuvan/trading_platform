import { useState, useEffect, useRef, useCallback } from 'react';

/**
 * usePriceFeed — React hook for live market data via WebSocket.
 *
 * Connects to the price-feed-service WebSocket, subscribes to the
 * requested symbols, and returns live ticks, bars, connection state,
 * and any error.
 *
 * Reconnects automatically with exponential backoff on disconnect.
 *
 * @param {string[]} symbols  - Symbols to subscribe to, e.g. ["ES", "NQ"]
 * @param {string}   [wsUrl]  - WebSocket endpoint (default from env or localhost:8080)
 * @returns {{ ticks: Object<string, object>, bars: Object<string, object[]>, connected: boolean, error: string|null }}
 */
const WS_ENDPOINT =
  (typeof import.meta !== 'undefined' && import.meta.env && import.meta.env.VITE_WS_ENDPOINT) ||
  'ws://localhost:8080';

const INITIAL_RECONNECT_MS = 1000;
const MAX_RECONNECT_MS = 30000;

export default function usePriceFeed(symbols = [], wsUrl = WS_ENDPOINT) {
  // Latest tick per symbol  { ES: { type, symbol, ts, bid, ask, last, volume } }
  const [ticks, setTicks] = useState({});
  // Accumulated bars per symbol  { ES: [ {type,symbol,timeframe,ts,o,h,l,c,v}, ... ] }
  const [bars, setBars] = useState({});
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState(null);

  const wsRef = useRef(null);
  const reconnectDelay = useRef(INITIAL_RECONNECT_MS);
  const reconnectTimer = useRef(null);
  const symbolsRef = useRef(symbols);
  const mountedRef = useRef(true);

  // Keep symbolsRef in sync for use inside WS callbacks
  useEffect(() => {
    symbolsRef.current = symbols;
  }, [symbols]);

  const connect = useCallback(() => {
    if (!mountedRef.current) return;

    try {
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!mountedRef.current) return;
        setConnected(true);
        setError(null);
        reconnectDelay.current = INITIAL_RECONNECT_MS;

        // Subscribe to current symbols
        if (symbolsRef.current.length > 0) {
          ws.send(JSON.stringify({ action: 'subscribe', symbols: symbolsRef.current }));
        }
      };

      ws.onmessage = (event) => {
        if (!mountedRef.current) return;
        try {
          const msg = JSON.parse(event.data);

          if (msg.type === 'tick') {
            setTicks((prev) => ({ ...prev, [msg.symbol]: msg }));
          } else if (msg.type === 'bar') {
            setBars((prev) => {
              const existing = prev[msg.symbol] || [];
              return { ...prev, [msg.symbol]: [...existing, msg] };
            });
          }
          // status messages are informational — could be surfaced later
        } catch {
          // ignore non-JSON frames
        }
      };

      ws.onclose = () => {
        if (!mountedRef.current) return;
        setConnected(false);
        scheduleReconnect();
      };

      ws.onerror = (e) => {
        if (!mountedRef.current) return;
        setError('WebSocket error');
        ws.close();
      };
    } catch (e) {
      setError(e.message);
      scheduleReconnect();
    }
  }, [wsUrl]);

  const scheduleReconnect = useCallback(() => {
    if (!mountedRef.current) return;
    clearTimeout(reconnectTimer.current);
    reconnectTimer.current = setTimeout(() => {
      reconnectDelay.current = Math.min(reconnectDelay.current * 2, MAX_RECONNECT_MS);
      connect();
    }, reconnectDelay.current);
  }, [connect]);

  // Re-subscribe when symbol list changes while connected
  useEffect(() => {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN && symbols.length > 0) {
      ws.send(JSON.stringify({ action: 'subscribe', symbols }));
    }
  }, [symbols]);

  // Connect on mount, disconnect on unmount
  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      clearTimeout(reconnectTimer.current);
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connect]);

  return { ticks, bars, connected, error };
}
