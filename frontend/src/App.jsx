import React, { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend
} from "recharts";

function parseRecent(text) {
  const lines = text.trim().split("\n").filter(Boolean);
  const arr = [];
  for (const l of lines) {
    try { arr.push(JSON.parse(l)); } catch {}
  }
  return arr.sort((a,b)=>a.window_start - b.window_start); // chronological
}

function tsLabel(t) {
  return new Date(t).toISOString().slice(11,19); // HH:mm:ss
}

export default function App() {
  const [data, setData] = useState([]);
  const [late, setLate] = useState([]);
  const [manualTs, setManualTs] = useState("");
  const [manualVal, setManualVal] = useState("");

  useEffect(() => {
    async function poll() {
      try {
        const r = await fetch("/recent");
        const txt = await r.text();
        if (txt.trim().length > 0) {
          const parsed = parseRecent(txt).slice(-30); // latest 30 windows
          setData(
            parsed.map(w => ({
              ts: tsLabel(w.window_start),
              count: w.count,
              sum: Number(w.sum.toFixed(2))
            }))
          );
        } else {
          setData([]);
        }

        const r2 = await fetch("/late");
        setLate(await r2.json());
      } catch {}
    }

    poll();
    const id = setInterval(poll, 1200);
    return () => clearInterval(id);
  }, []);

  async function sendManual() {
    const ts = manualTs ? Number(manualTs) : Date.now();
    const payload = {
      id: crypto.randomUUID(),
      ts,
      value: manualVal ? Number(manualVal) : Math.random() * 100
    };
    try {
      await fetch("/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      setManualTs("");
      setManualVal("");
    } catch (e) {
      console.error(e);
    }
  }

  return (
    <div className="container">
      <h1>FlowCore — Streaming Engine</h1>
      <p className="subtitle">
        Real-time tumbling windows (10s), watermarks, checkpointing, late events.
      </p>

      <div className="layout">
        {/* LEFT: GRAPH */}
        <div className="card graph-card">
          <h2>Window Counts Over Time</h2>

          <div className="chart-wrapper">
            {data.length === 0 ? (
              <p className="empty">Waiting for streaming data...</p>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="ts" angle={-20} textAnchor="end" height={50} />
                  <YAxis dataKey="count" />
                  <Tooltip />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="count"
                    stroke="#2563eb"
                    strokeWidth={3}
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>

        {/* RIGHT: LATE EVENTS + FORM (form pinned to bottom) */}
        <div className="card side-card">
          <h2>Late Events</h2>

          <div className="late-list">
            <ul>
              {late.length === 0 ? (
                <li className="empty">No late events</li>
              ) : (
                late.map(e => (
                  <li key={e.id}>
                    <strong>{e.id.slice(0,6)}</strong> → {tsLabel(e.ts)}
                    &nbsp; (value {Number(e.value).toFixed(2)})
                  </li>
                ))
              )}
            </ul>
          </div>

          {/* footer form stays at bottom because .side-card is a column flex container */}
          <div className="form-footer">
            <h3 style={{margin: "8px 0 6px 0"}}>Send Manual Event</h3>
            <input
              className="input"
              placeholder="timestamp (ms epoch)"
              value={manualTs}
              onChange={e=>setManualTs(e.target.value)}
            />
            <input
              className="input"
              placeholder="value"
              value={manualVal}
              onChange={e=>setManualVal(e.target.value)}
            />
            <button className="btn" onClick={sendManual}>Send</button>
          </div>
        </div>
      </div>
    </div>
  );
}
