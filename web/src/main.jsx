import React, { useState } from "react";
import { createRoot } from "react-dom/client";

function App() {
  const api = import.meta.env.VITE_API_URL || "http://localhost:8000";
  const [stateCode, setStateCode] = useState("VA");
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [error, setError] = useState("");

  const runSearch = async () => {
    console.log("runSearch clicked");
    setError("");
    setResults([]);
    try {
      const res = await fetch(`${api}/api/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ state: stateCode, q: query, limit: 10 }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);
      setResults(Array.isArray(data) ? data : (data.items ?? []));
    } catch (e) {
      console.error(e);
      setError(String(e));
    }
  };

  return (
    <div style={{ fontFamily: "system-ui", padding: 30 }}>
      <h1>AI Mortgage Risk Tool</h1>
      <div style={{ display: "grid", gridTemplateColumns: "120px 1fr 100px", gap: 10 }}>
        <label>State
          <input value={stateCode} onChange={e => setStateCode(e.target.value.toUpperCase())} maxLength={2} />
        </label>
        <label>County
          <input placeholder="e.g., County" value={query} onChange={e => setQuery(e.target.value)} />
        </label>
        <button type="button" onClick={runSearch}>Search</button>
      </div>

      {error && <p style={{ color: "red", marginTop: 12 }}>Error: {error}</p>}

      {results.map(r => (
        <div key={r.geo_id} style={{ border: "1px solid #ddd", marginTop: 10, padding: 10 }}>
          <h3>{r.name}</h3>
          <p>Recommendation score: {r.overall_score}</p>
          <p>FEMA NRI risk (lower = safer): {r.fema_risk_score}</p>
        </div>
      ))}
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
