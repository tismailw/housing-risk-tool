import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";

function App() {
  const api = import.meta.env.VITE_API_URL || "http://localhost:8000";

  const [stateCode, setStateCode] = useState("VA");
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [error, setError] = useState("");

  // Suggestions state
  const [suggestions, setSuggestions] = useState([]);
  const [activeIdx, setActiveIdx] = useState(-1);
  const abortRef = useRef(null);
  const debounceRef = useRef(null);

  const highlight = (text, q) => {
    if (!q) return text;
    const idx = text.toLowerCase().indexOf(q.toLowerCase());
    if (idx === -1) return text;
    return (
      <>
        {text.slice(0, idx)}
        <strong>{text.slice(idx, idx + q.length)}</strong>
        {text.slice(idx + q.length)}
      </>
    );
  };

  const runSearch = async () => {
    setError("");
    setResults([]);

    const sc = (stateCode || "").trim().toUpperCase();
    if (!/^[A-Z]{2}$/.test(sc)) {
      setError("Please enter a 2‑letter state code (e.g., VA).");
      return;
    }

    try {
      const res = await fetch(`${api}/api/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ state: sc, q: query || undefined }), // no default limit
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data?.message || data?.error || `HTTP ${res.status}`);
      setResults(Array.isArray(data) ? data : data.items ?? []);
    } catch (e) {
      console.error(e);
      setError(String(e.message || e));
    }
  };

  // Suggestion fetcher (debounced + cancellable)
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);

    const sc = (stateCode || "").trim().toUpperCase();
    const q = (query || "").trim();

    if (q.length < 2 || !/^[A-Z]{2}$/.test(sc)) {
      setSuggestions([]);
      setActiveIdx(-1);
      return;
    }

    debounceRef.current = setTimeout(async () => {
      if (abortRef.current) abortRef.current.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        // Try /api/suggest first
        let res = await fetch(
          `${api}/api/suggest?state=${encodeURIComponent(sc)}&q=${encodeURIComponent(q)}`,
          { signal: controller.signal }
        );

        // Fallback: use /api/search (small limit) and derive names
        if (res.status === 404) {
          res = await fetch(`${api}/api/search`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ state: sc, q, limit: 7 }),
            signal: controller.signal,
          });
        }

        const data = await res.json().catch(() => []);
        let list = [];
        if (Array.isArray(data)) list = data;
        else if (Array.isArray(data.items)) list = data.items;

        const names = Array.from(
          new Set(
            list
              .map((r) => r?.name || r?.county || r?.city || r?.label)
              .filter(Boolean)
          )
        ).slice(0, 7);

        setSuggestions(names);
        setActiveIdx(-1);
      } catch (e) {
        if (e.name !== "AbortError") {
          console.warn("suggest error", e);
          setSuggestions([]);
          setActiveIdx(-1);
        }
      }
    }, 180);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      if (abortRef.current) abortRef.current.abort();
    };
  }, [query, stateCode, api]);

  const pickSuggestion = (s) => {
    setQuery(s);
    setSuggestions([]);
    setActiveIdx(-1);
  };

  const onKeyDown = (e) => {
    // Tab to autocomplete first suggestion
    if (e.key === "Tab" && suggestions.length) {
      e.preventDefault();
      pickSuggestion(suggestions[0]);
      return;
    }
    if (!suggestions.length) {
      if (e.key === "Enter") runSearch();
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIdx((i) => (i + 1) % suggestions.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIdx((i) => (i - 1 + suggestions.length) % suggestions.length);
    } else if (e.key === "Enter") {
      if (activeIdx >= 0) {
        e.preventDefault();
        pickSuggestion(suggestions[activeIdx]);
      } else {
        runSearch();
      }
    } else if (e.key === "Escape") {
      setSuggestions([]);
      setActiveIdx(-1);
    }
  };

  return (
    <div style={{ fontFamily: "system-ui", padding: 30 }}>
      <h1>AI Mortgage Risk Tool</h1>

      <div
        style={{
          display: "flex",
          gridTemplateColumns: "120px 1fr auto",
          gap: 10,
          position: "relative",
          alignItems: "end",
          width: "100%",                // make row span full width
          maxWidth: 880,                // optional: keep it from stretching too wide

        }}
      >
        <label>
          State
          <input
            value={stateCode}
            onChange={(e) => setStateCode(e.target.value.toUpperCase())}
            maxLength={2}
            placeholder="State"
          />
        </label>

        <label style={{ position: "relative", flex: 1 }}>
          County
          <input
            placeholder="..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            autoComplete="off"
            role="combobox"
            aria-expanded={suggestions.length > 0}
            aria-autocomplete="list"
            aria-controls="suggest-list"
          />
          {suggestions.length > 0 && (
            <div
              id="suggest-list"
              role="listbox"
              style={{
                position: "absolute",
                zIndex: 10,
                top: "100%",
                left: 0,
                right: 0,
                background: "white",
                border: "1px solid #ddd",
                borderTop: "none",
                boxShadow: "0 8px 16px rgba(0,0,0,0.08)",
                maxHeight: 240,
                overflowY: "auto",
              }}
            >
              {suggestions.map((s, i) => (
                <div
                  key={`${s}-${i}`}
                  role="option"
                  aria-selected={i === activeIdx}
                  onMouseDown={() => pickSuggestion(s)}
                  onMouseEnter={() => setActiveIdx(i)}
                  style={{
                    padding: "8px 10px",
                    cursor: "pointer",
                    background: i === activeIdx ? "#f5f5f5" : "white",
                    borderTop: "1px solid #eee",
                    display: "flex",
                    gap: 8,
                    alignItems: "center",
                  }}
                >
                  {/* Optional pin icon look */}
                  <span style={{ fontSize: 12 }}>📍</span>
                  <span>{highlight(s, query)}</span>
                </div>
              ))}
            </div>
          )}
        </label>

        <button
          type="button"
          onClick={runSearch}
          style={{
            justifySelf: "end",         // push button to the right edge
            alignSelf: "end",           // align with inputs
            height: 32,                 // optional: match input height
            padding: "0 14px",
          }}
        >
          Search
        </button>
      </div>

      {error && <p style={{ color: "red", marginTop: 12 }}>Error: {error}</p>}

      {results.map(r => (
        <div key={r.geo_id} style={{ border: "1px solid #ddd", marginTop: 10, padding: 10 }}>
          <h3>{r.name}</h3>
          <p><b>Recommendation score:</b> {r.overall_score}</p>
          <p>FEMA NRI overall risk (lower = safer): {r.fema_risk_score}</p>
          {r.state_rank !== undefined ? (
            <p><b>State Rank (risk):</b> {r.state_rank}</p>
          ) : null}
          <small>{r.explanation}</small>
        </div>
      ))}

      {!results.length && !error && (
        <p style={{ marginTop: 12, color: "#666" }}>
          Enter a state and start typing — suggestions appear below. Press{" "}
          <kbd>Tab</kbd> to autocomplete, <kbd>Enter</kbd> to search.
        </p>
      )}
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
