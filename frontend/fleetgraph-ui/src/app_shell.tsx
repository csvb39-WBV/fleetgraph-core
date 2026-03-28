import React, { useState } from "react";

import FleetGraphNavigation, {
  type FleetGraphViewKey,
} from "./components/navigation/FleetGraphNavigation";
import ViewContainer from "./components/navigation/ViewContainer";

export function AppShell(): JSX.Element {
  const [activeView, setActiveView] = useState<FleetGraphViewKey>("watchlist-console");

  return (
    <div
      aria-label="FleetGraph App Shell"
      style={{
        display: "grid",
        gridTemplateRows: "72px 1fr",
        gridTemplateColumns: "280px 1fr",
        gridTemplateAreas: '"header header" "nav main"',
        minHeight: "100vh",
        fontFamily: "Segoe UI, Tahoma, Geneva, Verdana, sans-serif",
        background: "#f4f6f8",
        color: "#1f2933",
      }}
    >
      <header
        aria-label="Header / title area"
        style={{
          gridArea: "header",
          display: "flex",
          alignItems: "center",
          padding: "0 24px",
          borderBottom: "1px solid #d9e2ec",
          background: "#ffffff",
          fontWeight: 600,
          letterSpacing: "0.02em",
        }}
      >
        <div>
          <div style={{ fontSize: "18px" }}>FleetGraph Operator Console</div>
          <div style={{ fontSize: "12px", color: "#52606d", fontWeight: 400 }}>
            Watchlist Mode is the current operating priority.
          </div>
        </div>
      </header>

      <nav
        aria-label="Left navigation region"
        style={{
          gridArea: "nav",
          borderRight: "1px solid #d9e2ec",
          background: "#ffffff",
          padding: "20px 16px",
        }}
      >
        <FleetGraphNavigation activeView={activeView} onSelectView={setActiveView} />
      </nav>

      <main
        aria-label="Main content region"
        style={{
          gridArea: "main",
          padding: "20px",
          display: "grid",
          gap: "12px",
          alignContent: "start",
        }}
      >
        <div
          style={{
            fontSize: "11px",
            fontWeight: 600,
            textTransform: "uppercase",
            color: "#52606d",
            letterSpacing: "0.08em",
          }}
        >
          Main content region
        </div>

        <section
          aria-label="Active Workflow View"
          style={{
            background: "#ffffff",
            border: "1px solid #d9e2ec",
            padding: "16px",
            borderRadius: "4px",
          }}
        >
          <ViewContainer activeView={activeView} />
        </section>
      </main>
    </div>
  );
}

export default AppShell;
