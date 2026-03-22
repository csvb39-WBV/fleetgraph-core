import React from "react";

export function AppShell(): JSX.Element {
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
            Header / title area
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
        <div
          style={{
            fontSize: "11px",
            fontWeight: 600,
            textTransform: "uppercase",
            color: "#52606d",
            letterSpacing: "0.08em",
            marginBottom: "12px",
          }}
        >
          Left navigation region
        </div>
        <ul
          style={{
            margin: 0,
            padding: 0,
            listStyle: "none",
            lineHeight: 2,
          }}
        >
          <li>Priority Dashboard</li>
          <li>Company Intelligence</li>
          <li>Predictive Insights</li>
          <li>RFP Panel</li>
        </ul>
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
          aria-label="Priority Dashboard"
          style={{
            background: "#ffffff",
            border: "1px solid #d9e2ec",
            padding: "16px",
            borderRadius: "4px",
          }}
        >
          Priority Dashboard
        </section>

        <section
          aria-label="Company Intelligence"
          style={{
            background: "#ffffff",
            border: "1px solid #d9e2ec",
            padding: "16px",
            borderRadius: "4px",
          }}
        >
          Company Intelligence
        </section>

        <section
          aria-label="Predictive Insights"
          style={{
            background: "#ffffff",
            border: "1px solid #d9e2ec",
            padding: "16px",
            borderRadius: "4px",
          }}
        >
          Predictive Insights
        </section>

        <section
          aria-label="RFP Panel"
          style={{
            background: "#ffffff",
            border: "1px solid #d9e2ec",
            padding: "16px",
            borderRadius: "4px",
          }}
        >
          RFP Panel
        </section>
      </main>
    </div>
  );
}

export default AppShell;
