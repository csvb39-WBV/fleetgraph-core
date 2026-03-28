import React from "react";


export type FleetGraphViewKey =
  | "priority-dashboard"
  | "watchlist-console"
  | "company-intelligence"
  | "predictive-insights"
  | "rfp-panel";

type NavigationItem = {
  key: FleetGraphViewKey;
  label: string;
};

type Props = {
  activeView: FleetGraphViewKey;
  onSelectView: (view: FleetGraphViewKey) => void;
};

const NAVIGATION_ITEMS: NavigationItem[] = [
  { key: "priority-dashboard", label: "Priority Dashboard" },
  { key: "watchlist-console", label: "Watchlist Console" },
  { key: "company-intelligence", label: "Company Intelligence" },
  { key: "predictive-insights", label: "Predictive Insights" },
  { key: "rfp-panel", label: "RFP Panel" },
];


export function FleetGraphNavigation({ activeView, onSelectView }: Props): JSX.Element {
  return (
    <>
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
          display: "grid",
          gap: "8px",
        }}
      >
        {NAVIGATION_ITEMS.map((item) => {
          const isActive = item.key === activeView;

          return (
            <li key={item.key}>
              <button
                type="button"
                aria-pressed={isActive}
                onClick={() => onSelectView(item.key)}
                style={{
                  width: "100%",
                  textAlign: "left",
                  padding: "10px 12px",
                  borderRadius: "4px",
                  border: isActive ? "1px solid #486581" : "1px solid #d9e2ec",
                  background: isActive ? "#e6f1f7" : "#ffffff",
                  color: "#1f2933",
                  cursor: "pointer",
                  font: "inherit",
                }}
              >
                {item.label}
              </button>
            </li>
          );
        })}
      </ul>
    </>
  );
}


export default FleetGraphNavigation;
