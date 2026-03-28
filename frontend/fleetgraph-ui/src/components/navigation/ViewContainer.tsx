import React from "react";

import { CompanyIntelligenceView } from "../../views/CompanyIntelligenceView";
import { PredictiveInsightsView } from "../../views/PredictiveInsightsView";
import { PriorityDashboardView } from "../../views/PriorityDashboardView";
import { RfpPanelView } from "../../views/RfpPanelView";
import { WatchlistConsoleView } from "../../views/WatchlistConsoleView";
import type { FleetGraphViewKey } from "./FleetGraphNavigation";


type Props = {
  activeView: FleetGraphViewKey;
};

const DEFAULT_PAYLOAD = { company_id: "cmp-001" };


export function ViewContainer({ activeView }: Props): JSX.Element {
  if (activeView === "watchlist-console") {
    return <WatchlistConsoleView />;
  }

  if (activeView === "company-intelligence") {
    return <CompanyIntelligenceView payload={DEFAULT_PAYLOAD} />;
  }

  if (activeView === "predictive-insights") {
    return <PredictiveInsightsView payload={DEFAULT_PAYLOAD} />;
  }

  if (activeView === "rfp-panel") {
    return <RfpPanelView payload={DEFAULT_PAYLOAD} />;
  }

  return <PriorityDashboardView payload={DEFAULT_PAYLOAD} />;
}


export default ViewContainer;
