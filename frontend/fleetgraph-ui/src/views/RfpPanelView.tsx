import React, { useEffect, useMemo, useState } from "react";

import { getRfpPanel } from "../api/client";
import type { ApiPayload } from "../api/types";
import HighPrioritySignalsList, {
  type HighPrioritySignalRow,
} from "../components/rfp_panel/HighPrioritySignalsList";
import RfpPanelStatePanel, {
  type RfpPanelState,
} from "../components/rfp_panel/RfpPanelStatePanel";
import RfpPanelSummary from "../components/rfp_panel/RfpPanelSummary";
import TopIcpPanel from "../components/rfp_panel/TopIcpPanel";


type AnyRecord = Record<string, unknown>;

type NormalizedRfpSummary = {
  totalSignals: number;
  highPrioritySignals: number;
  panelStatus: string;
};

type NormalizedTopIcp = {
  icpName: string;
  fitScore: number;
  rationale: string;
};

type Props = {
  payload: ApiPayload;
};

const PRIORITY_RANK: Record<string, number> = {
  HIGH: 0,
  MEDIUM: 1,
  LOW: 2,
};

function asNumber(value: unknown): number {
  return typeof value === "number" ? value : 0;
}

function asString(value: unknown, fallback: string): string {
  return typeof value === "string" ? value : fallback;
}

function normalizeSummary(data: AnyRecord): NormalizedRfpSummary {
  const rfpSummary = (data.rfp_summary as AnyRecord | undefined) ?? {};
  const rfpPanel = (data.rfp_panel as AnyRecord | undefined) ?? {};

  return {
    totalSignals:
      asNumber(data.total_signals) ||
      asNumber(rfpSummary.total_signals) ||
      asNumber(rfpPanel.signal_count) ||
      0,
    highPrioritySignals:
      asNumber(data.high_priority_signals) ||
      asNumber(rfpSummary.high_priority_signals) ||
      asNumber(rfpPanel.high_priority_signal_count) ||
      0,
    panelStatus:
      asString(data.panel_status, "") ||
      asString(rfpSummary.panel_status, "") ||
      "N/A",
  };
}

function normalizeTopIcp(data: AnyRecord): NormalizedTopIcp {
  const topIcp = (data.top_icp as AnyRecord | undefined) ?? {};
  const rfpPanel = (data.rfp_panel as AnyRecord | undefined) ?? {};

  return {
    icpName:
      asString(topIcp.icp_name, "") ||
      asString(topIcp.name, "") ||
      asString(rfpPanel.top_icp, "") ||
      asString(data.best_icp, "") ||
      "N/A",
    fitScore:
      asNumber(topIcp.fit_score) ||
      asNumber(topIcp.score) ||
      0,
    rationale:
      asString(topIcp.rationale, "") ||
      asString(topIcp.reason, "") ||
      "N/A",
  };
}

function normalizeSignal(item: AnyRecord, index: number): HighPrioritySignalRow {
  return {
    signal_id: asString(item.signal_id, `signal-${index}`),
    signal_title:
      asString(item.signal_title, "") ||
      asString(item.title, "") ||
      asString(item.reason, "") ||
      "N/A",
    signal_priority:
      asString(item.signal_priority, "") ||
      asString(item.priority, "") ||
      "N/A",
  };
}

function sortSignals(items: HighPrioritySignalRow[]): HighPrioritySignalRow[] {
  return [...items].sort((a, b) => {
    const aRank = PRIORITY_RANK[a.signal_priority] ?? 3;
    const bRank = PRIORITY_RANK[b.signal_priority] ?? 3;
    if (aRank !== bRank) {
      return aRank - bRank;
    }
    if (a.signal_title !== b.signal_title) {
      return a.signal_title.localeCompare(b.signal_title);
    }
    return a.signal_id.localeCompare(b.signal_id);
  });
}


export function RfpPanelView({ payload }: Props): JSX.Element {
  const [state, setState] = useState<RfpPanelState>("loading");
  const [data, setData] = useState<AnyRecord | null>(null);

  useEffect(() => {
    let cancelled = false;

    setState("loading");
    setData(null);

    getRfpPanel(payload)
      .then((response) => {
        if (cancelled) {
          return;
        }

        const typed = response as AnyRecord;
        setData(typed);

        const rawSignals = Array.isArray(typed.high_priority_signals)
          ? (typed.high_priority_signals as unknown[])
          : Array.isArray(typed.pipeline_records)
            ? (typed.pipeline_records as unknown[])
            : [];
        const summary = normalizeSummary(typed);
        const topIcp = normalizeTopIcp(typed);
        const hasMeaningfulSummary =
          summary.totalSignals !== 0 ||
          summary.highPrioritySignals !== 0 ||
          summary.panelStatus !== "N/A";
        const hasMeaningfulTopIcp =
          topIcp.icpName !== "N/A" ||
          topIcp.fitScore !== 0 ||
          topIcp.rationale !== "N/A";

        if (rawSignals.length === 0 && !hasMeaningfulSummary && !hasMeaningfulTopIcp) {
          setState("empty");
        } else {
          setState("success");
        }
      })
      .catch(() => {
        if (cancelled) {
          return;
        }
        setState("error");
      });

    return () => {
      cancelled = true;
    };
  }, [payload]);

  const summary = useMemo((): NormalizedRfpSummary => {
    if (!data) {
      return {
        totalSignals: 0,
        highPrioritySignals: 0,
        panelStatus: "N/A",
      };
    }

    return normalizeSummary(data);
  }, [data]);

  const topIcp = useMemo((): NormalizedTopIcp => {
    if (!data) {
      return {
        icpName: "N/A",
        fitScore: 0,
        rationale: "N/A",
      };
    }

    return normalizeTopIcp(data);
  }, [data]);

  const highPrioritySignals = useMemo(() => {
    if (!data) {
      return [];
    }

    const raw = Array.isArray(data.high_priority_signals)
      ? (data.high_priority_signals as AnyRecord[])
      : Array.isArray(data.pipeline_records)
        ? ((data.pipeline_records as AnyRecord[]).filter(
            (item) => asString(item.priority, "") === "HIGH"
          ) as AnyRecord[])
        : [];

    const mapped = raw.map((item, index) => normalizeSignal(item, index));

    return sortSignals(mapped);
  }, [data]);

  return (
    <section aria-label="RFP Panel View">
      <h2>RFP Panel</h2>

      <RfpPanelStatePanel state={state} />

      {(state === "success" || state === "empty") && (
        <>
          <RfpPanelSummary
            totalSignals={summary.totalSignals}
            highPrioritySignals={summary.highPrioritySignals}
            panelStatus={summary.panelStatus}
          />

          {state === "success" && (
            <>
              <TopIcpPanel
                icpName={topIcp.icpName}
                fitScore={topIcp.fitScore}
                rationale={topIcp.rationale}
              />
              <HighPrioritySignalsList signals={highPrioritySignals} />
            </>
          )}
        </>
      )}
    </section>
  );
}


export default RfpPanelView;