import React, { useEffect, useMemo, useState } from "react";

import { getPredictiveInsights } from "../api/client";
import type { ApiPayload } from "../api/types";
import ImmediateActionsList, {
  type ImmediateActionRow,
} from "../components/predictive_insights/ImmediateActionsList";
import NextBestIcpPanel from "../components/predictive_insights/NextBestIcpPanel";
import PredictiveInsightsStatePanel, {
  type PredictiveInsightsState,
} from "../components/predictive_insights/PredictiveInsightsStatePanel";
import PredictiveInsightsSummary from "../components/predictive_insights/PredictiveInsightsSummary";


type AnyRecord = Record<string, unknown>;

type NormalizedPredictiveSummary = {
  predictedDemandScore: number;
  confidenceLevel: string;
  forecastWindow: string;
};

type NormalizedNextBestIcp = {
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

function normalizeSummary(data: AnyRecord): NormalizedPredictiveSummary {
  const predictiveSummary = (data.predictive_summary as AnyRecord | undefined) ?? {};
  const predictiveInsights = (data.predictive_insights as AnyRecord | undefined) ?? {};

  return {
    predictedDemandScore:
      asNumber(data.predicted_demand_score) ||
      asNumber(predictiveSummary.predicted_demand_score) ||
      asNumber(predictiveInsights.highest_opportunity_score) ||
      0,
    confidenceLevel:
      asString(data.confidence_level, "") ||
      asString(predictiveSummary.confidence_level, "") ||
      "N/A",
    forecastWindow:
      asString(data.forecast_window, "") ||
      asString(predictiveSummary.forecast_window, "") ||
      "N/A",
  };
}

function normalizeNextBestIcp(data: AnyRecord): NormalizedNextBestIcp {
  const nextBestIcp = (data.next_best_icp as AnyRecord | undefined) ?? {};
  const predictiveInsights = (data.predictive_insights as AnyRecord | undefined) ?? {};

  return {
    icpName:
      asString(nextBestIcp.icp_name, "") ||
      asString(nextBestIcp.name, "") ||
      asString(predictiveInsights.next_best_icp, "") ||
      asString(data.best_icp, "") ||
      "N/A",
    fitScore:
      asNumber(nextBestIcp.fit_score) ||
      asNumber(nextBestIcp.score) ||
      asNumber(predictiveInsights.highest_opportunity_score) ||
      0,
    rationale:
      asString(nextBestIcp.rationale, "") ||
      asString(nextBestIcp.reason, "") ||
      "N/A",
  };
}

function normalizeImmediateAction(item: AnyRecord, index: number): ImmediateActionRow {
  return {
    action_id: asString(item.action_id, `action-${index}`),
    action_title:
      asString(item.action_title, "") ||
      asString(item.title, "") ||
      asString(item.reason, "") ||
      "N/A",
    action_priority:
      asString(item.action_priority, "") ||
      asString(item.priority, "") ||
      "N/A",
  };
}

function sortImmediateActions(items: ImmediateActionRow[]): ImmediateActionRow[] {
  return [...items].sort((a, b) => {
    const aRank = PRIORITY_RANK[a.action_priority] ?? 3;
    const bRank = PRIORITY_RANK[b.action_priority] ?? 3;
    if (aRank !== bRank) {
      return aRank - bRank;
    }
    if (a.action_title !== b.action_title) {
      return a.action_title.localeCompare(b.action_title);
    }
    return a.action_id.localeCompare(b.action_id);
  });
}


export function PredictiveInsightsView({ payload }: Props): JSX.Element {
  const [state, setState] = useState<PredictiveInsightsState>("loading");
  const [data, setData] = useState<AnyRecord | null>(null);

  useEffect(() => {
    let cancelled = false;

    setState("loading");
    setData(null);

    getPredictiveInsights(payload)
      .then((response) => {
        if (cancelled) {
          return;
        }

        const typed = response as AnyRecord;
        setData(typed);

        const immediateActionsRaw = Array.isArray(typed.immediate_actions)
          ? (typed.immediate_actions as unknown[])
          : Array.isArray(typed.timed_prospects)
            ? (typed.timed_prospects as unknown[])
            : [];

        const nextBest = normalizeNextBestIcp(typed);
        const summary = normalizeSummary(typed);

        const hasMeaningfulSummary =
          summary.predictedDemandScore !== 0 ||
          summary.confidenceLevel !== "N/A" ||
          summary.forecastWindow !== "N/A";
        const hasMeaningfulNextBest =
          nextBest.icpName !== "N/A" ||
          nextBest.fitScore !== 0 ||
          nextBest.rationale !== "N/A";

        if (immediateActionsRaw.length === 0 && !hasMeaningfulSummary && !hasMeaningfulNextBest) {
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

  const summary = useMemo((): NormalizedPredictiveSummary => {
    if (!data) {
      return {
        predictedDemandScore: 0,
        confidenceLevel: "N/A",
        forecastWindow: "N/A",
      };
    }

    return normalizeSummary(data);
  }, [data]);

  const nextBestIcp = useMemo((): NormalizedNextBestIcp => {
    if (!data) {
      return {
        icpName: "N/A",
        fitScore: 0,
        rationale: "N/A",
      };
    }

    return normalizeNextBestIcp(data);
  }, [data]);

  const immediateActions = useMemo(() => {
    if (!data) {
      return [];
    }

    const raw = Array.isArray(data.immediate_actions)
      ? (data.immediate_actions as AnyRecord[])
      : Array.isArray(data.timed_prospects)
        ? ((data.timed_prospects as AnyRecord[]).filter(
            (item) => asString(item.timing, "") === "IMMEDIATE"
          ) as AnyRecord[])
        : [];

    const mapped = raw.map((item, index) => normalizeImmediateAction(item, index));

    return sortImmediateActions(mapped);
  }, [data]);

  return (
    <section aria-label="Predictive Insights View">
      <h2>Predictive Insights</h2>

      <PredictiveInsightsStatePanel state={state} />

      {(state === "success" || state === "empty") && (
        <>
          <PredictiveInsightsSummary
            predictedDemandScore={summary.predictedDemandScore}
            confidenceLevel={summary.confidenceLevel}
            forecastWindow={summary.forecastWindow}
          />

          {state === "success" && (
            <>
              <NextBestIcpPanel
                icpName={nextBestIcp.icpName}
                fitScore={nextBestIcp.fitScore}
                rationale={nextBestIcp.rationale}
              />
              <ImmediateActionsList actions={immediateActions} />
            </>
          )}
        </>
      )}
    </section>
  );
}


export default PredictiveInsightsView;