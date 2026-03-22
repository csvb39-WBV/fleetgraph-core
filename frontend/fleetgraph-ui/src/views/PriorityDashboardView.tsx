import React, { useEffect, useMemo, useState } from "react";

import { getPriorityDashboard } from "../api/client";
import type {
  ApiPayload,
  DashboardRecordResponse,
  PriorityDashboardResponse,
} from "../api/types";
import DashboardStatePanel, {
  type DashboardState,
} from "../components/dashboard/DashboardStatePanel";
import PipelineRecordTable, {
  type PipelineRecordRow,
} from "../components/dashboard/PipelineRecordTable";
import PrioritySummaryCards from "../components/dashboard/PrioritySummaryCards";


type Props = {
  payload: ApiPayload;
};

function derivePriorityScore(record: DashboardRecordResponse): number {
  if (typeof record.priority_score === "number") {
    return record.priority_score;
  }
  if (typeof record.opportunity_score === "number") {
    return record.opportunity_score;
  }
  if (record.priority === "HIGH") {
    return 90;
  }
  if (record.priority === "MEDIUM") {
    return 60;
  }
  if (record.priority === "LOW") {
    return 30;
  }
  return 0;
}

function mapRecord(
  record: DashboardRecordResponse,
  index: number,
  fallbackCompany: string
): PipelineRecordRow {
  const priorityScore = derivePriorityScore(record);
  return {
    record_id: record.record_id ?? `record-${index}`,
    company_name: record.company_name ?? fallbackCompany,
    opportunity_title: record.opportunity_title ?? record.reason ?? "N/A",
    priority_score: priorityScore,
    stage: record.stage ?? "READY",
    top_icp: record.top_icp ?? record.icp ?? "N/A",
  };
}

function deterministicSort(records: PipelineRecordRow[]): PipelineRecordRow[] {
  return [...records].sort((a, b) => {
    if (b.priority_score !== a.priority_score) {
      return b.priority_score - a.priority_score;
    }
    if (a.company_name !== b.company_name) {
      return a.company_name.localeCompare(b.company_name);
    }
    return a.record_id.localeCompare(b.record_id);
  });
}

function calculateHighPriorityRecords(records: PipelineRecordRow[]): number {
  return records.filter((record) => record.priority_score >= 80).length;
}

function calculateActiveCompanies(records: PipelineRecordRow[]): number {
  return new Set(records.map((record) => record.company_name)).size;
}


export function PriorityDashboardView({ payload }: Props): JSX.Element {
  const [state, setState] = useState<DashboardState>("loading");
  const [data, setData] = useState<PriorityDashboardResponse | null>(null);

  useEffect(() => {
    let cancelled = false;

    setState("loading");
    setData(null);

    getPriorityDashboard(payload)
      .then((response) => {
        if (cancelled) {
          return;
        }
        setData(response);
        if (response.records.length === 0) {
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

  const mappedSortedRecords = useMemo(() => {
    if (!data) {
      return [];
    }
    const mapped = data.records.map((record, index) =>
      mapRecord(record, index, data.company_id)
    );
    return deterministicSort(mapped);
  }, [data]);

  const summary = useMemo(() => {
    if (!data) {
      return {
        totalPipelineRecords: 0,
        highPriorityRecords: 0,
        activeCompanies: 0,
      };
    }

    // API summary remains the source of truth for total/high-priority counts.
    return {
      totalPipelineRecords: data.dashboard_summary.total_records,
      highPriorityRecords: data.dashboard_summary.high_priority_count,
      // Active companies is normalized from mapped records because API does not provide it directly.
      activeCompanies: calculateActiveCompanies(mappedSortedRecords),
    };
  }, [data, mappedSortedRecords]);

  return (
    <section aria-label="Priority Dashboard View">
      <h2>Priority Dashboard</h2>

      <DashboardStatePanel state={state} />

      {(state === "success" || state === "empty") && (
        <>
          <PrioritySummaryCards
            totalPipelineRecords={summary.totalPipelineRecords}
            highPriorityRecords={summary.highPriorityRecords}
            activeCompanies={summary.activeCompanies}
          />
          {state === "success" && <PipelineRecordTable records={mappedSortedRecords} />}
        </>
      )}
    </section>
  );
}


export default PriorityDashboardView;