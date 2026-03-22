import React, { useEffect, useMemo, useState } from "react";

import { getCompanyIntelligence } from "../api/client";
import type { ApiPayload } from "../api/types";
import CompanyIntelligenceStatePanel, {
  type CompanyIntelligenceState,
} from "../components/company_intelligence/CompanyIntelligenceStatePanel";
import CompanyIntelligenceSummary from "../components/company_intelligence/CompanyIntelligenceSummary";
import OpportunityList, {
  type OpportunityRow,
} from "../components/company_intelligence/OpportunityList";
import ProspectList, {
  type ProspectRow,
} from "../components/company_intelligence/ProspectList";
import RelatedPipelineRecords, {
  type PipelineRecordRow,
} from "../components/company_intelligence/RelatedPipelineRecords";


type NormalizedCompanyIntelligenceSummary = {
  companyName: string;
  highestScore: number;
  prioritySummary: string;
};

type AnyRecord = Record<string, unknown>;

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

function normalizeSummary(data: AnyRecord): NormalizedCompanyIntelligenceSummary {
  const ci = data.company_intelligence as AnyRecord | undefined;
  return {
    companyName:
      asString(data.company_name, "") ||
      asString(data.company_id, "") ||
      "N/A",
    highestScore:
      asNumber(data.highest_score) ||
      asNumber(ci?.highest_opportunity_score) ||
      0,
    prioritySummary:
      asString(data.priority_summary, "") ||
      asString(ci?.highest_priority, "") ||
      "N/A",
  };
}

function normalizeOpportunity(item: AnyRecord, index: number): OpportunityRow {
  return {
    opportunity_id: asString(item.opportunity_id, `opportunity-${index}`),
    title:
      asString(item.title, "") ||
      asString(item.reason, "") ||
      asString(item.icp, "") ||
      "N/A",
    score: asNumber(item.score) || asNumber(item.opportunity_score) || 0,
    stage: asString(item.stage, "") || "N/A",
  };
}

function normalizeProspect(item: AnyRecord, index: number): ProspectRow {
  return {
    prospect_id: asString(item.prospect_id, `prospect-${index}`),
    name:
      asString(item.name, "") ||
      asString(item.company_id, "") ||
      "N/A",
    role:
      asString(item.role, "") ||
      asString(item.icp, "") ||
      "N/A",
    priority: asString(item.priority, "") || "N/A",
  };
}

function normalizePipelineRecord(
  item: AnyRecord,
  index: number,
  data: AnyRecord
): PipelineRecordRow {
  return {
    record_id: asString(item.record_id, `record-${index}`),
    company_name:
      asString(item.company_name, "") ||
      asString(item.company_id, "") ||
      asString(data.company_name, "") ||
      asString(data.company_id, "") ||
      "N/A",
    opportunity_title:
      asString(item.opportunity_title, "") ||
      asString(item.reason, "") ||
      asString(item.icp, "") ||
      "N/A",
    priority_score:
      asNumber(item.priority_score) || asNumber(item.opportunity_score) || 0,
    stage: asString(item.stage, "") || "N/A",
  };
}

function sortOpportunities(items: OpportunityRow[]): OpportunityRow[] {
  return [...items].sort((a, b) => {
    if (b.score !== a.score) {
      return b.score - a.score;
    }
    if (a.title !== b.title) {
      return a.title.localeCompare(b.title);
    }
    return a.opportunity_id.localeCompare(b.opportunity_id);
  });
}

function sortProspects(items: ProspectRow[]): ProspectRow[] {
  return [...items].sort((a, b) => {
    const aRank = PRIORITY_RANK[a.priority] ?? 3;
    const bRank = PRIORITY_RANK[b.priority] ?? 3;
    if (aRank !== bRank) {
      return aRank - bRank;
    }
    if (a.name !== b.name) {
      return a.name.localeCompare(b.name);
    }
    return a.prospect_id.localeCompare(b.prospect_id);
  });
}

function sortPipelineRecords(items: PipelineRecordRow[]): PipelineRecordRow[] {
  return [...items].sort((a, b) => {
    if (b.priority_score !== a.priority_score) {
      return b.priority_score - a.priority_score;
    }
    if (a.company_name !== b.company_name) {
      return a.company_name.localeCompare(b.company_name);
    }
    return a.record_id.localeCompare(b.record_id);
  });
}


export function CompanyIntelligenceView({ payload }: Props): JSX.Element {
  const [state, setState] = useState<CompanyIntelligenceState>("loading");
  const [data, setData] = useState<AnyRecord | null>(null);

  useEffect(() => {
    let cancelled = false;

    setState("loading");
    setData(null);

    getCompanyIntelligence(payload)
      .then((response) => {
        if (cancelled) {
          return;
        }

        const typed = response as AnyRecord;
        setData(typed);

        const opps = (typed.opportunities as unknown[]) ?? [];
        const prospects = (typed.prospects as unknown[]) ?? [];
        const pipeline = (typed.pipeline_records as unknown[]) ?? [];

        if (opps.length === 0 && prospects.length === 0 && pipeline.length === 0) {
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

  const summary = useMemo((): NormalizedCompanyIntelligenceSummary => {
    if (!data) {
      return { companyName: "N/A", highestScore: 0, prioritySummary: "N/A" };
    }
    return normalizeSummary(data);
  }, [data]);

  const opportunities = useMemo(() => {
    if (!data) {
      return [];
    }
    const raw = (data.opportunities as AnyRecord[]) ?? [];
    return sortOpportunities(raw.map((item, i) => normalizeOpportunity(item, i)));
  }, [data]);

  const prospects = useMemo(() => {
    if (!data) {
      return [];
    }
    const raw = (data.prospects as AnyRecord[]) ?? [];
    return sortProspects(raw.map((item, i) => normalizeProspect(item, i)));
  }, [data]);

  const pipelineRecords = useMemo(() => {
    if (!data) {
      return [];
    }
    const raw = (data.pipeline_records as AnyRecord[]) ?? [];
    return sortPipelineRecords(raw.map((item, i) => normalizePipelineRecord(item, i, data)));
  }, [data]);

  return (
    <section aria-label="Company Intelligence View">
      <h2>Company Intelligence</h2>

      <CompanyIntelligenceStatePanel state={state} />

      {(state === "success" || state === "empty") && (
        <>
          <CompanyIntelligenceSummary
            companyName={summary.companyName}
            highestScore={summary.highestScore}
            prioritySummary={summary.prioritySummary}
          />

          {state === "success" && (
            <>
              <OpportunityList opportunities={opportunities} />
              <ProspectList prospects={prospects} />
              <RelatedPipelineRecords records={pipelineRecords} />
            </>
          )}
        </>
      )}
    </section>
  );
}


export default CompanyIntelligenceView;
