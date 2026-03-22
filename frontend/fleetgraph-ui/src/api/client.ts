import {
  getPriorityDashboard as getPriorityDashboardBase,
  getCompanyIntelligence as getCompanyIntelligenceBase,
  getPredictiveInsights as getPredictiveInsightsBase,
  getRfpPanel as getRfpPanelBase,
} from "../api_client";
import type { ApiPayload, PriorityDashboardResponse } from "./types";


export async function getPriorityDashboard(
  payload: ApiPayload
): Promise<PriorityDashboardResponse> {
  return getPriorityDashboardBase(payload) as Promise<PriorityDashboardResponse>;
}

export async function getCompanyIntelligence(
  payload: ApiPayload
): Promise<Record<string, unknown>> {
  return getCompanyIntelligenceBase(payload);
}

export async function getPredictiveInsights(
  payload: ApiPayload
): Promise<Record<string, unknown>> {
  return getPredictiveInsightsBase(payload);
}

export async function getRfpPanel(
  payload: ApiPayload
): Promise<Record<string, unknown>> {
  return getRfpPanelBase(payload);
}