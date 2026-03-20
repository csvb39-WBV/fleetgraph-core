import { DEMO_HEALTH, DEMO_RECORDS, DEMO_SUMMARY } from './demo_fixture.js'

const RAW_API_BASE_URL = import.meta.env.VITE_API_BASE_URL
const API_BASE_URL =
  typeof RAW_API_BASE_URL === 'string' && RAW_API_BASE_URL.length > 0
    ? RAW_API_BASE_URL.replace(/\/+$/, '')
    : 'http://127.0.0.1:8000'

async function getJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`, { method: 'GET' })
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${path}`)
  }
  return response.json()
}

export function getHealth() {
  return getJson('/health').catch(() => DEMO_HEALTH)
}

export async function getRelationshipSignalSummary() {
  try {
    const payload = await getJson('/relationship-signals/summary')
    if (!payload || typeof payload !== 'object' || typeof payload.record_count !== 'number') {
      return DEMO_SUMMARY
    }
    return payload
  } catch {
    return DEMO_SUMMARY
  }
}

export async function getRelationshipSignalRecords() {
  try {
    const payload = await getJson('/relationship-signals/records')
    if (!Array.isArray(payload)) {
      throw new Error('Malformed records response')
    }
    if (payload.length === 0) {
      return DEMO_RECORDS
    }
    return payload
  } catch (error) {
    if (error instanceof Error && error.message === 'Malformed records response') {
      throw error
    }
    return DEMO_RECORDS
  }
}

export function getRelationshipSignalOutput() {
  return getJson('/relationship-signals/output')
}
