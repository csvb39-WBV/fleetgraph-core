const API_BASE_URL = 'http://127.0.0.1:8000'

async function getJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`, { method: 'GET' })
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${path}`)
  }
  return response.json()
}

export function getHealth() {
  return getJson('/health')
}

export function getRelationshipSignalSummary() {
  return getJson('/relationship-signals/summary')
}

export function getRelationshipSignalRecords() {
  return getJson('/relationship-signals/records')
}

export function getRelationshipSignalOutput() {
  return getJson('/relationship-signals/output')
}
