import React from 'react'

export default function Summary({ summary, records }) {
  const supportsOrganizationCount = records.every(
    (record) => typeof record.organization_count === 'number'
  )
  const supportsLinkCount = records.every((record) => typeof record.link_count === 'number')

  const totalOrganizations = supportsOrganizationCount
    ? records.reduce((total, record) => total + record.organization_count, 0)
    : null
  const totalLinks = supportsLinkCount
    ? records.reduce((total, record) => total + record.link_count, 0)
    : null
  const domainCount = records.length

  return (
    <section>
      <h2>Summary</h2>

      <h3>Backend Summary</h3>
      <p>output_type: {summary?.output_type}</p>
      <p>output_schema_version: {summary?.output_schema_version}</p>
      <p>record_count: {summary?.record_count}</p>

      <h3>Derived Counts</h3>
      {totalOrganizations !== null ? <p>total_organizations: {totalOrganizations}</p> : null}
      {totalLinks !== null ? <p>total_links: {totalLinks}</p> : null}
      <p>domain_count: {domainCount}</p>
    </section>
  )
}
