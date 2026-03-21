import React from 'react'

export default function Summary({ summary, records, demoModeEnabled, demoStage }) {
  const cardStyle = {
    border: '1px solid #e5e7eb',
    borderRadius: '8px',
    padding: '16px',
    marginBottom: '16px',
    background: '#ffffff',
  }

  function toReadableLabel(fieldName) {
    return fieldName
      .split('_')
      .map((part) => (part ? part.charAt(0).toUpperCase() + part.slice(1) : part))
      .join(' ')
  }

  function renderValue(value) {
    if (value === null || value === undefined || value === '') {
      return <span>Not provided</span>
    }

    if (Array.isArray(value)) {
      if (value.length === 0) {
        return <span>Not provided</span>
      }

      return (
        <ul style={{ margin: '8px 0 0 16px', display: 'grid', gap: '8px' }}>
          {value.map((item, index) => (
            <li key={index}>{renderValue(item)}</li>
          ))}
        </ul>
      )
    }

    if (typeof value === 'object') {
      const entries = Object.entries(value)
      if (entries.length === 0) {
        return <span>Not provided</span>
      }

      return (
        <div style={{ display: 'grid', gap: '8px' }}>
          {entries.map(([key, nestedValue]) => (
            <div key={key}>
              <strong>{toReadableLabel(key)}:</strong> {renderValue(nestedValue)}
            </div>
          ))}
        </div>
      )
    }

    return <span>{String(value)}</span>
  }

  const supportsOrganizationCount = records.every(
    (record) => typeof record.organization_count === 'number'
  )
  const supportsLinkCount = records.every((record) => typeof record.link_count === 'number')

  const totalRecords =
    summary && typeof summary.record_count === 'number' ? summary.record_count : records.length
  const totalOrganizations = supportsOrganizationCount
    ? records.reduce((total, record) => total + record.organization_count, 0)
    : null
  const totalLinks = supportsLinkCount
    ? records.reduce((total, record) => total + record.link_count, 0)
    : null
  const domainCount = records.length
  const summaryEntries = summary ? Object.entries(summary) : []

  const summaryLinkCount =
    summary && typeof summary.link_count === 'number'
      ? summary.link_count
      : summary && typeof summary.total_links === 'number'
        ? summary.total_links
        : null

  let topObservation =
    'The current dataset is limited in size but still provides a usable starting point for relationship review.'

  if (
    (typeof totalLinks === 'number' && totalLinks >= 4) ||
    (typeof summaryLinkCount === 'number' && summaryLinkCount >= 4)
  ) {
    topObservation =
      'The current dataset shows multiple relationship links, indicating that the observed connections are more likely to be operationally meaningful than incidental.'
  } else if (typeof totalOrganizations === 'number' && totalOrganizations >= 2) {
    topObservation =
      'The current dataset spans multiple organizations, suggesting that the relationship signals extend beyond a single isolated entity.'
  } else if (domainCount >= 2 || records.length >= 2) {
    topObservation =
      'The current dataset includes multiple domain-level records, which may indicate shared infrastructure or broader network affiliation patterns.'
  }

  const hasSharedDomainRelationship = records.some(
    (record) => record && record.relationship_type === 'shared_domain'
  )
  const hasHighLinkPattern = records.some(
    (record) => record && typeof record.link_count === 'number' && record.link_count >= 4
  )
  const hasMultiOrganizationPattern = records.some(
    (record) => record && typeof record.organization_count === 'number' && record.organization_count >= 2
  )

  let strongestSignalPattern = 'No strong signal pattern is currently available.'

  if (hasSharedDomainRelationship) {
    strongestSignalPattern =
      'Shared domain relationships are the strongest visible pattern in the current dataset.'
  } else if (hasHighLinkPattern) {
    strongestSignalPattern =
      'Repeated link activity is the strongest visible pattern in the current dataset.'
  } else if (hasMultiOrganizationPattern) {
    strongestSignalPattern =
      'Multi-organization participation is the strongest visible pattern in the current dataset.'
  } else if (records.length > 0) {
    strongestSignalPattern =
      'A limited but potentially relevant relationship pattern is present in the current dataset.'
  }

  let whyItMatters =
    'Even a limited summary can help direct where deeper relationship investigation should begin.'

  if (hasSharedDomainRelationship) {
    whyItMatters =
      'Shared domain signals can reveal hidden infrastructure overlap, affiliate relationships, or vendor dependencies that are not obvious from organization names alone.'
  } else if (typeof totalOrganizations === 'number' && totalOrganizations >= 2) {
    whyItMatters =
      'Signals involving multiple organizations may point to meaningful affiliation, partnership, ownership, or operational exposure worth deeper review.'
  } else if (typeof totalLinks === 'number' && totalLinks >= 2) {
    whyItMatters =
      'Multiple observed links increase the relevance of the current dataset and make the relationship picture more actionable.'
  }

  return (
    <section style={{ display: 'grid', gap: '16px', color: '#111827' }}>
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: 700, margin: 0 }}>Summary</h2>
        <p style={{ fontSize: '16px', color: '#6b7280', margin: '8px 0 0 0' }}>
          Read-only summary view of relationship signal data.
        </p>
      </div>

      {demoModeEnabled ? (
        <section style={cardStyle}>
          <h3 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Demo Guidance</h3>
          <div style={{ display: 'grid', gap: '8px', marginTop: '12px', color: '#6b7280', fontSize: '14px' }}>
            <p style={{ margin: 0 }}>
              This page provides an executive summary of the current live relationship signal dataset.
            </p>
            <p style={{ margin: 0 }}>
              The overview metrics represent the current loaded records without changing backend truth.
            </p>
            <p style={{ margin: 0 }}>
              The detailed payload below reflects exactly what the backend summary is reporting.
            </p>
            <p style={{ margin: 0 }}>Current demo stage: {demoStage}.</p>
          </div>
        </section>
      ) : null}

      <section style={cardStyle}>
        <h3 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Overview</h3>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
            gap: '16px',
            marginTop: '16px',
          }}
        >
          <div style={cardStyle}>
            <div style={{ fontSize: '13px', color: '#6b7280' }}>Total Records</div>
            <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>{totalRecords}</div>
          </div>

          {totalOrganizations !== null ? (
            <div style={cardStyle}>
              <div style={{ fontSize: '13px', color: '#6b7280' }}>Total Organizations</div>
              <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>
                {totalOrganizations}
              </div>
            </div>
          ) : null}

          {totalLinks !== null ? (
            <div style={cardStyle}>
              <div style={{ fontSize: '13px', color: '#6b7280' }}>Total Links</div>
              <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>
                {totalLinks}
              </div>
            </div>
          ) : null}

          <div style={cardStyle}>
            <div style={{ fontSize: '13px', color: '#6b7280' }}>Domain Count</div>
            <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>{domainCount}</div>
          </div>
        </div>
      </section>

      <section style={cardStyle}>
        <h3 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Intelligence Summary</h3>

        <div style={{ display: 'grid', gap: '16px', marginTop: '16px' }}>
          <div>
            <div style={{ fontSize: '13px', color: '#6b7280', marginBottom: '8px' }}>Top Observation</div>
            <p style={{ margin: 0, fontSize: '14px', color: '#111827' }}>{topObservation}</p>
          </div>

          <div>
            <div style={{ fontSize: '13px', color: '#6b7280', marginBottom: '8px' }}>
              Strongest Signal Pattern
            </div>
            <p style={{ margin: 0, fontSize: '14px', color: '#111827' }}>{strongestSignalPattern}</p>
          </div>

          <div>
            <div style={{ fontSize: '13px', color: '#6b7280', marginBottom: '8px' }}>Why It Matters</div>
            <p style={{ margin: 0, fontSize: '14px', color: '#111827' }}>{whyItMatters}</p>
          </div>
        </div>
      </section>

      <section style={cardStyle}>
        <h3 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Summary Details</h3>

        {summaryEntries.length === 0 ? (
          <p style={{ marginTop: '16px', color: '#6b7280', fontSize: '14px' }}>No summary data available.</p>
        ) : (
          <dl style={{ display: 'grid', gap: '12px', marginTop: '16px', marginBottom: 0 }}>
            {summaryEntries.map(([fieldName, fieldValue]) => (
              <React.Fragment key={fieldName}>
                <dt>
                  <strong style={{ fontSize: '13px', color: '#6b7280' }}>{toReadableLabel(fieldName)}</strong>
                </dt>
                <dd style={{ marginInlineStart: '16px', marginBottom: 0, fontSize: '14px', color: '#111827' }}>
                  {renderValue(fieldValue)}
                </dd>
              </React.Fragment>
            ))}
          </dl>
        )}
      </section>
    </section>
  )
}
