import React from 'react'

export default function Signals({ records, selectedSignalId, onSelectSignal }) {
  const PRIMARY_FIELDS = [
    'id',
    'record_id',
    'signal_id',
    'organization',
    'organization_name',
    'domain',
    'source_domain',
    'target_domain',
    'relationship_type',
  ]
  const RELATIONSHIP_FIELDS = [
    'link_count',
    'organization_count',
    'shared_domain_count',
    'signal_type',
    'connection_type',
    'match_type',
    'strength',
    'confidence',
  ]
  const EVIDENCE_FIELDS = [
    'evidence',
    'evidence_count',
    'domains',
    'organizations',
    'links',
    'signals',
    'details',
    'summary',
    'reason',
    'reasons',
    'supporting_data',
  ]

  function toReadableLabel(fieldName) {
    return fieldName
      .split('_')
      .map((part) => (part ? part.charAt(0).toUpperCase() + part.slice(1) : part))
      .join(' ')
  }

  function renderObjectValue(value) {
    const entries = Object.entries(value)
    if (entries.length === 0) {
      return <span>Not provided</span>
    }
    return (
      <div>
        {entries.map(([key, nestedValue]) => (
          <div key={key}>
            <strong>{toReadableLabel(key)}:</strong> {renderValue(nestedValue)}
          </div>
        ))}
      </div>
    )
  }

  function renderArrayValue(values) {
    if (values.length === 0) {
      return <span>Not provided</span>
    }
    return (
      <ul>
        {values.map((item, index) => (
          <li key={index}>
            {item === null || item === undefined || item === '' ? (
              <span>Not provided</span>
            ) : Array.isArray(item) ? (
              renderArrayValue(item)
            ) : typeof item === 'object' ? (
              renderObjectValue(item)
            ) : (
              <span>{String(item)}</span>
            )}
          </li>
        ))}
      </ul>
    )
  }

  function renderValue(value) {
    if (value === null || value === undefined || value === '') {
      return <span>Not provided</span>
    }
    if (Array.isArray(value)) {
      return renderArrayValue(value)
    }
    if (typeof value === 'object') {
      return renderObjectValue(value)
    }
    return <span>{String(value)}</span>
  }

  function getRecordLabel(record, index) {
    const candidateFields = [
      'signal_id',
      'id',
      'record_id',
      'domain',
      'organization',
      'organization_name',
    ]
    for (const fieldName of candidateFields) {
      const value = record[fieldName]
      if (value !== null && value !== undefined && value !== '') {
        return String(value)
      }
    }
    return `Record ${index + 1}`
  }

  function renderFieldGroup(title, fieldNames, record, renderedFields) {
    const presentFieldNames = []
    for (const fieldName of fieldNames) {
      if (Object.prototype.hasOwnProperty.call(record, fieldName)) {
        presentFieldNames.push(fieldName)
        renderedFields.add(fieldName)
      }
    }

    if (presentFieldNames.length === 0) {
      return null
    }

    return (
      <section>
        <h4>{title}</h4>
        <dl>
          {presentFieldNames.map((fieldName) => (
            <React.Fragment key={fieldName}>
              <dt>{toReadableLabel(fieldName)}</dt>
              <dd>{renderValue(record[fieldName])}</dd>
            </React.Fragment>
          ))}
        </dl>
      </section>
    )
  }

  const selectedRecord = records.find((record) => record.signal_id === selectedSignalId)

  let selectedRecordDetails = null
  if (!selectedRecord) {
    selectedRecordDetails = <p>No record selected.</p>
  } else {
    const selectedRecordKeys = Object.keys(selectedRecord)
    if (selectedRecordKeys.length === 0) {
      selectedRecordDetails = <p>No record details available.</p>
    } else {
      const renderedFields = new Set()
      const primarySection = renderFieldGroup(
        'Primary Details',
        PRIMARY_FIELDS,
        selectedRecord,
        renderedFields
      )
      const relationshipSection = renderFieldGroup(
        'Relationship Details',
        RELATIONSHIP_FIELDS,
        selectedRecord,
        renderedFields
      )
      const evidenceSection = renderFieldGroup(
        'Evidence / Supporting Data',
        EVIDENCE_FIELDS,
        selectedRecord,
        renderedFields
      )

      const additionalFields = selectedRecordKeys.filter(
        (fieldName) => !renderedFields.has(fieldName)
      )

      const additionalSection = additionalFields.length > 0 ? (
        <section>
          <h4>Additional Fields</h4>
          <dl>
            {additionalFields.map((fieldName) => (
              <React.Fragment key={fieldName}>
                <dt>{toReadableLabel(fieldName)}</dt>
                <dd>{renderValue(selectedRecord[fieldName])}</dd>
              </React.Fragment>
            ))}
          </dl>
        </section>
      ) : null

      const renderedSections = [
        primarySection,
        relationshipSection,
        evidenceSection,
        additionalSection,
      ].filter(Boolean)

      selectedRecordDetails =
        renderedSections.length > 0 ? renderedSections : <p>No record details available.</p>
    }
  }

  return (
    <section>
      <h2>Signals</h2>
      <p>Read-only relationship signal explorer.</p>

      <h3>Record List</h3>
      <ul>
        {records.map((record, index) => (
          <li key={record.signal_id || index}>
            <button type="button" onClick={() => onSelectSignal(record.signal_id)}>
              {record.signal_id === selectedSignalId ? '[Selected] ' : ''}
              {getRecordLabel(record, index)}
            </button>
          </li>
        ))}
      </ul>

      <h3>Selected Record Detail</h3>
      {selectedRecordDetails}
    </section>
  )
}
