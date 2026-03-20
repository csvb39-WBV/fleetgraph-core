import React from 'react'

export default function Signals({ records, selectedSignalId, onSelectSignal }) {
  const selectedRecord = records.find((record) => record.signal_id === selectedSignalId)

  return (
    <section>
      <h2>Relationship Signals</h2>

      <h3>Available Records</h3>
      <ul>
        {records.map((record) => (
          <li key={record.signal_id}>
            <button type="button" onClick={() => onSelectSignal(record.signal_id)}>
              {record.signal_id}
            </button>
          </li>
        ))}
      </ul>

      <h3>Selected Record</h3>
      {selectedRecord ? (
        <dl>
          <dt>signal_id</dt>
          <dd>{selectedRecord.signal_id}</dd>

          <dt>domain</dt>
          <dd>{selectedRecord.domain}</dd>

          <dt>signal_type</dt>
          <dd>{selectedRecord.signal_type}</dd>

          <dt>organization_count</dt>
          <dd>{selectedRecord.organization_count}</dd>

          <dt>link_count</dt>
          <dd>{selectedRecord.link_count}</dd>

          <dt>organization_node_pairs</dt>
          <dd>{(selectedRecord.organization_node_pairs || []).join(', ')}</dd>

          <dt>supporting_unified_organization_ids</dt>
          <dd>{(selectedRecord.supporting_unified_organization_ids || []).join(', ')}</dd>

          <dt>supporting_source_ids</dt>
          <dd>{(selectedRecord.supporting_source_ids || []).join(', ')}</dd>
        </dl>
      ) : null}
    </section>
  )
}
