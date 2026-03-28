import React from 'react';

import type { WatchlistProject } from '../../services/watchlistApi';

type Props = {
  website: string;
  hqCity: string;
  hqState: string;
  recentProjects: WatchlistProject[];
};

export function ProjectsLocationsPanel({ website, hqCity, hqState, recentProjects }: Props): JSX.Element {
  return (
    <section aria-label="Projects Locations Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Projects & Locations</h3>
      <div style={{ display: 'grid', gap: '8px' }}>
        <div><strong>Website:</strong> {website || 'No website stored'}</div>
        <div><strong>HQ:</strong> {hqCity && hqState ? `${hqCity}, ${hqState}` : 'HQ details are incomplete'}</div>
      </div>
      <div style={{ marginTop: '12px' }}>
        <strong>Recent Projects</strong>
        {recentProjects.length === 0 ? (
          <p style={{ marginBottom: 0 }}>No recent projects are currently attached to this company.</p>
        ) : (
          <ul style={{ margin: '8px 0 0', paddingLeft: '18px', display: 'grid', gap: '10px' }}>
            {recentProjects.map((project) => (
              <li key={`${project.name}-${project.location}`}>
                <strong>{project.name}</strong> — {project.location}
                <div style={{ fontSize: '13px', color: '#475569' }}>
                  Status: {project.status} · Confidence: {project.confidence}
                </div>
                <div style={{ fontSize: '13px', color: '#475569' }}>Source: {project.source_url}</div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

export default ProjectsLocationsPanel;
