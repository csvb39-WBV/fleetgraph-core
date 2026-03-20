Frontend deployment

- Deploy frontend/fleetgraph-ui to Vercel.
- Set VITE_API_BASE_URL in Vercel environment variables to your public API base URL.
- Build command: npm run build.
- Output directory: dist.

Backend deployment guidance

- Deploy the existing Python API service to Render or Railway.
- Expose a public base URL for the API.
- Configure CORS there if cross-origin access is required outside this block.

Local development

- Local frontend development continues to use http://127.0.0.1:8000 by default.
- The deterministic fixture fallback remains available if the API is unavailable.

Demo-safe note

- If the API is unavailable, the frontend fixture fallback still supports product demos.