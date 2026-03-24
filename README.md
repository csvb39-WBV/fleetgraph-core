# FleetGraph Core

FleetGraph Core contains the backend services, runtime contracts, and test suite for the FleetGraph repository.

## Install

```bash
python -m pip install -r requirements.txt
```

## Validate

```bash
pytest -q
```

## Release Artifact Audit Mode

FleetGraph release audits evaluate a ZIP-compatible artifact, not an extracted repository snapshot with live `.git` metadata.

Authoritative artifact command:

```bash
git archive --worktree-attributes --format=zip --output=<artifact>.zip HEAD
```

The release archive is filtered by `.gitattributes` export rules so interpreter residue, cache directories, and coverage outputs are excluded from artifact-level audit.
