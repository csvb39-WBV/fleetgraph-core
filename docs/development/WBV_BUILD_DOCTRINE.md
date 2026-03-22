# WBV Build Doctrine

## Core Rules
- One block = one commit = one tag
- No advancement until:
  - tests pass
  - repo clean
  - tag created
  - remote synced

## Delivery
- Full files only
- No partial patches unless in Patch Mode

## Determinism
- Same input → same output
- No randomness
- No hidden state

## Repo Hygiene
- Working tree must be clean
- Tags must map exactly to commits
