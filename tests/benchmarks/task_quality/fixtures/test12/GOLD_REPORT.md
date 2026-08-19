# Sanitized Test12 Architecture Report

## Scope and frozen baseline

This oracle describes the fixed `origin/main` snapshot represented by the fixture manifest. It distinguishes static repository evidence from runtime claims.

## Architecture coverage

The report oracle requires the project entrypoint, models, macros, tests, seeds, snapshots, deployment control plane, and their cross-layer relationships. Counts reconcile to the offline inventory: 228 models, 52 macro SQL files, 42 SQL tests, 5 seeds, 2 snapshots, and 15 pipeline YAML files.

## Evidence

Every priority claim resolves to a content-addressed fixture path.

## Limitations

The fixture does not assert live warehouse state, successful deployment, or current remote state because no network or database execution occurs.
