# CEI-InOE Release Flow

## Purpose
This document defines a clear, repeatable versioning and release process based on:
- `main` for production-ready code
- `testing` for integration and validation
- Git tags as deployable app versions

This aligns with the existing deployment behavior where `cei_inoe_version` can target either a branch or a tag.

## Branch Roles
- `testing`
  - Integration branch for new features and fixes.
  - Can be deployed to non-production environments.
- `main`
  - Production-ready branch only.
  - Every commit should be releasable.
- `tags`
  - Official release identifiers, for example `v1.2.3`.
  - Production deployments should use tags, not branch names.

## Versioning Scheme
Use Semantic Versioning:
- `vMAJOR.MINOR.PATCH`

Examples:
- `v1.0.0` initial stable release
- `v1.3.0` backward-compatible feature release
- `v1.3.2` backward-compatible bugfix release

Optional prerelease tags:
- `v1.4.0-rc.1`
- `v1.4.0-rc.2`

Use prerelease tags only for validation before final promotion to `main`.

## MAJOR/MINOR/PATCH Decision Table

| Change Type | Bump | Examples | Backward Compatibility |
|---|---|---|---|
| Breaking API or data contract changes | MAJOR | Remove or rename endpoint fields, incompatible response format, required config changes that break clients, schema changes requiring operator intervention | No |
| New backward-compatible functionality | MINOR | New API endpoints, additive response fields, new connector, optional config additions, additive schema changes | Yes |
| Backward-compatible fixes | PATCH | Bug fixes, performance improvements, logging improvements, internal refactors with no contract changes, safe connector fixes | Yes |

## Release Flow
1. Develop on short-lived branches from `testing`.
2. Open pull requests into `testing`.
3. Validate on `testing` (tests, smoke checks, deployment checks).
4. Freeze `testing` for release candidate.
5. Optionally create prerelease tag from `testing` for staging/UAT.
6. Open pull request from `testing` into `main`.
7. Merge into `main` after approval and final validation.
8. Create annotated release tag on `main`, for example `v1.5.0`.
9. Deploy production using the tag via `cei_inoe_version`.
10. Publish release notes and update changelog.

## Hotfix Flow
1. Branch from `main` for urgent fix.
2. Implement and validate the fix.
3. Merge back to `main`.
4. Create PATCH tag, for example `v1.5.1`.
5. Deploy production with the new PATCH tag.
6. Merge the hotfix back into `testing` to avoid divergence.

## Release Checklist

### A. Scope and Version Decision
- Confirm release scope is complete on `testing`.
- Choose version bump using the MAJOR/MINOR/PATCH table.
- Confirm no unresolved blockers or critical known issues.

### B. Quality Gates
- All automated tests pass.
- API smoke tests pass (`/health`, `/ready`, key endpoints).
- Migration path is tested (upgrade and rollback expectations).
- Connector sanity checks pass (Tago, Airbeld, FusionSolar, Open-Meteo where applicable).
- No critical API or ingestor errors during soak period.

### C. Deployment Readiness
- Verify environment variables and secrets are correct.
- Verify datasource and active site settings.
- Verify compose stack starts cleanly.
- Verify Grafana dashboards and datasource connectivity.
- Confirm rollback target (previous stable tag).

### D. Promotion and Tagging
- Pull request `testing` to `main` is approved and merged.
- Create annotated tag on `main`.
- Tag format is `vMAJOR.MINOR.PATCH`.
- Tag message includes summary and migration notes.
- Push tag to origin and verify visibility.

### E. Production Deployment
- Set deployment version to release tag via `cei_inoe_version`.
- Run deployment playbook.
- Verify service health and readiness endpoints.
- Verify authenticated API endpoints.
- Verify ingestor processing and batch creation.
- Verify dashboards and key business metrics.

### F. Post-Release
- Publish release notes and changelog entry.
- Record known issues and follow-up tasks.
- Monitor logs and metrics during stabilization window.
- If rollback is required, redeploy previous stable tag and communicate the incident.

## Rollback Policy
- Roll back only to the previous stable production tag.
- Prefer redeploying known-good tag over ad-hoc fixes in production.
- If a schema migration is not backward-compatible, include explicit rollback instructions in release notes.

## Tag Naming Rules
- Stable: `vX.Y.Z`
- Prerelease: `vX.Y.Z-rc.N`
- Do not reuse or retag existing versions.
- Do not deploy production from untagged commits.

## Ownership
- Release Manager
  - Owns version decision, checklist completion, and tag creation.
- Reviewer(s)
  - Approve `testing` to `main` promotion.
- Operator
  - Executes deployment and validates runtime health.

## Practical Commands

Create a release tag from `main`:

```bash
git checkout main
git pull origin main
git tag -a v1.4.0 -m "Release v1.4.0"
git push origin v1.4.0
```

Deploy a specific version via Ansible:

```bash
ansible-playbook -i inventory/hosts.yml playbooks/deploy.yml \
  -e cei_inoe_version=v1.4.0 \
  --ask-become-pass \
  --ask-vault-pass
```
