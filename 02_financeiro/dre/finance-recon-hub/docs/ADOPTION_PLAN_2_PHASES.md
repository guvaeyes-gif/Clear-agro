# Adoption Plan (2 Phases)

## Phase 1 (Now, 2-4 weeks): Stabilize Core Ops
- Deploy 3 jobs: daily reconciliation, monthly close, CRM hygiene.
- Enforce DoD + governance + checklist gates.
- Add structured run logs and exception taxonomy.
- Weekly review of errors and manual interventions.

### Target Metrics
- Throughput: +30% jobs completed per week.
- Rework: -25% reruns/manual fixes.
- Quality: <5% failed runs without clear root cause.
- Tests: 100% smoke test execution for changed scripts.

## Phase 2 (Scale, 6-10 weeks): Multi-domain Expansion
- Expand to logistics exceptions, lab evidence, USA research digest.
- Add AIOS queue routing, approvals, and SLA dashboards.
- Introduce on-demand subagents for compliance and QA automation.
- Formalize incident playbooks and recovery SLAs.

### Target Metrics
- Throughput: +60% vs baseline.
- Rework: -40% vs baseline.
- Quality: >95% runs meeting DoD on first attempt.
- Tests: regression suite on all critical workflows.
