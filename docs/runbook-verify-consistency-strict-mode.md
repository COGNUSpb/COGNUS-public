# Runbook Adaptation: Verify Consistency Strict Mode

## Objective
Avoid false-positive `completed` states when Fabric identity, MSP, or TLS drift exists. One example is a healthy gateway with a non-committed chaincode and failing functional queries.

## What Changed
- `verify.consistency` now executes the lifecycle guard in strict mode by default.
- New environment flag:
  - `RUNBOOK_VERIFY_CONSISTENCY_STRICT=true` (default)
- Explicit Fabric failure classification in the SSH execution parser.
- Channel and lifecycle auto-repair enabled by default:
  - `RUNBOOK_CHAINCODE_CHANNEL_AUTO_REPAIR_ENABLED=true`
  - Detects `Writers + unknown authority` drift and the invalid endpoint `127.0.0.1:7050` in peer logs.
  - Forces channel block regeneration, orderer participation rejoin, and peer rejoin before retrying lifecycle.

## New Failure Codes
- `runbook_fabric_channel_not_joined`
- `runbook_fabric_chaincode_not_committed`
- `runbook_fabric_msp_policy_forbidden`
- `runbook_fabric_orderer_tls_mismatch`
- `runbook_fabric_orderer_channel_state_invalid`
- `runbook_fabric_orderer_identity_drift`

## Critical Signal Covered
When the peer can connect to the orderer but rejects blocks due to the `Writers` policy with `x509: certificate signed by unknown authority`, the runbook now classifies it as:
- `runbook_fabric_orderer_identity_drift`

This indicates an inconsistency between the channel or genesis cryptographic material and the active orderer certificates.

## Operational Behavior
- With `RUNBOOK_VERIFY_CONSISTENCY_STRICT=true`, the runbook fails early in `verify.consistency` when a real lifecycle or channel inconsistency is detected.
- To restore the legacy deferred behavior, use:
  - `RUNBOOK_VERIFY_CONSISTENCY_STRICT=false`

## Automatic Recovery Strategy
- The consistency flow is no longer diagnostic-only and now attempts recovery automatically:
  - rehydrates `orderer-ca` for fetch and commit operations;
  - regenerates the channel block with the active orderer MSP material;
  - reapplies the join operation through the orderer participation API;
  - re-runs the peer join and retries approve or commit.
- If the chaincode is still not `querycommitted` after auto-repair, the runbook fails with a specific code, preventing a false `completed` result.

## Recommendation
Keep strict mode enabled in real validation environments, especially when the goal is to guarantee functional queries through the API gateway and not only a healthcheck signal.
