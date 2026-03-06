# Milestone 03: BBMB Process Split (Message Handler + Worker)

## Objective
Split `chatting` into two runtime roles connected via BBMB queues so integration secrets and outbound dispatch stay on `UserOne`, while AI execution runs on `UserTwo`.

## Locked Decisions
- [x] Use BBMB as the transport boundary.
- [x] Hardcode queue names:
  - `chatting.tasks.v1`
  - `chatting.egress.v1`
- [x] Keep ingress + egress in one entrypoint (`message-handler`) with one config.
- [x] Worker runs separately with separate config and separate state DB.
- [x] Worker may emit multiple egress responses per task.
- [x] Egress validation is strict: unknown task => log and drop.
- [x] Do not forward `write_file` actions to egress.
- [x] Design for cross-machine deployment (UserOne host, UserTwo host, BBMB host).

## Phase Checklist

### Phase 1: Contracts + Transport
- [ ] Add queue payload contracts for task and egress messages.
- [ ] Add serialization/deserialization helpers with schema checks.
- [ ] Add BBMB queue adapter wrapper around `bbmb_client`.
- [ ] Add tests for contract parsing and BBMB adapter behavior.
- [ ] Commit.

### Phase 2: Worker Split
- [ ] Add `app/main_worker.py` entrypoint.
- [ ] Implement worker consume/process/publish/ack loop from `chatting.tasks.v1` to `chatting.egress.v1`.
- [ ] Ensure worker strips/blocks egress-ineligible action types (`write_file` forwarding disabled).
- [ ] Persist worker run/audit/dead-letter state.
- [ ] Add tests for multi-response egress publishing and retry semantics.
- [ ] Commit.

### Phase 3: Message Handler Split
- [ ] Add `app/main_message_handler.py` entrypoint.
- [ ] Implement ingress polling and publish to `chatting.tasks.v1`.
- [ ] Persist task ledger for strict egress validation.
- [ ] Implement egress consume/validate/dispatch/ack loop from `chatting.egress.v1`.
- [ ] Enforce channel/target allowlist + task existence checks.
- [ ] Add tests for strict unknown-task drop and allowlist behavior.
- [ ] Commit.

### Phase 4: Config + Docs + Ops
- [ ] Add `configs/message-handler-runtime.example.json`.
- [ ] Add `configs/worker-runtime.example.json`.
- [ ] Add/update systemd units for message-handler and worker.
- [ ] Update run docs with cross-machine topology and BBMB address settings.
- [ ] Commit.

### Phase 5: Compatibility + Cleanup
- [ ] Keep `app/main.py` as compatibility shim or mark deprecated.
- [ ] Remove or de-prioritize bootstrap mode paths.
- [ ] Run full tests (`python3 -m unittest discover -s tests`).
- [ ] Commit final milestone integration.

## Resume Notes
When resuming work, start by checking:
1. `git status --short`
2. This checklist file current checkbox state
3. Latest commit touching `app/main_worker.py` or `app/main_message_handler.py`
