# Local Run Notes

- Run unit tests: `python -m unittest`
- Run all tests via discovery: `python3 -m unittest discover -s tests`
- Run a specific test module: `python3 -m unittest tests.test_sqlite_store`
- Run connector tests: `python3 -m unittest tests.test_connectors`
- Run router tests: `python3 -m unittest tests.test_router`
- Run policy tests: `python3 -m unittest tests.test_policy`
- Run bootstrap flow tests: `python3 -m unittest tests.test_main`
- Run executor tests: `python3 -m unittest tests.test_executor`
- Run executor parser-only checks: `python3 -m unittest tests.test_executor.ParseExecutionResultTests`
- For executor schema-contract changes, run parser-only checks first, then full executor tests: `python3 -m unittest tests.test_executor.ParseExecutionResultTests && python3 -m unittest tests.test_executor`
- For schema-version contract hardening across models + executor parser, run: `python3 -m unittest tests.test_models tests.test_executor.ParseExecutionResultTests`
- Run state contract tests: `python3 -m unittest tests.test_state_contract`
- Run full interface contract tests: `python3 -m unittest tests.test_interface_contracts tests.test_state_contract`
- Run audit logging checks: `python3 -m unittest tests.test_models tests.test_sqlite_store tests.test_main`
- Run bootstrap observability log checks: `python3 -m unittest tests.test_main`
- Run idempotency contract checks: `python3 -m unittest tests.test_sqlite_store tests.test_main`
- Run bootstrap app locally: `python3 -m app.main --db-path /tmp/chatting-state.db`
- If `rg` is unavailable locally, use `find . -type f` for file discovery.
