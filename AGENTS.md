# Local Run Notes

- Run unit tests: `python -m unittest`
- Run all tests via discovery: `python3 -m unittest discover -s tests`
- Run a specific test module: `python3 -m unittest tests.test_sqlite_store`
- Run connector tests: `python3 -m unittest tests.test_connectors`
- Run router tests: `python3 -m unittest tests.test_router`
- Run policy tests: `python3 -m unittest tests.test_policy`
- Run bootstrap flow tests: `python3 -m unittest tests.test_main`
- Run executor tests: `python3 -m unittest tests.test_executor`
- Run bootstrap app locally: `python3 -m app.main --db-path /tmp/chatting-state.db`
- If `rg` is unavailable locally, use `find . -type f` for file discovery.
