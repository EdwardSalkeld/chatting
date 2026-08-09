## Summary
- treat handler context cancellation during connector polling/cleanup as clean shutdown
- add runtime coverage for the mid-poll cancellation path

## Testing
- go test ./...
- CHATTING_BBMB_SERVER_BIN=/srv/chatting/workspace/.tmp-bin/bbmb-server-linux-amd64 CHATTING_E2E_HANDLER_BINARY=/srv/chatting/workspace/.tmp-bin/postmerge-fix/chatting-handler PYTHONPATH=/srv/chatting/workspace/.tmp/worktrees/chatting-postmerge-fix /run/current-system/sw/bin/python3 -m unittest tests.test_split_mode_e2e
- repeated the split-mode e2e test 8 times with the patched handler binary
