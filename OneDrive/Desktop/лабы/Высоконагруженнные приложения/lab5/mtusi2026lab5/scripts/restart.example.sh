#!/bin/sh
set -eu
# TODO: copy to restart.sh and implement using YOUR compose/stack services.
# Restart API and ALL used state stores (DB, sessions, S3, broker as applicable).
# Preserve persistent volumes and schema/data; never down -v / flush / re-seed.
# Allow operator unseal if required in lab7; do not print credentials.
# Exit 0 only after the intended action; test waits for API readiness separately.
printf '%s
' 'TODO: implement restart.sh for your infrastructure' >&2
exit 2
