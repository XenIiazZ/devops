#!/bin/sh
set -eu
# TODO: copy to stop-one.sh, map TARGET_INSTANCE_ID to ONE API container/task.
# Stop that instance and leave it stopped; no stop of DB/LB/volumes.
# Return quickly (within 10 s); the test sends requests during this script.
# Restore the replica explicitly after the test. Never print credentials.
: "${TARGET_INSTANCE_ID:?test provides target API instance alias}"
printf '%s
' 'TODO: implement stop-one.sh for your infrastructure' >&2
exit 2
