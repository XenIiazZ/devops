PYTHON ?= python3
BASE_URL ?= http://localhost:8080
CA_FILE ?=
ACTION_SCRIPT ?=
TLS_ARGS = $(if $(CA_FILE),--ca-file "$(CA_FILE)",)

.PHONY: help check validate test load test-persistence test-failover

help:
	@echo "check: offline structure; validate: full OpenAPI (requirements-dev.txt); test: running API; load: synthetic writes"

check:
	$(PYTHON) scripts/check_contract.py

validate:
	$(PYTHON) scripts/validate_openapi.py

test:
	$(PYTHON) tests/smoke.py --base-url "$(BASE_URL)" $(TLS_ARGS)

load:
	$(PYTHON) tests/load.py --base-url "$(BASE_URL)" $(TLS_ARGS)

test-persistence:
	@test -n "$(ACTION_SCRIPT)" || (echo "Set ACTION_SCRIPT to your implemented executable restart hook"; exit 2)
	$(PYTHON) tests/resilience.py --mode persistence --action-script "$(ACTION_SCRIPT)" --base-url "$(BASE_URL)" $(TLS_ARGS)

test-failover:
	@test -n "$(ACTION_SCRIPT)" || (echo "Set ACTION_SCRIPT to your implemented executable stop-one hook"; exit 2)
	$(PYTHON) tests/resilience.py --mode failover --action-script "$(ACTION_SCRIPT)" --base-url "$(BASE_URL)" $(TLS_ARGS)
