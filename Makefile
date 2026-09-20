.PHONY: compile eunit ct test interop-a2a-setup interop-a2a dialyzer xref lint fmt check clean

compile:
	rebar3 compile

eunit:
	rebar3 eunit

ct:
	rebar3 ct

test: eunit ct

## Drive the binding with the official A2A Python SDK over its gRPC
## transport. `interop-a2a-setup' is idempotent; the CT group skips when
## the venv is absent and INTEROP_PYTHON is unset, so plain `rebar3 ct'
## needs no Python.
interop-a2a-setup:
	python3 -m venv test/interop/.venv
	./test/interop/.venv/bin/pip install --upgrade pip
	./test/interop/.venv/bin/pip install -r test/interop/requirements-a2a.txt

interop-a2a: interop-a2a-setup
	rebar3 ct --suite=test/livery_grpc_a2a_SUITE --group=python

dialyzer:
	rebar3 dialyzer

xref:
	rebar3 xref

lint:
	rebar3 lint

fmt:
	rebar3 fmt

check: fmt compile lint xref dialyzer eunit ct

clean:
	rebar3 clean
	rm -rf test/interop/.venv
