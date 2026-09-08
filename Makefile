PYTHON ?= python
.PHONY: test demo evaluate
test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s checks -v
demo:
	PYTHONPATH=src $(PYTHON) -m model_ledger demo --output artifacts/demo.json
evaluate:
	PYTHONPATH=src $(PYTHON) -m model_ledger evaluate --output artifacts/evaluation.json
