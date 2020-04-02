SOURCES:=src/ test/

.PHONY: install
install:
	pip install -r requirements.txt

.PHONY: format
format:
	isort --apply
	black --line-length 120 ${SOURCES}
	flake8 ${SOURCES}

.PHONY: test
test:
	pytest -svv test $(ARGS)
