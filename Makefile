SOURCE_DIR:=src/
CORE_DIR:=${SOURCE_DIR}/core/
PERSISTENCE_DIR:=${SOURCE_DIR}/persistence/
INTERFACE_DIR:=${SOURCE_DIR}/interface/
TEST_DIRS:=${CORE_DIR}/test ${PERSISTENCE_DIR}/test

.PHONY: install
install:
	pip install -r requirements.txt

#.PHONY: install-interface
#install-interface:

.PHONY: format
format:
	isort --apply
	black --line-length 120 ${CORE_DIR} ${PERSISTENCE_DIR}
	flake8 ${CORE_DIR} ${PERSISTENCE_DIR}

.PHONY: test
test:
	pytest -svvv ${TEST_DIRS} $(ARGS)

.PHONY: start-interface
start-interface:
	bash scripts/start_interface.sh