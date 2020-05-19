SOURCE_DIR:=src/
CORE_DIR:=${SOURCE_DIR}/core/
PERSISTENCE_DIR:=${SOURCE_DIR}/persistence/
INTERFACE_DIR:=${SOURCE_DIR}/interface/
TEST_DIRS:=${CORE_DIR}/test ${PERSISTENCE_DIR}/test

compile:
	gcc -lhdf5 -lhdf5_hl ${PERSISTENCE_DIR}/prv_reader.c -o ${PERSISTENCE_DIR}/prv_reader

install:
	pip install -r requirements.txt

#install-interface:

format:
	isort --apply
	black --line-length 120 ${CORE_DIR} ${PERSISTENCE_DIR}
	flake8 ${CORE_DIR} ${PERSISTENCE_DIR}

test:
	pytest -svv ${TEST_DIRS} $(ARGS)

.PHONY: compile install format test