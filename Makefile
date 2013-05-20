CREDENTIALS_TPL = tests/credentials.py.tpl
CREDENTIALS_FILE = tests/credentials.py
VERBOSITY =
TESTS =

ifeq "$(TESTS)" ""
	TEST_CLAUSE = tests
else
	TEST_CLAUSE = tests.${TESTS}
endif

ifeq "$(VERBOSITY)" "DEBUG"
	UNITTEST = -v ${TEST_CLAUSE}
else
	UNITTEST = ${TEST_CLAUSE}
endif


install:
	@echo 'Copying credentials...'
	@(cp -n ${CREDENTIALS_TPL} ${CREDENTIALS_FILE}) || echo "Credentials file already exists."
	@echo 'Installing packages...'
	@(pip install -r requirements.txt)

test:
	@(PYTHONPATH=`pwd -P` python -m unittest ${UNITTEST})
