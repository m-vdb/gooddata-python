# TESTS = '*'
CREDENTIALS_TPL = tests/credentials.py.tpl
CREDENTIALS_FILE = tests/credentials.py

install:
	@echo 'Copying credentials...'
	@(cp -n ${CREDENTIALS_TPL} ${CREDENTIALS_FILE}) || echo "Credentials file already exists."
	@echo 'Installing packages...'
	@(pip install -r requirements.txt)

test:
	@(PYTHONPATH=`pwd -P` python -m unittest tests.${TESTS})
