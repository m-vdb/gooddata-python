TEST_DIR = tests
CREDENTIALS_TPL = tests/credentials.py.tpl
CREDENTIALS_FILE = tests/credentials.py
LOGLEVEL = WARNING

install:
	@echo 'Copying credentials...'
	@(cp -n ${CREDENTIALS_TPL} ${CREDENTIALS_FILE}) || echo "Credentials file already exists."
	@echo 'Installing packages...'
	@(pip install -r requirements.txt)

test: test_archiver test_text test_schema test_connection test_project test_dataset test_migration

test_archiver:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_archiver'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_archiver.py --loglevel=${LOGLEVEL})

test_dataset:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_dataset'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_dataset.py --loglevel=${LOGLEVEL})

test_connection:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_connection'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_connection.py --loglevel=${LOGLEVEL})

test_migration:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_migration'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_migration.py --loglevel=${LOGLEVEL})

test_project:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_project'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_project.py --loglevel=${LOGLEVEL})

test_schema:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_schema'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_schema.py --loglevel=${LOGLEVEL})

test_text:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_text'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_text.py --loglevel=${LOGLEVEL})
