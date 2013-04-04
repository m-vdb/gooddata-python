TEST_DIR = 'tests/'

test: test_archiver test_text test_schema test_connection test_project test_dataset

test_archiver:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_archiver'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_archiver.py)

test_dataset:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_dataset'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_dataset.py)

test_connection:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_connection'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_connection.py)

test_project:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_project'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_project.py)

test_schema:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_schema'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_schema.py)

test_text:
	@echo '-------------------'
	@echo '-------------------'
	@echo 'Executing test_text'
	@(PYTHONPATH=`pwd -P` python ${TEST_DIR}/test_text.py)
