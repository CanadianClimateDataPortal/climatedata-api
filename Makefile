APP_ROOT := $(abspath Makefile/..)
APP_DIR := ${APP_ROOT}/climatedata_api
TEST_DIR := ${APP_ROOT}/tests
APP_FILES := $(shell find $(APP_DIR) -name '*.py')
TEST_FILES := $(shell find $(TEST_DIR) -name '*.py')

.PHONY: check-lint
check-lint:
	@pylint ${APP_DIR} ${TEST_DIR}

# Preview formatting changes
.PHONY: check-formatting
check-formatting:
	@autopep8 --max-line-length 120 --diff -v ${APP_FILES} ${TEST_FILES}
	@isort -c -v ${APP_DIR} ${TEST_DIR}

# Automatically fix formatting and imports
.PHONY: fix-formatting
fix-formatting:
	@autopep8 --max-line-length 120 --in-place -v ${APP_FILES} ${TEST_FILES}
	@isort -v ${APP_DIR} ${TEST_DIR}

.PHONY: test
test:
	@pytest ${TEST_DIR}