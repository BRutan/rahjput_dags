PYTHON_VERSION := 3.9
AIRFLOW_VERSION := 2.0.0
AIRFLOW_EXTRAS := postgres
CONSTRAINT := https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

.PHONY: venv
venv:
ifneq ($(wildcard ./venv/.*),)
	@echo "venv already exists"
else
	python3 -m virtualenv venv
	venv/bin/python3 -m pip install --upgrade pip
	venv/bin/python3 -m pip install --trusted-host pypi.python.org "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT} --use-deprecated legacy-resolver
	venv/bin/python3 -m pip install -r airflow.requirements.txt --use-deprecated legacy-resolver
	venv/bin/python3 -m pip install -r local.requirements.txt --use-deprecated legacy-resolver
endif

start-db:
	@docker-compose up -d postgres
	@docker-compose up initdb

start-airflow: start-db
	@docker-compose up webserver scheduler

stop-airflow:
	@docker-compose down

reset-airflow:
	@docker-compose down -v || true
	@docker-compose rm -f
	rm -f webserver_config.py

rebuild-airflow:
	@docker-compose build

# Used by docker file:
internal-install-airflow:
	pip install --trusted-host pypi.python.org "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT} --use-deprecated legacy-resolver

internal-install-deps:
	pip install -r airflow.requirements.txt --use-deprecated legacy-resolver

internal-install-local-deps:
	pip install -r local.requirements.txt --use-deprecated legacy-resolver