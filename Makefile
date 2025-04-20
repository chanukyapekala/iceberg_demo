PYSPARK_PYTHON := $(shell poetry env info -p)/bin/python
SPARK_SUBMIT = /opt/homebrew/bin/spark-submit
ICEBERG_PACKAGES = org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0
WAREHOUSE_PATH = file:///Users/chanukya/GIT/iceberg_warehouse
SPARK_COMMON_FLAGS = \
 --packages $(ICEBERG_PACKAGES) \
 --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.local.type=hadoop \
 --conf spark.sql.catalog.local.warehouse=$(WAREHOUSE_PATH)

export PYSPARK_PYTHON

# Generic rule to run any module
define RUN_SPARK_MODULE
$(SPARK_SUBMIT) \
  $(SPARK_COMMON_FLAGS) \
  src/iceberg_demo/$(1).py
endef

# Generic rule to run streaming module
define RUN_SPARK_STREAMING_MODULE
$(SPARK_SUBMIT) \
  $(SPARK_COMMON_FLAGS) \
  src/iceberg_demo/streaming/$(1).py
endef

export RUN_SPARK_MODULE

# Targets
run-schema-evolution:
	$(call RUN_SPARK_MODULE,schema_evolution)

run-partitioning:
	$(call RUN_SPARK_MODULE,partitioning)

run-load-sample-data:
	$(call RUN_SPARK_MODULE,load_sample_data)

run-metadata-changes:
	$(call RUN_SPARK_MODULE,metadata_changes)

run-time-travel:
	$(call RUN_SPARK_MODULE,time_travel)

run-snapshot-diff:
	$(call RUN_SPARK_MODULE,snapshot_diff)

run-list-tables:
	$(call RUN_SPARK_MODULE,list_tables)

run-query-list-tables:
	$(call RUN_SPARK_MODULE,query_list_tables)

run-display-table:
	$(call RUN_SPARK_MODULE,display_table_rows)

run-scd-type-2:
	$(call RUN_SPARK_MODULE,scd_type2)

run-ml-issue:
	$(call RUN_SPARK_MODULE,ml_issue)

run-gen_ai_issue:
	$(call RUN_SPARK_MODULE,gen_ai_issue)

run-producer:
	$(call RUN_SPARK_STREAMING_MODULE,producer)

run-consumer:
	$(call RUN_SPARK_STREAMING_MODULE,consumer)

# Airflow Variables
AIRFLOW_HOME=$(PWD)/airflow
AIRFLOW__CORE__DAGS_FOLDER=$(AIRFLOW_HOME)/dags
AIRFLOW_CMD=export AIRFLOW_HOME=$(AIRFLOW_HOME) && export AIRFLOW__CORE__DAGS_FOLDER=$(AIRFLOW__CORE__DAGS_FOLDER) && poetry run airflow

.PHONY: validate-dags
validate-dags:
	@echo "Validating DAGs..."
	@$(PYSPARK_PYTHON) -m py_compile $(AIRFLOW_HOME)/dags/*.py

.PHONY: reload-dags
reload-dags: stop-airflow
	@echo "Clearing Airflow DAG bag..."
	@rm -rf $(AIRFLOW_HOME)/dags/__pycache__
	@echo "Restarting Airflow..."
	@$(MAKE) start-airflow
	@echo "DAGs reloaded."

# Airflow Targets
.PHONY: init-airflow
init-airflow:
	@echo "Initializing Airflow database..."
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && $(AIRFLOW_CMD) db init

.PHONY: create-admin
create-admin:
	@echo "Creating Airflow admin user..."
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && $(AIRFLOW_CMD) users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

.PHONY: start-webserver
start-webserver:
	@echo "Starting Airflow webserver..."
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && $(AIRFLOW_CMD) webserver --port 8081 &

.PHONY: start-scheduler
start-scheduler:
	@echo "Starting Airflow scheduler..."
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && $(AIRFLOW_CMD) scheduler &

.PHONY: start-airflow
start-airflow: init-airflow create-admin start-webserver start-scheduler
	@echo "Airflow is ready."

.PHONY: stop-airflow
stop-airflow:
	@echo "Stopping Airflow..."
	@pkill -f "airflow webserver"
	@pkill -f "airflow scheduler"
	@echo "Airflow stopped."

.PHONY: run-stream-writer-airflow-dag
run-stream-writer-airflow-dag: start-airflow
	@echo "Triggering Airflow DAG..."
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && export PYTHONPATH=$(AIRFLOW_HOME)/dags && $(AIRFLOW_CMD) dags trigger stream_writer_dag

.PHONY: run-ml-genai-writer-airflow-dag
run-ml-genai-writer-airflow-dag: start-airflow
	@echo "Triggering Airflow DAG..."
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && export PYTHONPATH=$(AIRFLOW_HOME)/dags && $(AIRFLOW_CMD) dags trigger ml_genai_writer_dag

.PHONY: run-batch-writer-airflow-dag
run-batch-writer-airflow-dag: start-airflow
	@echo "Triggering Airflow DAG..."
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && export PYTHONPATH=$(AIRFLOW_HOME)/dags && $(AIRFLOW_CMD) dags trigger batch_writer_dag
