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
	  src/project/$(1).py
endef

# Targets
run-schema-evolution:
	$(call RUN_SPARK_MODULE,schema_evolution)

run-partitioning:
	$(call RUN_SPARK_MODULE,partitioning)

run-basic:
	$(call RUN_SPARK_MODULE,basic)

run-schema-evolution:
	$(call RUN_SPARK_MODULE,schema_evolution)

run-all: run-basic run-schema-evolution run-partitioning
