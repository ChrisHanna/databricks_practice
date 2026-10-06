# Databricks notebook source
# MAGIC %md
# MAGIC # Hourly equipment health with delayed and corrected readings
# MAGIC 
# MAGIC Synthetic practice data. Contract fingerprint: `0268972c86dc651f85506684e2e34af0bbf4a9826ccf5d76a08d62acd1550f6c`. Review the sourced requirements, configuration, operators and acceptance checks before execution.

# COMMAND ----------

import json
import re
from pyspark.sql import functions as F
spark.conf.set('spark.sql.session.timeZone', 'UTC')
cfg = json.loads('{\n  "schema_version": "1.0",\n  "title": "Hourly equipment health with delayed and corrected readings",\n  "seed": 42,\n  "task_kind": "analysis",\n  "decisions": [\n    {\n      "key": "objective",\n      "value": "Correct hourly equipment-health averages and late-reading inclusion.",\n      "status": "confirmed",\n      "source": "User prompt",\n      "impact": "Business outcome.",\n      "severity": "requirements",\n      "bindings": {}\n    },\n    {\n      "key": "deliverable",\n      "value": "Databricks-ready PySpark pipeline notebook with plans, checks, and distributed execution explanations.",\n      "status": "confirmed",\n      "source": "User prompt",\n      "impact": "Artifact scope.",\n      "severity": "requirements",\n      "bindings": {\n        "outputs": [\n          "readings_raw",\n          "resolved_readings",\n          "hourly_health",\n          "hourly_comparison",\n          "late_quarantine"\n        ]\n      }\n    },\n    {\n      "key": "grain",\n      "value": "One logical reading per reading_id; physical repeats and corrected versions share identity.",\n      "status": "confirmed",\n      "source": "User prompt and answer",\n      "impact": "Identity resolution.",\n      "severity": "requirements",\n      "bindings": {}\n    },\n    {\n      "key": "metric",\n      "value": "Per machine and UTC event hour averages and independent valid/missing counts for temperature and vibration.",\n      "status": "confirmed",\n      "source": "User answer",\n      "impact": "Aggregation.",\n      "severity": "requirements",\n      "bindings": {\n        "operations": [\n          {\n            "kind": "filter",\n            "name": "eligible_readings",\n            "input": "readings_raw",\n            "predicate": "arrival_delay_minutes <= 120"\n          },\n          {\n            "kind": "deduplicate",\n            "name": "resolved_readings",\n            "input": "eligible_readings",\n            "keys": [\n              "reading_id"\n            ],\n            "order_by": [\n              {\n                "column": "reading_version",\n                "direction": "desc"\n              },\n              {\n                "column": "arrival_timestamp",\n                "direction": "desc"\n              }\n            ]\n          },\n          {\n            "kind": "derive",\n            "name": "resolved_with_hour",\n            "input": "resolved_readings",\n            "columns": [\n              {\n                "name": "event_hour",\n                "expression": "timestampadd(SECOND, -second(event_timestamp), timestampadd(MINUTE, -minute(event_timestamp), event_timestamp))"\n              }\n            ]\n          },\n          {\n            "kind": "aggregate",\n            "name": "hourly_health",\n            "input": "resolved_with_hour",\n            "group_by": [\n              "site_id",\n              "machine_id",\n              "event_hour"\n            ],\n            "measures": [\n              {\n                "name": "total_reading_count",\n                "expression": "count(*)"\n              },\n              {\n                "name": "valid_temperature_count",\n                "expression": "count(temperature_c)"\n              },\n              {\n                "name": "missing_temperature_count",\n                "expression": "sum(case when temperature_c is null then 1 else 0 end)"\n              },\n              {\n                "name": "avg_temperature_c",\n                "expression": "avg(temperature_c)"\n              },\n              {\n                "name": "valid_vibration_count",\n                "expression": "count(vibration_mm_s)"\n              },\n              {\n                "name": "missing_vibration_count",\n                "expression": "sum(case when vibration_mm_s is null then 1 else 0 end)"\n              },\n              {\n                "name": "avg_vibration_mm_s",\n                "expression": "avg(vibration_mm_s)"\n              }\n            ]\n          },\n          {\n            "kind": "filter",\n            "name": "reference_eligible",\n            "input": "readings_raw",\n            "predicate": "arrival_timestamp <= timestampadd(HOUR, 2, event_timestamp)"\n          },\n          {\n            "kind": "deduplicate",\n            "name": "reference_resolved",\n            "input": "reference_eligible",\n            "keys": [\n              "reading_id"\n            ],\n            "order_by": [\n              {\n                "column": "reading_version",\n                "direction": "desc"\n              },\n              {\n                "column": "arrival_timestamp",\n                "direction": "desc"\n              }\n            ]\n          },\n          {\n            "kind": "derive",\n            "name": "reference_with_hour",\n            "input": "reference_resolved",\n            "columns": [\n              {\n                "name": "event_hour",\n                "expression": "timestampadd(SECOND, -second(event_timestamp), timestampadd(MINUTE, -minute(event_timestamp), event_timestamp))"\n              }\n            ]\n          },\n          {\n            "kind": "aggregate",\n            "name": "hourly_reference",\n            "input": "reference_with_hour",\n            "group_by": [\n              "site_id",\n              "machine_id",\n              "event_hour"\n            ],\n            "measures": [\n              {\n                "name": "ref_total_reading_count",\n                "expression": "count(*)"\n              },\n              {\n                "name": "ref_valid_temperature_count",\n                "expression": "count(temperature_c)"\n              },\n              {\n                "name": "ref_missing_temperature_count",\n                "expression": "sum(case when temperature_c is null then 1 else 0 end)"\n              },\n              {\n                "name": "ref_avg_temperature_c",\n                "expression": "avg(temperature_c)"\n              },\n              {\n                "name": "ref_valid_vibration_count",\n                "expression": "count(vibration_mm_s)"\n              },\n              {\n                "name": "ref_missing_vibration_count",\n                "expression": "sum(case when vibration_mm_s is null then 1 else 0 end)"\n              },\n              {\n                "name": "ref_avg_vibration_mm_s",\n                "expression": "avg(vibration_mm_s)"\n              }\n            ]\n          },\n          {\n            "kind": "join",\n            "name": "hourly_comparison",\n            "left": "hourly_health",\n            "right": "hourly_reference",\n            "keys": [\n              "site_id",\n              "machine_id",\n              "event_hour"\n            ],\n            "how": "left",\n            "cardinality": "many_to_one"\n          },\n          {\n            "kind": "filter",\n            "name": "too_late_records",\n            "input": "readings_raw",\n            "predicate": "arrival_timestamp > timestampadd(HOUR, 2, event_timestamp)"\n          },\n          {\n            "kind": "derive",\n            "name": "late_quarantine",\n            "input": "too_late_records",\n            "columns": [\n              {\n                "name": "quarantine_reason",\n                "expression": "\'arrival_after_two_hour_cutoff\'"\n              }\n            ]\n          }\n        ]\n      }\n    },\n    {\n      "key": "population",\n      "value": "Highest eligible version, latest arrival tie-break; arrivals after two hours quarantined.",\n      "status": "confirmed",\n      "source": "User answers",\n      "impact": "Eligibility.",\n      "severity": "requirements",\n      "bindings": {}\n    },\n    {\n      "key": "time",\n      "value": "Fixed UTC window 2026-01-01 through 2026-01-08; two-hour allowed lateness.",\n      "status": "delegated",\n      "source": "User unavailable; reproducibility default",\n      "impact": "Time boundaries.",\n      "severity": "requirements",\n      "bindings": {}\n    },\n    {\n      "key": "simulation",\n      "value": "10,000 base readings, 3 sites, 12 machines, one 45% volume machine, 5% missing temperature, 3% repeats, 2% corrections, 10% delayed and 1% too late.",\n      "status": "confirmed",\n      "source": "User prompt plus delegated numeric defaults",\n      "impact": "Synthetic evidence.",\n      "severity": "requirements",\n      "bindings": {\n        "tables": [\n          {\n            "name": "readings_raw",\n            "grain": "One physical reading record; repeated rows and corrected versions share reading_id",\n            "rows": 10000,\n            "primary_key": "reading_id",\n            "columns": [\n              {\n                "name": "reading_id",\n                "kind": "id"\n              },\n              {\n                "name": "machine_id",\n                "kind": "category",\n                "categories": [\n                  "machine_01",\n                  "machine_02",\n                  "machine_03",\n                  "machine_04",\n                  "machine_05",\n                  "machine_06",\n                  "machine_07",\n                  "machine_08",\n                  "machine_09",\n                  "machine_10",\n                  "machine_11",\n                  "machine_12"\n                ],\n                "weights": [\n                  0.45,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05,\n                  0.05\n                ]\n              },\n              {\n                "name": "site_id",\n                "kind": "expression",\n                "expression": "case when machine_id in (\'machine_01\',\'machine_02\',\'machine_03\',\'machine_04\') then \'site_01\' when machine_id in (\'machine_05\',\'machine_06\',\'machine_07\',\'machine_08\') then \'site_02\' else \'site_03\' end"\n              },\n              {\n                "name": "event_timestamp",\n                "kind": "timestamp",\n                "start": "2026-01-01T00:00:00Z",\n                "end": "2026-01-08T00:00:00Z"\n              },\n              {\n                "name": "reading_version",\n                "kind": "integer",\n                "minimum": 1,\n                "maximum": 1\n              },\n              {\n                "name": "temperature_c",\n                "kind": "decimal",\n                "minimum": 40,\n                "maximum": 100,\n                "null_rate": 0.05\n              },\n              {\n                "name": "vibration_mm_s",\n                "kind": "decimal",\n                "minimum": 0,\n                "maximum": 20\n              },\n              {\n                "name": "arrival_class",\n                "kind": "category",\n                "categories": [\n                  "on_time",\n                  "delayed",\n                  "too_late"\n                ],\n                "weights": [\n                  0.89,\n                  0.1,\n                  0.01\n                ]\n              },\n              {\n                "name": "arrival_delay_minutes",\n                "kind": "expression",\n                "expression": "case when arrival_class = \'delayed\' then 90 when arrival_class = \'too_late\' then 180 else 0 end"\n              },\n              {\n                "name": "arrival_timestamp",\n                "kind": "expression",\n                "expression": "timestampadd(MINUTE, arrival_delay_minutes, event_timestamp)"\n              },\n              {\n                "name": "microbatch_id",\n                "kind": "expression",\n                "expression": "floor(timestampdiff(SECOND, to_timestamp(\'2026-01-01T00:00:00Z\'), arrival_timestamp) / 3600)"\n              }\n            ],\n            "duplicate_rate": 0.03,\n            "changed_duplicate_rate": 0.02,\n            "version_column": "reading_version",\n            "duplicate_changes": {\n              "temperature_c": "temperature_c + 1.5",\n              "vibration_mm_s": "vibration_mm_s + 0.25",\n              "arrival_delay_minutes": "arrival_delay_minutes + 30",\n              "arrival_timestamp": "timestampadd(MINUTE, 30, arrival_timestamp)",\n              "microbatch_id": "microbatch_id + 1"\n            }\n          }\n        ]\n      }\n    },\n    {\n      "key": "quality",\n      "value": "Deterministic version resolution and reason-coded late quarantine.",\n      "status": "confirmed",\n      "source": "User answers",\n      "impact": "Clean contract.",\n      "severity": "requirements",\n      "bindings": {}\n    },\n    {\n      "key": "acceptance",\n      "value": "Exact reference equality, identity uniqueness, population reconciliation, anomaly bounds.",\n      "status": "confirmed",\n      "source": "User answer",\n      "impact": "Executable success.",\n      "severity": "requirements",\n      "bindings": {\n        "checks": [\n          {\n            "name": "raw_physical_row_count",\n            "kind": "row_count",\n            "table": "readings_raw",\n            "expected": 10500\n          },\n          {\n            "name": "raw_temperature_null_rate",\n            "kind": "null_rate",\n            "table": "readings_raw",\n            "columns": [\n              "temperature_c"\n            ],\n            "expected": 0.05,\n            "tolerance": 0.02\n          },\n          {\n            "name": "resolved_logical_identity",\n            "kind": "unique",\n            "table": "resolved_readings",\n            "columns": [\n              "reading_id"\n            ]\n          },\n          {\n            "name": "resolved_population_bounds",\n            "kind": "row_count",\n            "table": "resolved_readings",\n            "minimum": 9500,\n            "maximum": 10000\n          },\n          {\n            "name": "hourly_metric_reconciliation",\n            "kind": "predicate",\n            "table": "hourly_health",\n            "predicate": "total_reading_count = valid_temperature_count + missing_temperature_count and total_reading_count = valid_vibration_count + missing_vibration_count",\n            "maximum_failures": 0\n          },\n          {\n            "name": "hourly_reference_exact_match",\n            "kind": "predicate",\n            "table": "hourly_comparison",\n            "predicate": "total_reading_count = ref_total_reading_count and valid_temperature_count = ref_valid_temperature_count and missing_temperature_count = ref_missing_temperature_count and coalesce(avg_temperature_c, -9999) = coalesce(ref_avg_temperature_c, -9999) and valid_vibration_count = ref_valid_vibration_count and missing_vibration_count = ref_missing_vibration_count and coalesce(avg_vibration_mm_s, -9999) = coalesce(ref_avg_vibration_mm_s, -9999)",\n            "maximum_failures": 0\n          },\n          {\n            "name": "late_quarantine_present",\n            "kind": "row_count",\n            "table": "late_quarantine",\n            "minimum": 1,\n            "maximum": 500\n          },\n          {\n            "name": "late_quarantine_reason",\n            "kind": "predicate",\n            "table": "late_quarantine",\n            "predicate": "quarantine_reason = \'arrival_after_two_hour_cutoff\' and arrival_timestamp > timestampadd(HOUR, 2, event_timestamp)",\n            "maximum_failures": 0\n          }\n        ]\n      }\n    },\n    {\n      "key": "pipeline",\n      "value": "bounded batch",\n      "status": "confirmed",\n      "source": "User answer and supported-scope boundary",\n      "impact": "Execution semantics.",\n      "severity": "requirements",\n      "bindings": {}\n    },\n    {\n      "key": "persistence",\n      "value": "Disabled by default and runtime opt-in only.",\n      "status": "confirmed",\n      "source": "User prompt",\n      "impact": "Write safety.",\n      "severity": "runtime",\n      "bindings": {}\n    }\n  ],\n  "provisional": false,\n  "provisional_reason": "",\n  "verify_reproducibility": true,\n  "scenario": "pipeline",\n  "tables": [\n    {\n      "name": "readings_raw",\n      "grain": "One physical reading record; repeated rows and corrected versions share reading_id",\n      "rows": 10000,\n      "primary_key": "reading_id",\n      "columns": [\n        {\n          "name": "reading_id",\n          "kind": "id",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": null\n        },\n        {\n          "name": "machine_id",\n          "kind": "category",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [\n            "machine_01",\n            "machine_02",\n            "machine_03",\n            "machine_04",\n            "machine_05",\n            "machine_06",\n            "machine_07",\n            "machine_08",\n            "machine_09",\n            "machine_10",\n            "machine_11",\n            "machine_12"\n          ],\n          "weights": [\n            0.45,\n            0.05,\n            0.05,\n            0.05,\n            0.05,\n            0.05,\n            0.05,\n            0.05,\n            0.05,\n            0.05,\n            0.05,\n            0.05\n          ],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": null\n        },\n        {\n          "name": "site_id",\n          "kind": "expression",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": "case when machine_id in (\'machine_01\',\'machine_02\',\'machine_03\',\'machine_04\') then \'site_01\' when machine_id in (\'machine_05\',\'machine_06\',\'machine_07\',\'machine_08\') then \'site_02\' else \'site_03\' end"\n        },\n        {\n          "name": "event_timestamp",\n          "kind": "timestamp",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": "2026-01-01T00:00:00Z",\n          "end": "2026-01-08T00:00:00Z",\n          "reference_table": null,\n          "expression": null\n        },\n        {\n          "name": "reading_version",\n          "kind": "integer",\n          "minimum": 1,\n          "maximum": 1,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": null\n        },\n        {\n          "name": "temperature_c",\n          "kind": "decimal",\n          "minimum": 40,\n          "maximum": 100,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.05,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": null\n        },\n        {\n          "name": "vibration_mm_s",\n          "kind": "decimal",\n          "minimum": 0,\n          "maximum": 20,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": null\n        },\n        {\n          "name": "arrival_class",\n          "kind": "category",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [\n            "on_time",\n            "delayed",\n            "too_late"\n          ],\n          "weights": [\n            0.89,\n            0.1,\n            0.01\n          ],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": null\n        },\n        {\n          "name": "arrival_delay_minutes",\n          "kind": "expression",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": "case when arrival_class = \'delayed\' then 90 when arrival_class = \'too_late\' then 180 else 0 end"\n        },\n        {\n          "name": "arrival_timestamp",\n          "kind": "expression",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": "timestampadd(MINUTE, arrival_delay_minutes, event_timestamp)"\n        },\n        {\n          "name": "microbatch_id",\n          "kind": "expression",\n          "minimum": 0,\n          "maximum": 100,\n          "categories": [],\n          "weights": [],\n          "true_rate": 0.5,\n          "null_rate": 0.0,\n          "start": null,\n          "end": null,\n          "reference_table": null,\n          "expression": "floor(timestampdiff(SECOND, to_timestamp(\'2026-01-01T00:00:00Z\'), arrival_timestamp) / 3600)"\n        }\n      ],\n      "duplicate_rate": 0.03,\n      "changed_duplicate_rate": 0.02,\n      "version_column": "reading_version",\n      "duplicate_changes": {\n        "temperature_c": "temperature_c + 1.5",\n        "vibration_mm_s": "vibration_mm_s + 0.25",\n        "arrival_delay_minutes": "arrival_delay_minutes + 30",\n        "arrival_timestamp": "timestampadd(MINUTE, 30, arrival_timestamp)",\n        "microbatch_id": "microbatch_id + 1"\n      }\n    }\n  ],\n  "operations": [\n    {\n      "kind": "filter",\n      "name": "eligible_readings",\n      "input": "readings_raw",\n      "predicate": "arrival_delay_minutes <= 120"\n    },\n    {\n      "kind": "deduplicate",\n      "name": "resolved_readings",\n      "input": "eligible_readings",\n      "keys": [\n        "reading_id"\n      ],\n      "order_by": [\n        {\n          "column": "reading_version",\n          "direction": "desc"\n        },\n        {\n          "column": "arrival_timestamp",\n          "direction": "desc"\n        }\n      ]\n    },\n    {\n      "kind": "derive",\n      "name": "resolved_with_hour",\n      "input": "resolved_readings",\n      "columns": [\n        {\n          "name": "event_hour",\n          "expression": "timestampadd(SECOND, -second(event_timestamp), timestampadd(MINUTE, -minute(event_timestamp), event_timestamp))"\n        }\n      ]\n    },\n    {\n      "kind": "aggregate",\n      "name": "hourly_health",\n      "input": "resolved_with_hour",\n      "group_by": [\n        "site_id",\n        "machine_id",\n        "event_hour"\n      ],\n      "measures": [\n        {\n          "name": "total_reading_count",\n          "expression": "count(*)"\n        },\n        {\n          "name": "valid_temperature_count",\n          "expression": "count(temperature_c)"\n        },\n        {\n          "name": "missing_temperature_count",\n          "expression": "sum(case when temperature_c is null then 1 else 0 end)"\n        },\n        {\n          "name": "avg_temperature_c",\n          "expression": "avg(temperature_c)"\n        },\n        {\n          "name": "valid_vibration_count",\n          "expression": "count(vibration_mm_s)"\n        },\n        {\n          "name": "missing_vibration_count",\n          "expression": "sum(case when vibration_mm_s is null then 1 else 0 end)"\n        },\n        {\n          "name": "avg_vibration_mm_s",\n          "expression": "avg(vibration_mm_s)"\n        }\n      ]\n    },\n    {\n      "kind": "filter",\n      "name": "reference_eligible",\n      "input": "readings_raw",\n      "predicate": "arrival_timestamp <= timestampadd(HOUR, 2, event_timestamp)"\n    },\n    {\n      "kind": "deduplicate",\n      "name": "reference_resolved",\n      "input": "reference_eligible",\n      "keys": [\n        "reading_id"\n      ],\n      "order_by": [\n        {\n          "column": "reading_version",\n          "direction": "desc"\n        },\n        {\n          "column": "arrival_timestamp",\n          "direction": "desc"\n        }\n      ]\n    },\n    {\n      "kind": "derive",\n      "name": "reference_with_hour",\n      "input": "reference_resolved",\n      "columns": [\n        {\n          "name": "event_hour",\n          "expression": "timestampadd(SECOND, -second(event_timestamp), timestampadd(MINUTE, -minute(event_timestamp), event_timestamp))"\n        }\n      ]\n    },\n    {\n      "kind": "aggregate",\n      "name": "hourly_reference",\n      "input": "reference_with_hour",\n      "group_by": [\n        "site_id",\n        "machine_id",\n        "event_hour"\n      ],\n      "measures": [\n        {\n          "name": "ref_total_reading_count",\n          "expression": "count(*)"\n        },\n        {\n          "name": "ref_valid_temperature_count",\n          "expression": "count(temperature_c)"\n        },\n        {\n          "name": "ref_missing_temperature_count",\n          "expression": "sum(case when temperature_c is null then 1 else 0 end)"\n        },\n        {\n          "name": "ref_avg_temperature_c",\n          "expression": "avg(temperature_c)"\n        },\n        {\n          "name": "ref_valid_vibration_count",\n          "expression": "count(vibration_mm_s)"\n        },\n        {\n          "name": "ref_missing_vibration_count",\n          "expression": "sum(case when vibration_mm_s is null then 1 else 0 end)"\n        },\n        {\n          "name": "ref_avg_vibration_mm_s",\n          "expression": "avg(vibration_mm_s)"\n        }\n      ]\n    },\n    {\n      "kind": "join",\n      "name": "hourly_comparison",\n      "left": "hourly_health",\n      "right": "hourly_reference",\n      "keys": [\n        "site_id",\n        "machine_id",\n        "event_hour"\n      ],\n      "how": "left",\n      "cardinality": "many_to_one"\n    },\n    {\n      "kind": "filter",\n      "name": "too_late_records",\n      "input": "readings_raw",\n      "predicate": "arrival_timestamp > timestampadd(HOUR, 2, event_timestamp)"\n    },\n    {\n      "kind": "derive",\n      "name": "late_quarantine",\n      "input": "too_late_records",\n      "columns": [\n        {\n          "name": "quarantine_reason",\n          "expression": "\'arrival_after_two_hour_cutoff\'"\n        }\n      ]\n    }\n  ],\n  "outputs": [\n    "readings_raw",\n    "resolved_readings",\n    "hourly_health",\n    "hourly_comparison",\n    "late_quarantine"\n  ],\n  "checks": [\n    {\n      "name": "raw_physical_row_count",\n      "kind": "row_count",\n      "table": "readings_raw",\n      "columns": [],\n      "expected": 10500,\n      "minimum": null,\n      "maximum": null,\n      "tolerance": 0.0,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": null,\n      "maximum_failures": 0\n    },\n    {\n      "name": "raw_temperature_null_rate",\n      "kind": "null_rate",\n      "table": "readings_raw",\n      "columns": [\n        "temperature_c"\n      ],\n      "expected": 0.05,\n      "minimum": null,\n      "maximum": null,\n      "tolerance": 0.02,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": null,\n      "maximum_failures": 0\n    },\n    {\n      "name": "resolved_logical_identity",\n      "kind": "unique",\n      "table": "resolved_readings",\n      "columns": [\n        "reading_id"\n      ],\n      "expected": null,\n      "minimum": null,\n      "maximum": null,\n      "tolerance": 0.0,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": null,\n      "maximum_failures": 0\n    },\n    {\n      "name": "resolved_population_bounds",\n      "kind": "row_count",\n      "table": "resolved_readings",\n      "columns": [],\n      "expected": null,\n      "minimum": 9500,\n      "maximum": 10000,\n      "tolerance": 0.0,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": null,\n      "maximum_failures": 0\n    },\n    {\n      "name": "hourly_metric_reconciliation",\n      "kind": "predicate",\n      "table": "hourly_health",\n      "columns": [],\n      "expected": null,\n      "minimum": null,\n      "maximum": null,\n      "tolerance": 0.0,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": "total_reading_count = valid_temperature_count + missing_temperature_count and total_reading_count = valid_vibration_count + missing_vibration_count",\n      "maximum_failures": 0\n    },\n    {\n      "name": "hourly_reference_exact_match",\n      "kind": "predicate",\n      "table": "hourly_comparison",\n      "columns": [],\n      "expected": null,\n      "minimum": null,\n      "maximum": null,\n      "tolerance": 0.0,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": "total_reading_count = ref_total_reading_count and valid_temperature_count = ref_valid_temperature_count and missing_temperature_count = ref_missing_temperature_count and coalesce(avg_temperature_c, -9999) = coalesce(ref_avg_temperature_c, -9999) and valid_vibration_count = ref_valid_vibration_count and missing_vibration_count = ref_missing_vibration_count and coalesce(avg_vibration_mm_s, -9999) = coalesce(ref_avg_vibration_mm_s, -9999)",\n      "maximum_failures": 0\n    },\n    {\n      "name": "late_quarantine_present",\n      "kind": "row_count",\n      "table": "late_quarantine",\n      "columns": [],\n      "expected": null,\n      "minimum": 1,\n      "maximum": 500,\n      "tolerance": 0.0,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": null,\n      "maximum_failures": 0\n    },\n    {\n      "name": "late_quarantine_reason",\n      "kind": "predicate",\n      "table": "late_quarantine",\n      "columns": [],\n      "expected": null,\n      "minimum": null,\n      "maximum": null,\n      "tolerance": 0.0,\n      "reference_table": null,\n      "reference_columns": [],\n      "predicate": "quarantine_reason = \'arrival_after_two_hour_cutoff\' and arrival_timestamp > timestampadd(HOUR, 2, event_timestamp)",\n      "maximum_failures": 0\n    }\n  ],\n  "scale_acknowledged": false\n}')
SPEC_FINGERPRINT = '0268972c86dc651f85506684e2e34af0bbf4a9826ccf5d76a08d62acd1550f6c'


# COMMAND ----------

# MAGIC %md
# MAGIC ## How this pipeline executes
# MAGIC 
# MAGIC Spark range and seeded expressions generate partitioned rows. Python builds plans; workers process data. Filters, projections and derived expressions are lazy transformations; counts, displays and writes trigger execution. Planning errors may still appear before an action. No full dataset is collected.
# MAGIC 
# MAGIC Execution partitions split work; Delta partition columns organize storage. This compiler does not choose a fixed partition count, cache automatically, or partition stored tables without evidence. Row width, key distribution, available resources and query patterns matter as much as row count.
# MAGIC 
# MAGIC **eligible_readings (filter):** Filters eligible rows with the declared business predicate; usually partition-local.
# MAGIC 
# MAGIC **resolved_readings (deduplicate):** A key-partitioned ordered window chooses the declared winning version. It generally shuffles and sorts. Ordering and tie-breaking must match the business rule.
# MAGIC 
# MAGIC **resolved_with_hour (derive):** Native expressions add business fields. No Python UDF is introduced.
# MAGIC 
# MAGIC **hourly_health (aggregate):** Grouping generally shuffles keys after partial aggregation. Hot keys may concentrate work; AQE skew-join handling is not a universal aggregation fix. Measure before salting.
# MAGIC 
# MAGIC **reference_eligible (filter):** Filters eligible rows with the declared business predicate; usually partition-local.
# MAGIC 
# MAGIC **reference_resolved (deduplicate):** A key-partitioned ordered window chooses the declared winning version. It generally shuffles and sorts. Ordering and tie-breaking must match the business rule.
# MAGIC 
# MAGIC **reference_with_hour (derive):** Native expressions add business fields. No Python UDF is introduced.
# MAGIC 
# MAGIC **hourly_reference (aggregate):** Grouping generally shuffles keys after partial aggregation. Hot keys may concentrate work; AQE skew-join handling is not a universal aggregation fix. Measure before salting.
# MAGIC 
# MAGIC **hourly_comparison (join):** Joins combine declared keys. Broadcast is a candidate only when the right side is small; inspect the actual plan. A many-to-one uniqueness guard triggers an action. Uneven keys can cause skew, and duplicate right keys can multiply rows.
# MAGIC 
# MAGIC **too_late_records (filter):** Filters eligible rows with the declared business predicate; usually partition-local.
# MAGIC 
# MAGIC **late_quarantine (derive):** Native expressions add business fields. No Python UDF is introduced.
# MAGIC 
# MAGIC Weighted categories deliberately reproduce uneven populations; verify the configured rates before diagnosing skew. Synthetic skew does not prove production timings.
# MAGIC 
# MAGIC Validation has explicit actions and may recompute shared lineage. Combine checks or reuse materialized data only after measurement and checking serverless support. Small correctness tests do not establish billion-row performance. Inspect the executed profile for spills, task imbalance, shuffle volume and timing; a planned Exchange is expected redistribution.

# COMMAND ----------

def generate_pipeline(spark, cfg, input_partitions=None):
    import math
    from datetime import date, datetime

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    frames = {}
    dimensions = {table["name"]: table for table in cfg["tables"]}

    def uniform(salt):
        return F.pmod(F.xxhash64(F.col("__row_id"), F.lit(cfg["seed"]), F.lit(salt)), F.lit(1000000)).cast(
            "double"
        ) / F.lit(1000000.0)

    for table in cfg["tables"]:
        options = {"numPartitions": input_partitions} if input_partitions else {}
        frame = spark.range(table["rows"], **options).withColumnRenamed("id", "__row_id")
        for column in table["columns"]:
            name = column["name"]
            kind = column["kind"]
            salt = table["name"] + ":" + name
            u = uniform(salt)
            if kind == "id":
                value = F.col("__row_id") + F.lit(1)
            elif kind == "integer":
                value = (F.floor(u * (column["maximum"] - column["minimum"] + 1)) + column["minimum"]).cast(
                    "long"
                )
            elif kind == "decimal":
                value = F.round(column["minimum"] + u * (column["maximum"] - column["minimum"]), 2).cast(
                    "decimal(18,2)"
                )
            elif kind == "boolean":
                value = u < F.lit(column["true_rate"])
            elif kind == "category":
                categories = column["categories"]
                weights = column["weights"] or [1.0] * len(categories)
                total = sum(weights)
                cumulative = 0.0
                value = None
                for category, weight in zip(categories[:-1], weights[:-1]):
                    cumulative += weight / total
                    condition = u < F.lit(cumulative)
                    value = (
                        F.when(condition, F.lit(category))
                        if value is None
                        else value.when(condition, F.lit(category))
                    )
                value = F.lit(categories[-1]) if value is None else value.otherwise(F.lit(categories[-1]))
            elif kind == "date":
                start = date.fromisoformat(column["start"])
                end = date.fromisoformat(column["end"])
                value = F.date_add(F.lit(start), F.floor(u * ((end - start).days + 1)).cast("int"))
            elif kind == "timestamp":
                start = datetime.fromisoformat(column["start"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(column["end"].replace("Z", "+00:00"))
                span = int((end - start).total_seconds())
                value = F.timestamp_seconds(
                    F.lit(int(start.timestamp())) + F.floor(u * (span + 1)).cast("long")
                )
            elif kind == "foreign_key":
                reference = dimensions[column["reference_table"]]
                value = (F.floor(u * reference["rows"]) + F.lit(1)).cast("long")
            elif kind == "expression":
                value = F.expr(column["expression"])
            else:
                raise ValueError("Unsupported source column kind")
            if column["null_rate"]:
                value = F.when(uniform(salt + ":null") < column["null_rate"], F.lit(None)).otherwise(value)
            frame = frame.withColumn(name, value)
        base = frame
        duplicate_count = math.floor(table["rows"] * table["duplicate_rate"])
        changed_count = math.floor(table["rows"] * table["changed_duplicate_rate"])
        if duplicate_count:
            frame = frame.unionByName(base.filter(F.col("__row_id") < duplicate_count))
        if changed_count:
            changed = base.filter(F.col("__row_id") < changed_count)
            updates = {key: F.expr(expression) for key, expression in table["duplicate_changes"].items()}
            updates[table["version_column"]] = F.col(table["version_column"]) + F.lit(1)
            changed = changed.select(*[updates.get(key, F.col(key)).alias(key) for key in base.columns])
            frame = frame.unionByName(changed)
        frames[table["name"]] = frame.drop("__row_id")

    for operation in cfg["operations"]:
        kind = operation["kind"]
        if kind == "join":
            left, right = frames[operation["left"]], frames[operation["right"]]
            keys = operation["keys"]
            if right.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
                raise ValueError("Many-to-one join rejected: right-side join keys are not unique")
            frame = left.join(right, keys, operation["how"])
        else:
            frame = frames[operation["input"]]
            if kind == "derive":
                for column in operation["columns"]:
                    frame = frame.withColumn(column["name"], F.expr(column["expression"]))
            elif kind == "filter":
                frame = frame.filter(F.expr(operation["predicate"]))
            elif kind == "select":
                frame = frame.select(*operation["columns"])
            elif kind == "deduplicate":
                ranking_keys = list(
                    dict.fromkeys(operation["keys"] + [key["column"] for key in operation["order_by"]])
                )
                if (
                    frame.dropDuplicates()
                    .groupBy(*ranking_keys)
                    .count()
                    .filter(F.col("count") > 1)
                    .limit(1)
                    .count()
                ):
                    raise ValueError(
                        "Deduplication rejected: conflicting rows tie on keys and declared ordering"
                    )
                order = [
                    F.col(key["column"]).asc_nulls_first()
                    if key["direction"] == "asc"
                    else F.col(key["column"]).desc_nulls_last()
                    for key in operation["order_by"]
                ]
                # Identical ties represent indistinguishable output rows; conflicts were rejected.
                order.extend(F.col(key).asc_nulls_first() for key in sorted(frame.columns))
                window = Window.partitionBy(*operation["keys"]).orderBy(*order)
                frame = (
                    frame.withColumn("__rank", F.row_number().over(window))
                    .filter(F.col("__rank") == 1)
                    .drop("__rank")
                )
            elif kind == "aggregate":
                frame = frame.groupBy(*operation["group_by"]).agg(
                    *[
                        F.expr(measure["expression"]).alias(measure["name"])
                        for measure in operation["measures"]
                    ]
                )
            else:
                raise ValueError("Unsupported pipeline operation")
        frames[operation["name"]] = frame
    return {name: frames[name] for name in cfg["outputs"]}

tables = generate_pipeline(spark, cfg)
tables = {name: tables[name] for name in cfg['outputs']}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the execution plan
# MAGIC explain(formatted) requests planning evidence, not a row scan. Look for Exchange (redistribution), BroadcastHashJoin/BroadcastExchange, and aggregate operators. Names may differ by runtime/engine. Adaptive execution can change the final plan: inspect the executed query profile after an action for task skew, spills, shuffle volume and actual timings. This cell does not measure partition counts or prove that a suggested optimization helps. Serverless uses DataFrame APIs; no RDD introspection is used.
# MAGIC 
# MAGIC References: [Spark execution](https://spark.apache.org/docs/latest/rdd-programming-guide.html) · [Databricks serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations).

# COMMAND ----------

for name in cfg['outputs']:
    print('Plan:', name)
    tables[name].explain('formatted')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Acceptance evidence
# MAGIC Checks are explicit assertions from the contract, not proof that every business rule is correct. No checks are silently skipped.

# COMMAND ----------

def validate_pipeline(tables, cfg):
    from functools import reduce

    missing = set(cfg["outputs"]) - set(tables)
    if missing:
        raise ValueError("Missing declared outputs: " + ", ".join(sorted(missing)))
    from pyspark.sql import functions as F

    counts = {name: tables[name].count() for name in cfg["outputs"]}
    checks = []
    for check in cfg["checks"]:
        frame = tables[check["table"]]
        kind = check["kind"]
        entry = {"name": check["name"], "kind": kind, "table": check["table"]}
        if kind == "row_count":
            actual = counts[check["table"]]
            passed = check["expected"] is None or actual == check["expected"]
            passed = passed and (check["minimum"] is None or actual >= check["minimum"])
            passed = passed and (check["maximum"] is None or actual <= check["maximum"])
        elif kind == "unique":
            columns = check["columns"]
            duplicates = frame.groupBy(*columns).count().filter(F.col("count") > 1).count()
            nulls = frame.filter(reduce(lambda a, b: a | b, [F.col(key).isNull() for key in columns])).count()
            actual = {"duplicate_groups": duplicates, "null_key_rows": nulls}
            passed = duplicates == 0 and nulls == 0
        elif kind == "null_rate":
            nulls = frame.filter(F.col(check["columns"][0]).isNull()).count()
            actual = nulls / counts[check["table"]] if counts[check["table"]] else 0.0
            passed = abs(actual - check["expected"]) <= check["tolerance"]
        elif kind == "foreign_key":
            columns = check["columns"]
            reference = tables[check["reference_table"]]
            valid = reduce(lambda a, b: a & b, [F.col(key).isNotNull() for key in columns])
            keys = ["join_key_" + str(index) for index in range(len(columns))]
            left = frame.filter(valid).select(*[F.col(key).alias(alias) for key, alias in zip(columns, keys)])
            right = reference.select(
                *[F.col(key).alias(alias) for key, alias in zip(check["reference_columns"], keys)]
            ).distinct()
            actual = left.join(right, keys, "left_anti").count()
            passed = actual <= check["maximum_failures"]
        elif kind == "predicate":
            actual = frame.filter(
                ~F.coalesce(F.expr(check["predicate"]).cast("boolean"), F.lit(False))
            ).count()
            passed = actual <= check["maximum_failures"]
        else:
            raise ValueError("Unsupported acceptance check")
        entry.update(passed=bool(passed), actual=actual)
        checks.append(entry)
    return {"passed": all(check["passed"] for check in checks), "counts": counts, "checks": checks}

summary = validate_pipeline(tables, cfg)
summary['spec_fingerprint'] = SPEC_FINGERPRINT
summary['generator_version'] = '0.1.0'
print(json.dumps(summary, indent=2))
if not summary['passed']:
    raise ValueError('Acceptance checks failed; inspect summary')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Small-run reproducibility
# MAGIC Generate ranges with three input partitions; no input repartition call is needed. Equality comparisons can add shuffles and actions. This is not a performance benchmark.

# COMMAND ----------

repeated = generate_pipeline(spark, cfg, input_partitions=3)
for name, frame in tables.items():
    other = repeated[name]
    if frame.exceptAll(other).limit(1).count() or other.exceptAll(frame).limit(1).count():
        raise ValueError('Same-seed comparison failed: ' + name)
summary['reproducibility'] = {'same_seed_different_input_partitions': True}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime storage choice
# MAGIC Choose in-memory or save to Delta. When saving, enter your own catalog, schema and unique prefix. Only declared outputs are saved. Writes fail on collisions; multi-table writes are not transactional.
# MAGIC 
# MAGIC **Storage versus execution partitions:** execution partitions distribute tasks; Delta table partition columns organize stored data. This template does not use partitionBy and does not repartition just to write. Avoid choosing a high-cardinality table partition key or a fixed file count without data-volume and query evidence. File count is not guaranteed to equal input partition count. The reader's separate Delta scans break generation lineage, but its validation actions can scan repeatedly. Filters may enable pruning; the actual plan/profile is the evidence.

# COMMAND ----------

persist = False
target_catalog = ""
target_schema = ""
table_prefix = ""
if 'dbutils' in globals():
    dbutils.widgets.dropdown('persist', 'false', ['false', 'true'], 'Save to Delta?')
    defaults = [('target_catalog', ''), ('target_schema', ''), ('table_prefix', '')]
    for key, default in defaults:
        dbutils.widgets.text(key, default)
    persist = dbutils.widgets.get('persist').lower() == 'true'
    target_catalog = dbutils.widgets.get('target_catalog')
    target_schema = dbutils.widgets.get('target_schema')
    table_prefix = dbutils.widgets.get('table_prefix')
saved_tables = {}
if persist:
    for value in [target_catalog, target_schema, table_prefix]:
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,62}', value):
            raise ValueError('Catalog, schema and prefix must be simple SQL identifiers')
    targets = {key: f'{target_catalog}.{target_schema}.{table_prefix}_{key}' for key in tables}
    if any(len(target.rsplit('.', 1)[1]) > 63 for target in targets.values()):
        raise ValueError('Combined table name must be <= 63 characters')
    existing = [target for target in targets.values() if spark.catalog.tableExists(target)]
    if existing:
        raise ValueError('Targets already exist; use a new prefix: ' + ', '.join(existing))
    for key, frame in tables.items():
        frame.write.format('delta').mode('errorifexists').saveAsTable(targets[key])
        saved_tables[key] = targets[key]


# COMMAND ----------

summary['saved_tables'] = saved_tables
if 'dbutils' in globals():
    dbutils.notebook.exit(json.dumps(summary))

