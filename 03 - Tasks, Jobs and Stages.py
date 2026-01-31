# Databricks notebook source
# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md # Distributed Count Example

# COMMAND ----------

# MAGIC %md Let's disable the Adaptive Query Execution (More on that later)

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# MAGIC %md How many bytes are in a partition (maximum)?

# COMMAND ----------

max_bytes_in_part = spark.conf.get('spark.sql.files.maxPartitionBytes')
print(f"Max bytes in a partition: {max_bytes_in_part}")

# Parse the value, handling both numeric strings and strings with suffixes like "128m" or "128MB"
import re
match = re.match(r'^(\d+)([a-zA-Z]*)$', max_bytes_in_part)
if match:
    value = int(match.group(1))
    suffix = match.group(2).lower()
    if suffix in ('', 'b'):
        max_mib_in_part = value / 1024 / 1024
    elif suffix in ('k', 'kb'):
        max_mib_in_part = value / 1024
    elif suffix in ('m', 'mb'):
        max_mib_in_part = value
    elif suffix in ('g', 'gb'):
        max_mib_in_part = value * 1024
    else:
        max_mib_in_part = value / 1024 / 1024  # default to bytes
else:
    max_mib_in_part = int(max_bytes_in_part) / 1024 / 1024

print(f"This is {max_mib_in_part} megabytes")

# COMMAND ----------

# MAGIC %md How many cores do we have in the cluster?

# COMMAND ----------

print(f"Number of cores: {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

# Get the first JSON file from the directory
json_dir = f"{SOURCE_LOCATION}/ecommerce/events/events-1m.json/"
json_files = [f.path for f in dbutils.fs.ls(json_dir) if f.path.endswith('.json')]
file_path = json_files[0] if json_files else json_dir

# COMMAND ----------

display(dbutils.fs.ls(file_path))

# COMMAND ----------

print(dbutils.fs.head(file_path).replace("\\n","\n"))

# COMMAND ----------

df = spark.read.json(file_path)

# COMMAND ----------

# MAGIC %md Guess: How many partitions?

# COMMAND ----------

# df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md Guess: How many jobs? How many stages? How many tasks?

# COMMAND ----------

df.count()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled', 'true')

# COMMAND ----------

df.count()
