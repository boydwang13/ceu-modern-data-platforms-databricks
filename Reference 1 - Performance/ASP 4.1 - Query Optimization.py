# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Query Optimization
# MAGIC
# MAGIC We'll explore query plans and optimizations for several examples including logical optimizations and examples with and without predicate pushdown.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Logical optimizations
# MAGIC 1. Predicate pushdown
# MAGIC 1. No predicate pushdown
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.explain.html#pyspark.sql.DataFrame.explain" target="_blank">DataFrame</a>: **`explain`**

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let’s run our set up cell, and get our initial DataFrame stored in the variable **`df`**. Displaying this DataFrame shows us events data.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

df = spark.read.format("delta").load(DA.paths.events)
display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Logical Optimization
# MAGIC
# MAGIC **`explain(..)`** prints the query plans, optionally formatted by a given explain mode. Compare the following logical plan & physical plan, noting how Catalyst handled the multiple **`filter`** transformations.

# COMMAND ----------

from pyspark.sql.functions import col

limit_events_df = (
    df.filter(col("event_name") != "reviews")
    .filter(col("event_name") != "checkout")
    .filter(col("event_name") != "register")
    .filter(col("event_name") != "email_coupon")
    .filter(col("event_name") != "cc_info")
    .filter(col("event_name") != "delivery")
    .filter(col("event_name") != "shipping_info")
    .filter(col("event_name") != "press")
)

limit_events_df.explain(True)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Of course, we could have written the query originally using a single **`filter`** condition ourselves. Compare the previous and following query plans.

# COMMAND ----------

better_df = df.filter(
    (col("event_name").isNotNull())
    & (col("event_name") != "reviews")
    & (col("event_name") != "checkout")
    & (col("event_name") != "register")
    & (col("event_name") != "email_coupon")
    & (col("event_name") != "cc_info")
    & (col("event_name") != "delivery")
    & (col("event_name") != "shipping_info")
    & (col("event_name") != "press")
)

better_df.explain(True)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Of course, we wouldn't write the following code intentionally, but in a long, complex query you might not notice the duplicate filter conditions. Let's see what Catalyst does with this query.

# COMMAND ----------

stupid_df = (
    df.filter(col("event_name") != "finalize")
    .filter(col("event_name") != "finalize")
    .filter(col("event_name") != "finalize")
    .filter(col("event_name") != "finalize")
    .filter(col("event_name") != "finalize")
)

stupid_df.explain(True)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Caching
# MAGIC
# MAGIC By default the data of a DataFrame is present on a Spark cluster only while it is being processed during a query -- it is not automatically persisted on the cluster afterwards. (Spark is a data processing engine, not a data storage system.) You can explicitly request Spark to persist a DataFrame on the cluster by invoking its **`cache`** method.
# MAGIC
# MAGIC If you do cache a DataFrame, you should always explicitly evict it from cache by invoking **`unpersist`** when you no longer need it.
# MAGIC
# MAGIC ✨ Caching a DataFrame can be appropriate if you are certain that you will use the same DataFrame multiple times, as in:
# MAGIC
# MAGIC - Exploratory data analysis
# MAGIC - Machine learning model training
# MAGIC
# MAGIC _Warning_: Aside from those use cases, you should **not** cache DataFrames because it is likely that you'll *degrade* the performance of your application.
# MAGIC
# MAGIC - Caching consumes cluster resources that could otherwise be used for task execution
# MAGIC - Caching can prevent Spark from performing query optimizations, as shown in the next example

# COMMAND ----------

# MAGIC %md
# MAGIC Licence: <a target='_blank' href='https://github.com/databricks-academy/apache-spark-programming-with-databricks/blob/published/LICENSE'>Creative Commons Zero v1.0 Universal</a>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
