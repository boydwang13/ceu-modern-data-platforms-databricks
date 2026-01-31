# Databricks notebook source
# Input data and working folders locations
MY_VOLUME="/Volumes/dbx_course/source/files"
SOURCE_LOCATION=f"{MY_VOLUME}/source"
ASIGNMENT_SOURCE_LOCATION=f"{MY_VOLUME}/assignment"
TARGET_LOCATION=f"{MY_VOLUME}/target"

# Define paths for data access

# Core data paths
sales_path = f"{SOURCE_LOCATION}/ecommerce/sales/sales.delta"
users_path = f"{SOURCE_LOCATION}/ecommerce/users/users.delta"
events_path = f"{SOURCE_LOCATION}/ecommerce/events/events.delta"
products_path = f"{SOURCE_LOCATION}/products/products.delta"

# Working directories - for lab exercises
workdir = "/Volumes/dbx_course/target/files"


# COMMAND ----------

from types import SimpleNamespace
DA = SimpleNamespace(
    paths = SimpleNamespace(
        datasets = MY_VOLUME,
        workdir=workdir,
        sales=sales_path,
        events=events_path,
        users=users_path,
        products=products_path,
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dbx_course; -- this is like a Database in Snowflake
# MAGIC USE CATALOG dbx_course;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS source;
# MAGIC CREATE SCHEMA IF NOT EXISTS target;
# MAGIC USE SCHEMA target;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS dbx_course.source.files;
# MAGIC CREATE VOLUME IF NOT EXISTS dbx_course.target.files;
# MAGIC

# COMMAND ----------

# Check if data already exists before copying - sources
import_config = [
{
        's3_source': 's3a://dbx-data-public/v03/',
        'copy_target': SOURCE_LOCATION
    }
]

try:
    dbutils.fs.ls("s3://dbx-class-exercise-datasets/fifa/")

    import_config.append({
        's3_source': 's3://dbx-class-exercise-datasets/',
        'copy_target': ASIGNMENT_SOURCE_LOCATION
    })
except:
    print("Exercise datasets not present yet, skipping copy. This is OK.")

for location in import_config:
    try:
        files = dbutils.fs.ls(location['s3_source'])
        if len(files) > 0:
            print(f"{location['copy_target']} already exists, skipping copy. This is OK.")
        else:
            raise Exception("Empty directory")
    except:
        print(f"Copying data from {location['s3_source']}...")
        dbutils.fs.cp(location['s3_source'], location['copy_target'], recurse=True)
        print(f"Done! files are copied into {location['copy_target']}")


# COMMAND ----------

try:
    spark.sql("DROP VIEW IF EXISTS sales;")
except:
    spark.sql("DROP TABLE IF EXISTS sales;")

try:
    spark.sql("DROP VIEW IF EXISTS users;")
except:
    spark.sql("DROP TABLE IF EXISTS users;")

try:
    spark.sql("DROP VIEW IF EXISTS products;")
except:
    spark.sql("DROP TABLE IF EXISTS products;")

try:
    spark.sql("DROP VIEW IF EXISTS events;")
except:
    spark.sql("DROP TABLE IF EXISTS events;")

# COMMAND ----------

# MAGIC %run ./Common-Functions

# COMMAND ----------

# Reset working directory for lab exercises
reset_working_dir()

# COMMAND ----------

displayHTML("✅ Classroom setup complete! 🎉")
displayHTML(f"<br/>")
displayHTML(f"✅ Catalog 'dbx_course' present")
displayHTML(f"<br/>")
displayHTML(f"<b>Available paths via DA.paths:</b>")
displayHTML(f"<pre>DA.paths.datasets</pre> → {DA.paths.datasets}")
displayHTML(f"<pre>DA.paths.workdir</pre> → {DA.paths.workdir}")
displayHTML(f"<pre>DA.paths.users</pre> → {DA.paths.users}")
displayHTML(f"<pre>DA.paths.events</pre> → {DA.paths.events}")
displayHTML(f"<pre>DA.paths.products</pre> → {DA.paths.products}")
displayHTML(f"<pre>DA.paths.sales</pre> → {DA.paths.sales}")
