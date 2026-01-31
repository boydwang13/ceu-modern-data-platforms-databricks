# Databricks notebook source
# Input data and working folders locations
SOURCE_LOCATION = "/Volumes/dbx_course/source/files/datasets"
ASIGNMENT_SOURCE_LOCATION = "/Volumes/dbx_course/source/files/assignment"
TARGET_LOCATION = workdir = "/Volumes/dbx_course/target/files/"

# Core data paths
sales_path = f"{SOURCE_LOCATION}/ecommerce/sales/sales.delta"
users_path = f"{SOURCE_LOCATION}/ecommerce/users/users.delta"
events_path = f"{SOURCE_LOCATION}/ecommerce/events/events.delta"
products_path = f"{SOURCE_LOCATION}/products/products.delta"


# COMMAND ----------

from types import SimpleNamespace
DA = SimpleNamespace(
    paths = SimpleNamespace(
        datasets=SOURCE_LOCATION,
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

displayHTML("""
<div style="font-family: sans-serif; padding: 10px;">
    <h2 style="color: #2e7d32;">✅ Classroom Setup Complete!</h2>
    <p><strong>Catalog:</strong> <code>dbx_course</code></p>

    <h3>Dataset Locations</h3>
    <table style="border-collapse: collapse; width: 100%; max-width: 800px;">
        <tr style="background-color: #f5f5f5;">
            <th style="text-align: left; padding: 8px; border: 1px solid #ddd;">Dataset</th>
            <th style="text-align: left; padding: 8px; border: 1px solid #ddd;">Path</th>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>DA.paths.datasets</code></td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>""" + DA.paths.datasets + """</code></td>
        </tr>
        <tr style="background-color: #f9f9f9;">
            <td style="padding: 8px; border: 1px solid #ddd;"><code>DA.paths.workdir</code></td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>""" + DA.paths.workdir + """</code></td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>DA.paths.users</code></td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>""" + DA.paths.users + """</code></td>
        </tr>
        <tr style="background-color: #f9f9f9;">
            <td style="padding: 8px; border: 1px solid #ddd;"><code>DA.paths.sales</code></td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>""" + DA.paths.sales + """</code></td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>DA.paths.events</code></td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>""" + DA.paths.events + """</code></td>
        </tr>
        <tr style="background-color: #f9f9f9;">
            <td style="padding: 8px; border: 1px solid #ddd;"><code>DA.paths.products</code></td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>""" + DA.paths.products + """</code></td>
        </tr>
    </table>
</div>
""")
