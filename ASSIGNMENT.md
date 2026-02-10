# Exercise Datasets

## Setup

Before accessing the datasets, run the classroom setup in a new cell:

```
%run ./Includes/Classroom-Setup
```

This will:
- Set up the catalog and schema (`dbx_course.target`)
- Copy the exercise datasets to the Unity Catalog volume (if not copied yet)

---

## Important: Cell Format

Each command below should go in its **own separate cell**. Do NOT:
- Paste multiple commands into a single cell
- Add `# comment` lines before magic commands (like `%fs` or `%sql`)

Magic commands must be the **first line** in a cell, otherwise Databricks treats it as Python code.

---

## Dataset Location

All datasets are available at: `/Volumes/dbx_course/source/files/assignment/`

---

## Climate Change

Global temperature data from 1750 onwards.

| File | Size |
|------|------|
| `GlobalTemperatures.csv` | 201 KB |
| `GlobalLandTemperaturesByCountry.csv` | 22 MB |
| `GlobalLandTemperaturesByState.csv` | 29 MB |
| `GlobalLandTemperaturesByMajorCity.csv` | 13 MB |
| `GlobalLandTemperaturesByCity.csv` | 508 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/climate_change/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/climate_change/GlobalTemperatures.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/climate_change/GlobalTemperatures.csv", header=True, inferSchema=True)
display(df)
```

---

## Consumer Reviews (Amazon Products)

Amazon product reviews dataset.

| File | Size |
|------|------|
| `1429_1.csv` | 47 MB |
| `Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products.csv` | 95 MB |
| `Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products_May19.csv` | 253 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/consumer_reviews_amazon_products/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/consumer_reviews_amazon_products/1429_1.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/consumer_reviews_amazon_products/1429_1.csv", header=True, inferSchema=True)
display(df)
```

---

## FIFA

FIFA player data (2015-2022) for male and female players.

| File | Size |
|------|------|
| `players_15.csv` - `players_22.csv` | 10-13 MB each |
| `female_players_16.csv` - `female_players_22.csv` | 140-237 KB each |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/fifa/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/fifa/players_22.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/fifa/players_22.csv", header=True, inferSchema=True)
display(df)
```

---

## Global Terrorism Database

Terrorism incidents from 1970 onwards with 135 columns of detailed information.

| File | Size |
|------|------|
| `globalterrorismdb_0718dist.csv` | 155 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/global_terrorism_database/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/global_terrorism_database/globalterrorismdb_0718dist.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/global_terrorism_database/globalterrorismdb_0718dist.csv", header=True, inferSchema=True)
display(df)
```

---

## Goodreads Books

Book metadata including ratings, authors, and publication info (11K+ books).

| File | Size |
|------|------|
| `books.csv` | 1.5 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/goodreads_books/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/goodreads_books/books.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/goodreads_books/books.csv", header=True, inferSchema=True)
display(df)
```

---

## Powerlifting

OpenPowerlifting competition data with millions of records.

| File | Size |
|------|------|
| `openpowerlifting.csv` | 239 MB |
| `openpowerlifting-2024-01-06-4c732975.csv` | 573 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/powerlifting/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/powerlifting/openpowerlifting.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/powerlifting/openpowerlifting.csv", header=True, inferSchema=True)
display(df)
```

---

## Rotten Tomatoes Movies & Reviews

Movie metadata and critic reviews.

| File | Size |
|------|------|
| `rotten_tomatoes_movies.csv` | 16 MB |
| `rotten_tomatoes_critic_reviews.csv` | 216 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/rotten_tomatoes_movies_reviews/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/rotten_tomatoes_movies_reviews/rotten_tomatoes_movies.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/rotten_tomatoes_movies_reviews/rotten_tomatoes_movies.csv", header=True, inferSchema=True)
display(df)
```

---

## San Francisco Building Permits

Building permit records (199K+ permits, 43 columns).

| File | Size |
|------|------|
| `Building_Permits.csv` | 75 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/san_francisco_building_permits/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/san_francisco_building_permits/Building_Permits.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/san_francisco_building_permits/Building_Permits.csv", header=True, inferSchema=True)
display(df)
```

---

## Used Cars

Used car sales data with pricing, condition, and vehicle details.

| File | Size |
|------|------|
| `car_prices.csv` | 84 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/used_cars/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/used_cars/car_prices.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/used_cars/car_prices.csv", header=True, inferSchema=True)
display(df)
```

---

## UK Traffic Accidents

UK road accident data (2005-2014) and traffic flow statistics.

| File | Size |
|------|------|
| `accidents_2005_to_2007.csv` | 156 MB |
| `accidents_2009_to_2011.csv` | 129 MB |
| `accidents_2012_to_2014.csv` | 127 MB |
| `ukTrafficAADF.csv` | 53 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/uk_traffic_accidents/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/uk_traffic_accidents/accidents_2012_to_2014.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/uk_traffic_accidents/accidents_2012_to_2014.csv", header=True, inferSchema=True)
display(df)
```

---

## US Election 2020 Tweets

Twitter data from the 2020 US Presidential Election.

| File | Size |
|------|------|
| `hashtag_donaldtrump.csv` | 461 MB |
| `hashtag_joebiden.csv` | 363 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/us_election_2020_tweets/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/us_election_2020_tweets/hashtag_donaldtrump.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/us_election_2020_tweets/hashtag_donaldtrump.csv", header=True, inferSchema=True)
display(df)
```

---

## World Cities

Global cities database with population and coordinates (3.2M+ cities).

| File | Size |
|------|------|
| `worldcitiespop.csv` | 157 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/world_cities/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/world_cities/worldcitiespop.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/world_cities/worldcitiespop.csv", header=True, inferSchema=True)
display(df)
```

---

## Vehicle Sales Data

Car sales data with pricing and vehicle specifications.

| File | Size |
|------|------|
| `car_prices.csv` | 84 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/vehicle_sales_data/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/vehicle_sales_data/car_prices.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/vehicle_sales_data/car_prices.csv", header=True, inferSchema=True)
display(df)
```

---

## The Movie Database (TMDb)

Movie metadata and credits from TMDb (5000 movies).

| File | Size |
|------|------|
| `tmdb_5000_movies.csv` | 5.4 MB |
| `tmdb_5000_credits.csv` | 38 MB |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/the_movie_database/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/the_movie_database/tmdb_5000_movies.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/the_movie_database/tmdb_5000_movies.csv", header=True, inferSchema=True)
display(df)
```

---

## Superstore Sales

Retail sales data with orders, returns, and regional managers.

| File | Size |
|------|------|
| `Superstore_Orders.csv` | 2.3 MB |
| `Superstore_Returns.csv` | 5.5 KB |
| `Superstore_People.csv` | 97 B |

**Cell 1 - List folder:**
```
%fs ls /Volumes/dbx_course/source/files/assignment/superstore_sales/
```

**Cell 2 - Preview the CSV:**
```
%fs head /Volumes/dbx_course/source/files/assignment/superstore_sales/Superstore_Orders.csv
```

**Cell 3 - Load CSV:**
```python
df = spark.read.csv("/Volumes/dbx_course/source/files/assignment/superstore_sales/Superstore_Orders.csv", header=True, inferSchema=True)
display(df)
```
