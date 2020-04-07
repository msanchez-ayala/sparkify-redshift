# Data Warehousing in Amazon Redshift

Fictional music streaming startup Sparkify has been growing significantly and
wants to move their data and processes to the cloud. The data is initially
stored in Amazon S3 in JSON logs of user activity and song metadata.

I create the following to help them out:

- An ETL pipeline to extract data from S3, stage it in Redshift, and transform
it into dimensional tables for the analytics team to continue gathering insights
into what music their customers listen to.

## Database Schema

![](images/erd.jpeg?raw=true)

We have two staging tables that capture all of the data from S3. That
information is then transformed and loaded into the fact and dimension tables
shown at right.

## Repo Organization: How to Use
Assuming you have an active Redshift cluster, you can reproduce this project
by doing the following:

1. Create a virtual environment with python=3.7.4. Download the packages in
**requirements.txt**

2. Run **create_tables.py.** This module connects to the Redshift cluster, drops
any existing tables, and creates all 7 tables shown above.

  Note: You will need to set up your own config file. I reference one called **dwh.cfg** in all three of the modules discussed below.

3. Run **etl.py.** This module connects to the Redshift cluster database, copies
log_data and song_data from S3 into staging tables, and finally transforms/loads
that data into the five fact and dimension tables above.

The last module, **sql_queries.py** contains all of the SQL queries used for
both read and write queries called from **create_tables.py** and **etl.py**.


## How-to

## Sample Queries
Most common user locations

```
SELECT
  location,
  COUNT(location)
FROM
  songplays
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 10
```

Breakdown of user demographic

```
SELECT
  gender,
  COUNT(gender)
FROM
  users
GROUP BY
  1
 ```

Most common hour of the day to stream music

```
SELECT
  hour,
  COUNT(hour)
FROM
  time
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 5
```
