# Data Warehouse
The data is residing in S3 and it's from the app `Sparkify`, a startup. From analyzing the data, they want to see what the users' interests are. This can lead to strengthening their position in the music app field by becoming a more user-friendly app.

Data warehouse with AWS `Redshift` and `IAM role` is applied in this project in order to insert the data into the dimensional tables.
## Database Design
![Dimensional Tables](./Image/schema_dwh.jpg)

As shown in the above diagram, the fact table is `songplays`, and the remaining tables are fact tables.
### Primary key in each table
Column Name | Data Type | Dist key / Sort key
----------- | --------- | -------------------
songplay_id | INT IDENTITY(0,1) | Sort key
user_id | INT | Dist key & Sort key
song_id | VARCHAR | Sort key
artist_id | VARCHAR | Sort key
start_time | TIMESTAMP | Sort key

The `distkey` and the `sortkey` are assigned like the above table.
These tables are first created along with the staging tables in the `create_tables.py` file.

## ETL
The staging tables in S3 is first collected by using the `COPY` command in the `etl.py` file. The required information like __IAM role ARN__ or the S3 path of the data is written in the `dwh.cfg` file.

In order to put those necessary information, creating the Redshift cluter and IAM role with the right IAM permission is done before running the `etl.py` file.

After that, `INSERT` and `SELECT` commands are needed to insert the data from staging tables into the dimensional tables.