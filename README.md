# Project: Data Lake

## Purpose of the Project

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

## Prerequisites

Before running dag, tables must be created on Redshift with using this file:

create_tables.sql


## Data Sources
Data uploaded from  two directories that contain files in JSON format:

<ol>
<li>Log data: s3://udacity-dend/log_data
<li> Song data: s3://udacity-dend/song_data
</ol>

## Data Quality

To check data quality, total number of count is controlled. If a table has no rows, error message is created.


## Scripts

<ol>
<li>create_tables : sql for creating table on redshift datawarehosue
<li>udac_example_dag.py : dag information for loading staging, fact and dimensions tables.
<li>stage_redshift.py : loading data from JSON files on S3 to Redshift staging tables
<li>load_fact.py : loading fact tableson Redshift from staging table
<li>load_dimension.py : loading dimensions table from staging and fact tables
<li>data_quality.py : checking row counts of table to ensure data quality
</ol>
