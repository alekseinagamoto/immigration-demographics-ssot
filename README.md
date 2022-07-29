# Immmigration Demographics Single-Source-Of_Truth

- [About](#about)
- [Data Warehouse Design Design](#data-warehouse-design)
- [ETL Pipeline Design](#etl-pipeline-design)
- [Getting Started](#getting-started)
- [Directory Strucure](#directory-structure)
- [References](#references)

## About

A startup called Sparkify has been growing their user base and wants to move their process and data onto the cloud to continue to be able to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently the data resides in S3 and it is required to create a data warehouse (AWS Redshift) and an ETL pipeline that extracts Sparkify's data from S3, stages the data before transforming the data into a set of dimensional tables in the data warehouse.

## Data Warehouse Design

### Data

The data resides in S3, in a directory of JSON logs on user activity (```s3://udacity-dend/log_data
```) in the app as well as a directory with JSON metadata on the songs (```s3://udacity-dend/song_data```) in their app:


### Database Schema

Using the user activity and song metadata, two staging tables and a star schema optimized for queries on song play analysis are created. This includes the following tables:


- Staging tables: **events_staging**, **songs_staging**
- Fact table: **songplays**.
- Dimension tables: **songs**, **artists**, **users**, **time**. 


## ETL Pipeline Design

The ETL pipline comprises the following components:

- ETL of user activity and song metadata into staging tables events_staging and songs_staging. 
- ETL of song data into songs table from songs_staging table.
- ETL of artist data into artists table from songs_staging table.
- ETL of time data into time table from events_staging.
- ETL of user data into users table from events_staging table.
- ETL of songplay data into songplays table from events_staging and songs_staging tables.

## Getting Started

### Tech Stack

- Docker
- Jupyter Notebook
- Boto3
- AWS S3
- AWS Redshift

### Build the containers

Ensure docker is installed.

```cmd

docker compose build

```

### Start the container

```cmd

docker compose up

```

Docker compose will spin up one container:

- jupyter (jupyter notebook container)

### Create Redshift Data Warehouse on AWS

Run the ```IaC.ipynb``` notebook. The notebook will walk you through the steps to create a Redshift cluster.

### Create the Data Warehouse

You can create the Data Warehouse tables by running the appropriate cells in the ```IaC.ipynb``` notebook after creation of the Redshift cluster.

Alternative, after creation of the Redshift cluster, you can run ```create_tables.py```:

```cmd

docker-compose run --rm jupyter_notebook python src/scripts/create_tables.py

```

### Execute ETL Pipeline

You can run the ETL pipeline by running the appropriate cells in the ```IaC.ipynb``` notebook after creation of the Redshift cluster and data warehouse tables.

Alternatively, after creation of the Redshift cluster and running ```create_tables.py```, you can run ```etl.py```:

```cmd

docker compose run --rm jupyter_notebook python src/scripts/etl.py

```

Alternatively, you can run the python scripts from a shell in the container:

```cmd

docker exec -it jupyter bash 

```

## Directory Structure

```
├── services
|    └── jupyter
|        └── Dockerfile         <- Docker file for jupter notebook.
|
|
├── src                         <- Source code for use in this project.
|   ├── notebooks           
|   |   └── IaC.ipynb           <- Noteook for Infrastructure-as-Code deployment and disposal of infrastructure. 
|   |         
|   └── scripts  
|       ├── etl.py              <- Script to execute ETL pipeline.
|       ├── create_tables.py    <- Script to create database and tables.
|       └── sql_queries.py      <- Script with SQL queries.
|      
|
├── docker-compose.yml          <- Docker-compose file for running the services.
|
|
├── README.md                   <- README for developers using this project.
├── poetry.lock                 <- The poetry.lock file for reproducing the analysis environment.
└── pyproject.toml              <- The pyproject.toml file for reproducing the analysis environment.
```

## References

This project is part of the [Udacity Data Engineering nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

- [Docker documentation](https://docs.docker.com/)
- [Jupyter notebook image](https://hub.docker.com/r/jupyter/minimal-notebook/tags/)
- [Markdown guide](https://www.markdownguide.org/basic-syntax/)
- [Poetry documentation](https://python-poetry.org/docs/)
- [Amazon Redshift documentation](https://aws.amazon.com/redshift/)
- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)