# Project: Data Warehouse
This is an ETL, 'Extract, Transform, and Load', pipeline for the company Sparkify that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


## Table of contents
* [Significance](#Significance)
* [Files](#files)
* [Prerequisites](#prerequisites)
* [Launch](#launch)

## Significance
This move from legacy JSON files to the Redshift and the cloud will give the analytics team at Sparkify greatly increase the insights in what songs their users are listening to, and at the same time probably reduce the cost of operation.


## Files
The files used to make the etl, , and the warehouse are:
* dwh.cfg
	* This file stores the credentials to create the cluster, endpoint/host, the iam role, and the path to the json files where the old data is stored.
* create_tables.py
	* This file automates the execution of the create queries imported from the sql_queries file.
* sql_queries.py
	* This file contains all the queries needed to CREATE the staging tables and the fact and dimension tables, as well as the COPY and INSERT queries.
* etl.py
	* This file automates the copying and inserting into the fact and dimension tables.


## Prerequisites
* psycopg2
* Redshift
* AWS IAM User
* AWS IAM Role
* AWS Cluster


## Launch
To run this project you need to check that the dwh.cfg file contains an functioning IAM role and user endpoint and put the endpoint in HOST and the IAM ROLE in the ARN.

Run the create_tables.py file in terminal to create all the tables needed, and then the etl.py file to load and insert into the new fact table and dimension tables. 
