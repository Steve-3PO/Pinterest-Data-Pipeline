# Pinterest Data Pipeline (Data and Cloud Engineering)

[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)

## Overview 
> - Pinterest crunches billions of data points every day to decide how to provide more value to their users.
> - The goal of this project will be to simulate the pipeline implemented by Pinterest with both Batch and Stream Processing which will involve a host of various AWS software platforms.

## Learning Objectives
> - To implement a data pipeline using AWS.
> - To process Batch and Stream data.
> - To conduct cleaning and analysis on the processed data.

## Technologies Used
> - Kafka: Used as a scalable and fault-tolerant messaging system for data streaming.
> - AWS Managed Streaming for Apache Kafka (MSK): Managed service for Apache Kafka, providing easy setup, monitoring, and scaling capabilities.
> - MSK Connect: Enables seamless data integration between Apache Kafka and other data sources and sinks.
> - AWS API Gateway: Used to create, publish, and manage APIs, allowing seamless interaction with the pipeline.
> - AWS S3: Scalable object storage used for data storage and retrieval.
> - Spark: Fast and distributed processing engine used for data transformations and analytics.
> - Spark Structured Streaming: Enables real-time processing and analysis of streaming data.
> - Databricks: Cloud-based platform for collaborative data engineering and analytics.
> - Airflow: Open-source platform for orchestrating and scheduling workflows.
> - AWS Managed Workflows for Apache Airflow (MWAA): Fully managed service for Apache Airflow, simplifying the deployment and management of workflows.
> - AWS Kinesis: Managed service for real-time data streaming and processing.

## Project Highlights
> - Developed a robust data processing pipeline capable of handling Pinterest's experimental data requirements.
> - Implemented a Lambda architecture, combining batch processing and stream processing methods for efficient data analysis.
> - Leveraged Kafka and MSK to handle scalable and fault-tolerant data streaming.
> - Utilized Spark and Spark Structured Streaming for real-time data processing and analytics.
> - Integrated various data sources and sinks using MSK Connect for seamless data integration.
> - Ensured data storage and retrieval using AWS S3.
> - Orchestrated and scheduled workflows using Airflow and MWAA for efficient pipeline management.

## Project Structure

### Milestone 1 - "Batch Processing: Configure the EC2 Kafka client."
> - Create .pem key file and connect to EC2 Instance
> - Set up Kafka and topics from EC2 instance

### Milestone 2 - "Batch Processing: Connect a MSK cluster to a S3 Bucket."
> - Create a custom plugin, MSK Connect
> - Create a connector, MSK Connect

### Milestone 3 - "Batch Processing: Configuring an API in API Gateway."
> - Build a Kafka REST proxy intergration method for the API
> - Set up Kafka REST proxy on EC2 instance
> - Send data to the API

### Milestone 4 - "Batch Processing: Databricks."
> - Set up Databricks
> - Mount an S3 bucket

### Milestone 5 - "Batch Processing: Spark on Databricks."
> - Clean the Dataframes
> - Querying the Dataframes with SQL and Pyspark

### Milestone 6 - "Batch Processing: AWS MWAA."
> - Create and upload DAG to MWAA
> - Trigger DAG

### Milestone 7 - "Stream Processing: AWS Kinesis."
> - Create Data Stream with Kinesis
> - Configure an API with Kinesis Proxy Integration
> - Send data to Kinesis and read to Databricks
> - Transform Kinesis stream in Databricks and Write to Delta Tables


<!-- ## M1 - Batch Processing: Configure the EC2 Kafka client.

### Creating .pem file

### Connecting to EC2 Instance

### Setting up Kafka on EC2 Instance

### Creating Kafka Topics


## M2 - Batch Processing: Connect a MSK cluster to a S3 Bucket.

### Creating custom plugin, MSK Connect

### Creating connector, MSK Connect


## M3 - Batch Processing: Configuring an API in API Gateway.

### Building a Kafka REST proxy integration method for the API

### Setting up the Kafka REST proxy on EC2 Instance 

### Sending Data to API


## M4 - Batch Processing: Databricks.

### Setting up DataBricks

### Mounting S3 Bucket 


## M5 - Batch Processing: Spark on Databricks.

### Cleaning the Pinterest Posts Dataframe

### Cleaning the Geolocation Dataframe

### Cleaning the User Dataframe

### Task 1: Find the most popular category in each country.

### Task 2: Find which was the most popular category each year.

### Task 3: Find the user with more followers in each country.

### Task 4: Find the most popular category for different age groups.

### Task 5: Find the median follower count for different age groups.

### Task 6: Find jow many users have joined each year.

### Task 7: Find the median follower count of users based on their joining year. 

### Task 8: Find the median follower count of users band on their joining year and age group.


## M6 - Batch Processing: AWS MWAA.

### Creating and uploading DAG to MWAA environment

### Triggering the DAG to run Databricks Notebook


## M7 - Stream Processing: AWS Kinesis.

### Creating data streams with Kinesis

### Configuring an API with Kinesis proxy integration

### Sending data to Kinesis streams

### Reading data from Kinesis stream in Databricks

### Transform Kinesis streams in Databricks

### Writing the streaming data to Delta Tables -->