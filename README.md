# Airbnb Data Analysis (Using Snowflake with Airflow)

![1687340633833](https://github.com/Shiv0989/Airbnb-Data-Analysis/assets/83575973/3d68d790-f5ee-4ff9-858a-27cdd6d92b68)


## Overview
This project involves analyzing Airbnb data along with census and NSW LGA datasets to extract valuable insights and answer business-related questions using ELT data pipelines with Airflow and Snowflake.

## Data Collection and Description
### Airbnb Listing Data
- Contains information about hosts, properties, and reviews for specific months in 2020 and 2021.

### Census Data
- Population and housing data collected by the Australian Bureau of Statistics, detailing various demographic and economic parameters.

### NSW LGA Data
- Includes datasets mapping LGA codes to names and suburbs.

## Data Warehouse Design
Implemented a four-layer architecture: Raw, Staging, Warehouse, and Datamart.

### Raw Layer
- Uploaded data to Google Cloud Storage, connected with Snowflake, and created external tables.

### Staging Layer
- Created dimension tables (e.g., host, review, listing) and a fact table to establish a star schema.

### Warehouse Layer
- Joined dimension tables with the fact table to form a comprehensive star schema.

### Datamart Layer
- Designed tables to answer business questions, providing insights into listing performance, property types, and host behaviors.

## Data Processing and Integration
### Airflow DAG
- Set up an Airflow Directed Acyclic Graph (DAG) to automate the ELT process, loading data into the defined star schema.

### Fact and Dimension Tables
- Created a staging table with all listing data, dimension tables for various data categories, and a central fact table.

## Business Questions and Insights
1. **Population Analysis:** Analyzed population demographics to understand differences between the best and worst performing neighborhoods in terms of estimated revenue.
2. **Optimal Listings:** Identified the best property types and room types for top-performing neighborhoods to maximize stays.
3. **Host Location Preference:** Determined that hosts prefer to live in different LGAs than their listings.
4. **Revenue Analysis:** Found that most hosts with unique listings cannot cover the annualized median mortgage repayment, suggesting the need for multiple listings to enhance revenue potential.

## Ad-Hoc Analysis
- **Revenue per Active Listing:** Analyzed trends in estimated revenue per active listing, considering factors like listing neighborhood and property type.
- **Host Behavior:** Investigated the impact of host location on listing performance and revenue generation.

## Conclusion
This project successfully combined various datasets to build a robust data warehouse and perform in-depth analysis. The insights gained can help stakeholders make informed decisions regarding property management, pricing strategies, and market targeting. The use of Snowflake and Airflow for data processing and automation ensures efficient handling of large datasets and streamlined analytical workflows.

## How to Run
1. **Set up Google Cloud Storage:** Upload the datasets to your GCP bucket.
2. **Configure Snowflake:** Set up your Snowflake account and create the necessary databases and tables.
3. **Airflow Configuration:** Deploy the provided Airflow DAG to automate the ELT process.
4. **Run the ETL Process:** Execute the DAG to load data into Snowflake and create the star schema.
5. **Perform Analysis:** Use the provided notebooks and scripts to perform ad-hoc analysis and answer business questions.

## Dependencies
- Google Cloud Storage
- Snowflake
- Apache Airflow
- Python (with relevant libraries like pandas, etc.)
