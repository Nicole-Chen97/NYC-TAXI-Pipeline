# 🚕 NYC Taxi Data Pipeline

## Overview :
  - who : This project build a clean end-to-end pipeline to provide NYC taxi …… for …. operation team
  - what :
 
---
## Data:
  - Dataset Overview :
    - This project uses the publicly available trip record data provided by [New York City Taxi and Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
    - The dataset contains **trip-level transactional records generated** by taxi operations across New York City and is released on a **monthly** basis.
    - Each trip record includes information such as:
      - Pickup and drop-off timestamps
      - Pickup and drop-off locations
      - Trip distance
      - Fare amount
      - Payment type
      - Passenger count
      - Service type
  - Taxi Service Types :
    - New York City operates multiple taxi services. This project focuses on:
    - Yellow Taxi :
      - Yellow taxis can pick up passengers throughout most areas of New York City's boroughs and are commonly used in:
        - Manhattan business districts
        - Tourist destinations
        - Airports
        - High-demand urban areas
      Yellow taxis represent the largest portion of taxi trips in New York City. 
      
    - Green Taxi :
      - Green taxi were introduced to improve transportation access in areas traditionally underserved by yellow taxis. They are primarily allowed to pick up passengers in :
        - Upper Manhattan
        - The Bronx
        - Brooklyn
        - Staten Island
      Green taxis are more frequently used by local residents traveling within outer borough neighborhoods.
    Both yellow and green taxis are regulated by the TLC and follow similar metered fare structures.
      
  - Dataset Characteristics :
    - The TLC data is published in **Parquet format**, making it suitable for large-scale analytical processing.
    - Volume  : Yellow taxi trips typically contain significantly higher trip volumes and larger file sizes.

  - Data Quality Considerations :
    Several data quality issues should be considered when working with TLC data:
    - Different taxi service types may contain different schemas.
    - Data availability varies by month and service type.
    - Some records contain missing or invalid values that require cleaning.

  - Summary :
    - Due to the **monthly release schedule** of the TLC dataset and the analytical nature of the business requirements, this project implements a **monthly batch-processing pipeline** that ingests NYC TLC trip records, standardizes data across multiple taxi service types, and transforms raw data into **analytics-ready datasets** for downstream reporting and business intelligence.

---

## System Architecture :
picture 

---

## - Data Pipeline :
- The data pipeline is designed as a monthly batch workflow. It processes NYC TLC trip records from raw data ingestion to analytics-ready tables.
- Pipeline Steps :
  1. Data  Ingestion : 
    - Download raw data in PARQUET format. 
    - Upload raw file to Google Cloud Storage , which is serves as the data lake.
  2. Data Processing : 
    - Read raw Parquet files from GCS using **Apache Spark**.
    - Perform basic data cleaning and schema standardization.
    - Partition the processed data by **year** and **month** for efficient storage and querying.
  3. External Table Creation : 
    - Create **BigQuery external tables** on top of the processed Parquet files stored in GCS.
    - Enable **Hive-style partitioning** to improve query performance.
  4. Data Modeling 
    - Use **dbt** to transform external tables into analytics-ready data models.
    - Build **staging**, **intermediate**, and **mart** layers following a layered modeling approach.
  5. Analytics Output : 
    - Generate fact and reporting tables for downstream analytics, business intelligence, and reporting.

## Data Storage : 
This project adopts a layered storage architecture consisting of a Data Lake, BigQuery External Tables, and a Data Warehouse. Each layer serves a different purpose in the data pipeline.
  - Data Lake
    - A data lake is used to store large volumes of raw and unprocessed data  before being transformed for analytical use.
    - compared with traditional databases, a data lake provides :
      - Cost- effective storage for large datasets
      - Schema-on-read flexibility
      - Long-term storage of historical data
      - the ability to reprocess data when business requirements change
            
    In this project, Google Cloud Storage (GCS) is used as the Data Lake because it provides scalable object storage and integrates seamlessly with other Google Cloud services such as BigQuery.

    - Processed taxi trip records are stored in PARQUET format and partitioned by :
      - Year
      - Month
      This partitioning strategy reduces the amount of data scanned during queries, resulting in improved query performance and lower query costs.
                
      To utilize the partitioned strategy, Spark is used to performed data cleaning and schema standardization and then wriote partitioned Parquet files back to GCS.
      the benefits include: 
        - Parallel data processing
        - Parallel file writing
        - Reduced memory pressure through distributed execution
        - Faster data retrieval from partitioned storage
      Using Spark enables efficient distributed processing and scalable handling of large datasets.
     
                
  - BigQuery External Table :
    
    BigQuery External Tables allow BigQuery to query data stored directly in GCS without importing the files into the data warehouse.
    
    In this project, external tables are created directly on top of processed Parquet files stored in Google Cloud Storage. 
    
    Benefits include : 
    
    - Eliminates duplicate data storage
    - Reduce storage cost
    - Allows immediate access to file stored in the Data Lake
    - Simplifies data ingestion workflows
    
    This layer acts as the bridge between the Data Lake and the analytics layer.
  
  - Data Warehouse : 

    After data transformation with dbt, the curated datasets are materialized in BigQuery, which serves as the project's data warehouse.

    Unlike a Data Lake, the Data Warehouse stores structured, analytics-ready data optimized for Online Analytical Processing (OLAP).

    The warehouse supports downstream use cases such as:
    - Business reporting
    - Dashboarding
    - Data analytics
    - Business intelligence
    
    Because BigQuery is a fully managed, serverless data warehouse, it provides:
    - High-performance analytical queries
    - Automatic scalability
    - No infrastructure management
    - Pay-as-you-query pricing



## Data Modeling :
  This project use **dbt** to implement a **Medallion Architecture** , organizing data into Staging , Intermediate, and Mart layers.

  The Layered design improves maintainability , data quality , and model reusability while providing a clear  separation of responsibilities across the transformation process.

  DBT is used to be as the transformation framework because it provides
    - SQL-based data transformations
    - Dependency management through model references
    - Bulit-in data quality testing
    - Data lineage visualization
  
  - Stagging :
    
    the staging layer standardizes raw taxi data from external tables . 
    
    key responsibilities in this project : 
    - Standardizing column names and data types
    - Creating consistent schemas across taxi service types
    - Preparing source data for downstream transformations  
    
    Models in this layer are materialized as **views** to **minimize storage cost and keep transformations lightweight**.
    
  - Intermediate Layer
    - the intermediate layer integrates and enriches data from multiple taxi service types .
      
      Key responsibilities:
        - Merge Yellow Taxi and Green Taxi datasets into a unified schema
        - Generating surrogate keys
        - Remove duplicate records
        - Enrich trip records with with additional business attributes
    This layer produces reusable business entities that serve as the foundation for downstream analytical models.
    
  
  - Mart Layer 
    the mart layer provides analytics-ready datasets optimized for reporting and business analysis.
    
    """The mart layer is materialized as incremental tables because the TLC dataset is released monthly, allowing only new monthly partitions to be processed instead of rebuilding the entire dataset."""

    Key responsibilities:
    - Build fact and dimension models
    - Support incremental loading
    - Enrich trip data with location dimensions
    - Optimize analytical query performance
    - Providing datasets for downstream reporting and dashboarding

  ### Materialization Strategy

    Different materialization strategies are used across the project to balance query performance, storage cost, and maintainability.

    | Layer        | Materialization   | Reason                        |
    | ------------ | ----------------- | ----------------------------- |
    | Staging      | View              | Reduce storage cost           |
    | Intermediate | View              | Reusable transformation layer |
    | Mart         | Incremental Table | Efficient monthly updates     |
    | ------------ | ----------------- | ----------------------------- |


  ### Reporting ? 
  Business-oriented reporting tables are created from mart models to support analytical use cases such as:
  - Airport fee analysis
  - Borough-level transportation analysis
  - Monthly taxi activity summaries
  - Revenue and trip volume reporting

----

## Workflow Design:
  Apache Airflow is used to orchestrate the end-to-end data pipeline.
  The orchestration layer is responsible for coordinating workflow execution across different systems, including Google Cloud Storage, Spark, BigQuery, and dbt.
    

### Key Features

- **Monthly Scheduling**
  - The NYC TLC dataset is published on a **monthly basis**. Therefore, the pipeline is scheduled to run once per month, ensuring that processing begins only after new data becomes available. This approach avoids unnecessary executions while aligning the workflow with the data release cycle.
- **Task Dependencies**
  The pipeline consists of multiple sequential stages, including data ingestion, processing, storage, and transformation. Task dependencies ensure that each stage starts only after its upstream tasks have completed successfully, preventing incomplete or inconsistent data from propagating through the pipeline.

- **Parameterized Execution**
  The pipeline supports parameterized execution by allowing users to specify one or more taxi service types (e.g., Yellow Taxi or Green Taxi) when triggering the workflow.

  This design eliminates duplicated DAG logic and enables a single workflow to process multiple datasets using the same processing pipeline.

- **Dynamic Task Mapping**
  Based on the selected taxi service types, Airflow dynamically generates processing tasks at runtime.
  This allows the workflow to scale naturally as additional taxi services or datasets are introduced without modifying the DAG structure.


- **Selective Backfill**
  Historical data can be reprocessed selectively for specific taxi service types and time periods.
  Instead of rerunning the entire pipeline, users can backfill only the affected datasets (for example, May Yellow Taxi), reducing compute costs, shortening recovery time, and making error correction more efficient.


----


## Challenges


### Schema Standardization

Yellow Taxi and Green Taxi datasets contain slightly different schemas, requiring schema alignment before downstream transformations.



### Storage and Query Separation

Instead of loading raw data directly into BigQuery, the pipeline uses GCS together with BigQuery External Tables to separate storage from computation while reducing storage duplication.

---

### Incremental Data Processing

Since the dataset is released monthly, rebuilding the entire warehouse for every execution would be inefficient.

Incremental models are therefore used in the mart layer to process only newly released partitions.

---

### Flexible Workflow Design

Supporting multiple taxi service types introduced workflow complexity.

Parameterized execution together with Dynamic Task Mapping enables a single DAG to process different datasets while supporting selective backfill.




## References

This project was inspired by the dbt module (Chapter 4) from the [2026 Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

Building upon the core ideas from the course, I extended the original implementation by designing and implementing a more complete cloud-based data pipeline, including:

Starting from the data modeling concepts introduced in the course, I extended the project into a complete end-to-end data pipeline by implementing:
- Data ingestion from the NYC TLC dataset
- Google Cloud Storage (GCS) as the data lake
- Apache Spark for data preprocessing and partitioning
- BigQuery External Tables for querying data stored in GCS
- Apache Airflow for workflow orchestration, scheduling, and backfill
- dbt for layered data modeling using the Medallion Architecture

While the dbt modeling approach was inspired by the course, the overall pipeline architecture, storage design, orchestration workflow, and implementation were independently designed and developed.