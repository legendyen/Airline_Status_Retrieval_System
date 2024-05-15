# Airline_Status_Retrieval_System
In this Project, we designed and implemented a comprehensive ETL pipeline that automates the extraction of weather and flight data from various APIs. The core of the pipeline involves a Python script that handles data ingestion directly into Google Cloud Platform Cloud Storage. This pipeline aims on the automantion of provide a data-driven decision-making dashborad.

## Key Components and Workflow
- **Data Ingestion**: We debeloped Python scripts to automate data extraction from multiple APIs.
- **Batch Processing**: We implemented two ETL pipelines by deploying two PySpark script to transform on Google Cloud Dataproc. In addition, we used Google Cloud Scheduler and Workflow template to trigger the transformation process on Dataproc every three hours. This setup ensures data stays up-to-date.
- **Data Integration**: The transformed data is then loaded into Google BigQuery. This integration allow access to the data through BigQuery.

## Technologies Used
- **Google Cloud Platform**: Leveraged for cloud storage and managing the data lifecycle in a scalable environment.
- **Google Cloud Dataproc**: Utilized for orchestrating batch processing tasks, significantly reducing the complexity of deploying and managing Spark jobs.
- **PySpark**: Chosen for its powerful data processing capabilities, allowing for efficient manipulation of large data sets.
- **BigQuery**: Used as the data warehousing solution to facilitate quick access to processed data and enable advanced data analytics.
