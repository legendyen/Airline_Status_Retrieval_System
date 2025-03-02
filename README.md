# Airline Status Retrieval System

## Overview
The **Airline Status Retrieval System** is a microservices-based API processing system designed for real-time flight and weather data retrieval. It ensures **99.9% flight status accuracy** by implementing **time interval mapping** across 600K+ records. The system enables seamless integration with external flight tracking services, providing valuable insights into flight delays due to weather conditions.

## Key Features
- **Real-Time Data Ingestion**: Developed Python scripts to extract flight and weather data from multiple APIs.
- **Batch Processing with PySpark**: Deployed ETL pipelines on **Google Cloud Dataproc**, scheduled to run every **three hours** using **Google Cloud Scheduler and Workflow Templates**.
- **Database Integration**: Processed data is stored in **Google BigQuery** for **advanced analytics and visualization**.
- **API Testing & Debugging**: Ensured seamless integration with external flight tracking systems through rigorous API testing.
- **Delay Pattern Analysis**: Implemented **linear regression (RMSE 42%)** to identify weather-based flight delay trends, revealing that **cloudy conditions cause 33% longer delays**.
- **Dashboard Integration**: Designed an interactive **Google Looker Studio dashboard** for real-time performance monitoring.

## Technologies Used
- **Programming Languages**: Python, SQL
- **Cloud Services**: Google Cloud Platform (GCP), Cloud Storage, BigQuery, Dataproc, Cloud Scheduler
- **Data Processing**: PySpark, REST APIs
- **System Monitoring**: Looker Studio for real-time dashboard visualization
- **Containerization**: Docker for deployment

## System Architecture
1. **Data Ingestion**
   - Flight and weather data extracted via REST APIs
   - Stored in **Google Cloud Storage**
2. **Batch Processing & Transformation**
   - **PySpark jobs** on Google Cloud Dataproc transform raw data
   - Scheduled execution every **three hours** using **Cloud Scheduler**
3. **Data Warehousing**
   - Transformed data loaded into **BigQuery** for querying
4. **Analytics & Dashboarding**
   - Developed **Google Looker Studio** reports to visualize **flight status and delay patterns**

## Deployment
1. **Clone the repository**
   ```bash
   git clone https://github.com/your-repo/Airline_Status_Retrieval_System.git
   cd Airline_Status_Retrieval_System
   ```
2. **Set up a virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
4. **Run ETL pipeline**
   ```bash
   python etl_pipeline.py
   ```

## Dashboard Outcome
[Flight Delay Prediction Dashboard](https://lookerstudio.google.com/u/0/reporting/59796ec9-a2b8-449a-b114-d5e6b3e7371f/page/tEnnC) provides real-time insights into:
- Flight delay trends across different route and airline company
![image](https://github.com/user-attachments/assets/7526965f-7dca-46b1-a597-3fa4c086f5fc)

- Average wait times by weather type
![image](https://github.com/user-attachments/assets/855ce473-ce10-4246-b6a4-13fa2d81400a)

- Predictive analytics for delay time based on weather and flight metrics
![image](https://github.com/user-attachments/assets/ddb70f5a-3a26-4be4-9c78-0d0635a40ae0)


## Future Enhancements
- **Integration with Machine Learning Models**: Predictive analytics using advanced ML techniques (e.g., XGBoost)
- **Enhancing System Scalability**: Implementing Kubernetes for better container orchestration
- **Improved Data Enrichment**: Adding real-time air traffic control data for enhanced forecasting

## Contributors
- **Leo (Sung-Jen)** - Lead Developer & Data Engineer
- **Claire** - API Development & Dashboarding

---
This project was developed to **enhance real-time flight tracking and delay prediction accuracy**, leveraging **cloud-based automation and AI-driven insights**.
