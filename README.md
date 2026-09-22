# **End-to-End Logistics Data Engineering Platform**

## 📌 Project Overview

This project implements an end-to-end cloud data engineering platform for a logistics business.

The platform ingests batch logistics data using **Azure Data Factory**, stores data in **Azure Data Lake Storage Gen2**, processes and transforms the data using **Azure Databricks and PySpark**, and implements a **Medallion Architecture** consisting of Bronze, Silver, and Gold layers.

The project also implements a real-time streaming pipeline using **Azure Event Hubs** and **Databricks Structured Streaming** for shipment tracking events.

The final Gold layer follows a **Star Schema** and provides business-ready SQL views for logistics analysis.

---

## 🏗️ Architecture

```text
                    ┌─────────────────────────┐
                    │       Source Data       │
                    │                         │
                    │ CSV / JSON Logistics    │
                    │ Data                    │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │    Azure Data Factory   │
                    │                         │
                    │ Dynamic Metadata-Based  │
                    │ Batch Ingestion         │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │      ADLS Gen2          │
                    │                         │
                    │ Raw → Bronze Storage    │
                    └────────────┬────────────┘
                                 │
                                 ▼
              ┌─────────────────────────────────────┐
              │          Azure Databricks           │
              │                                     │
              │       Unity Catalog + PySpark       │
              │                                     │
              │  ┌─────────────┐                    │
              │  │   Bronze    │                    │
              │  │ Raw Delta   │                    │
              │  └──────┬──────┘                    │
              │         │                           │
              │         ▼                           │
              │  ┌─────────────┐                    │
              │  │   Silver    │                    │
              │  │ Cleaned &   │                    │
              │  │ Transformed │                    │
              │  └──────┬──────┘                    │
              │         │                           │
              │         ▼                           │
              │  ┌─────────────┐                    │
              │  │    Gold     │                    │
              │  │ Star Schema │                    │
              │  └──────┬──────┘                    │
              └─────────┼───────────────────────────┘
                        │
                        ▼
              ┌──────────────────────┐
              │   Business Views     │
              │                      │
              │ Shipment Status      │
              │ Orders by Priority   │
              │ Delivery Performance │
              └──────────────────────┘


                     REAL-TIME STREAMING PIPELINE
              
               ┌──────────────────┐
               │ Python Producer  │
               │     VS Code      │
               └────────┬─────────┘
                        │
                        ▼
               ┌──────────────────┐
               │  Azure Event Hub │
               │ logistics_events │
               └────────┬─────────┘
                        │
                        ▼
               ┌────────────────────────────┐
               │ Databricks Structured      │
               │ Streaming                  │
               └────────────┬───────────────┘
                            │
                            ▼
                     Delta Streaming
                            │
                            ▼
                   Silver / Gold Layer

```
## ☁️ Azure Services Used

| Service                      | Purpose                                |
| ---------------------------- | -------------------------------------- |
| Azure Data Factory           | Batch data ingestion and orchestration |
| Azure Data Lake Storage Gen2 | Central data lake storage              |
| Azure Databricks             | Data processing and transformation     |
| Unity Catalog                | Data governance and catalog management |
| Delta Lake                   | Reliable transactional data storage    |
| Azure Event Hubs             | Real-time event ingestion              |
| PySpark                      | Data transformation and processing     |
| GitHub                       | Source control and version management  |

---

## 🔄 Batch Data Pipeline
    
    Source CSV / JSON
           ↓
    Azure Data Factory
           ↓
    ADLS Gen2 Raw
           ↓
    ADLS Gen2 Bronze
           ↓
    Azure Databricks
           ↓
        Silver
           ↓
          Gold

## ⚡ Real-Time Streaming Pipeline

    Python Producer
          ↓
    Azure Event Hubs
          ↓
    Kafka-Compatible Endpoint
          ↓
    Databricks Structured Streaming
          ↓
    Streaming Bronze
          ↓
    Streaming Silver
          ↓
    Gold fact_tracking_events

## 🗂️ Project Structure

    End-to-End-Logistics-Data-Engineering-Project/
    │
    ├── README.md
    │
    ├── Databricks/
    │   │
    │   ├── Bronze/
    │   │   └── bronze_adls.py
    │   │
    │   ├── Silver/
    │   │   └── Silver_transformations.py
    │   │
    │   ├── Gold/
    │   │   ├── Model.schema.py
    │   │   └── streaming_gold_transformations.py
    │   │
    │   └── Streaming/
    │       ├── STREAMING_TRACKING_EVENTS.py
    │       └── streaming_silver_transformations.py
    │
    ├── ADF/
    │   ├── pipelines/
    │   ├── datasets/
    │   └── linkedServices/
    │
    ├── SQL/
    │   └── business_views.sql
    │
    ├── architecture/
    │   └── architecture-diagram.png
    │
    ├── docs/
    │   ├── FINAL_PROJECT_STATUS.md
    │   ├── project-overview.md
    │   └── data-dictionary.md
    │
    ├── customers.csv
    ├── drivers.csv
    ├── vehicles.csv
    ├── warehouses.csv
    ├── orders.csv
    ├── shipments.csv
    └── tracking_events.csv
    
---    
## 🧪 Data Quality

- Missing customer city
- Missing customer ID
- Negative order value
- Missing driver ID
- Invalid latitude
- Invalid longitude
- Duplicate streaming events

## 📌 Final Architecture
                         SOURCE DATA
                              │
                              ▼
                   ┌──────────────────┐
                   │ Azure Data       │
                   │ Factory          │
                   └────────┬─────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │ ADLS Gen2        │
                   │ Raw / Bronze     │
                   └────────┬─────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │ Azure Databricks │
                   │ + Unity Catalog  │
                   └────────┬─────────┘
                            │
                            ▼
                         BRONZE
                            │
                            ▼
                         SILVER
                            │
                            ▼
                          GOLD
                            │
                 ┌──────────┴──────────┐
                 │                     │
           Star Schema          Business Views
                 │                     │
        Fact + Dimensions      SQL Analytics


                REAL-TIME STREAMING

          Python Event Producer
                    │
                    ▼
             Azure Event Hubs
                    │
                    ▼
        Databricks Structured
              Streaming
                    │
                    ▼
               Delta Lake
                    │
                    ▼
             Silver / Gold
## 📌 Project Status
 **Status: Completed**

**Implemented components:**

    -✅ Azure Data Factory ingestion
    -✅ ADLS Gen2 data lake
    -✅ Dynamic metadata-driven ingestion
    -✅ Bronze layer
    -✅ Silver layer
    -✅ Gold Star Schema
    -✅ Unity Catalog
    -✅ Delta Lake
    -✅ Azure Event Hubs
    -✅ Databricks Structured Streaming
    -✅ Streaming Bronze
    -✅ Streaming Silver
    -✅ Streaming-to-Gold integration
    -✅ Fact and dimension tables
    -✅ Business SQL views
-✅ Databricks Secret Scope
✅ Managed Identity
✅ GitHub source control
