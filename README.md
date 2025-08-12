# SQL-Based E-Commerce Analytics Platform (Olist Dataset)

## Executive Summary

This repository documents an end-to-end analytics project on the Brazilian Olist e-commerce dataset. Using MySQL, I developed a complete data pipeline that starts with cleaning raw, multi-source `.csv` files and culminates in a series of advanced analytical queries. The project is designed to mirror real-world business intelligence tasks and extract strategic insights from complex data.

## Key Insights & Queries

* **Data Integrity:** Implemented a robust data cleaning process to standardize formats, impute missing values, and resolve inconsistencies, creating a reliable foundation for analysis.
* **Customer Segmentation (RFM):** Engineered an RFM model to identify and profile key customer segments, enabling data-driven decisions for retention and marketing efforts.
* **Operational Efficiency:** Analyzed the entire order lifecycle to pinpoint bottlenecks in the delivery chain and benchmark seller processing times against fulfillment SLAs.
* **Revenue & Product Strategy:** Identified top-performing and under-performing product categories, and discovered product bundling opportunities through co-purchase pattern analysis.
* **Geospatial Hotspots:** Aggregated and analyzed geolocation data to map high-density customer zones, providing a basis for targeted logistics or marketing.

## Technologies Used

* **Database:** MySQL
* **Language:** SQL
* **Version Control:** Git / GitHub

## Replicating the Analysis

A MySQL environment (e.g., MySQL Server & Workbench) is required to run this project.

1.  **Create Schema:** Initialize a new database schema (e.g., `olist_db`).
2.  **Download Dataset:** Obtain the raw `.csv` files from the [Olist Kaggle page](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
3.  **Build Database:** Use a schema script to create the necessary tables and then import the data from the corresponding `.csv` files.
4.  **Run Analysis:** Execute the `olist_analysis.sql` script to apply all cleaning transformations and run the 10 analytical queries.
