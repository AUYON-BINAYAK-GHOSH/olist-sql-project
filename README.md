# End-to-End SQL Analysis of the Olist E-Commerce Dataset

## Project Overview

This repository contains a comprehensive SQL-based analysis of the public Olist e-commerce dataset, which features nearly 100,000 orders from a Brazilian marketplace. The project showcases a full data analysis workflow, from initial data cleaning and preparation to in-depth, actionable business intelligence queries.

The entire analysis is performed using SQL (MySQL dialect) and is structured to be a showcase of practical data analysis skills.

## Key Features

* **Data Cleaning:** A robust script to clean the raw data, handling missing values, illogical timestamps, and duplicate geolocation entries.
* **RFM Customer Segmentation:** Identifies high-value and at-risk customer segments.
* **Performance Analysis:** Deep dives into delivery performance, product category success, and payment method trends.
* **Advanced Analytics:** Includes queries for geospatial analysis (customer hotspots), product bundling, and sales funnel drop-off rates.

## How to Use This Project

To run this analysis, you will need a MySQL database environment (like MySQL Server + MySQL Workbench).

1.  **Set Up Database:** Create a new database schema (e.g., `olist_db`).
2.  **Download Data:** Get the Olist dataset from Kaggle: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
3.  **Create Tables & Import Data:** First, you must create the tables using a schema script and then import the data from the downloaded `.csv` files into their corresponding tables in your database.
4.  **Run the Analysis Script:** Execute the `olist_analysis.sql` script provided in this repository. It will first clean the data and then run all 10 analysis projects.