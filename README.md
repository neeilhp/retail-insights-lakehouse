# 🛍️ Retail Insights Lakehouse

📌 Project Overview

This project simulates a modular data pipeline for retail analytics using PySpark and pandas. It demonstrates key ETL concepts, schema modeling, and enrichment logic — all designed for reproducibility and clarity. The pipeline transforms raw sales data, applies business rules, joins with dimension tables, and outputs clean CSVs ready for analysis or dashboarding.


🔗 Live Dashboard
[View on Looker Studio](https://lookerstudio.google.com/reporting/591680cc-d6bb-4c9f-ba8e-e1f41821a26c/page/iILaF)


📊 Key Features
- Modular visuals across 6 sections
- STAR-style captions for business storytelling
- Time trend analysis and loyalty tier breakdown
- Built from Athena queries and S3 outputs

🧠 Business Insights
- Silver-tier customers contribute 41.5% of revenue
- Bulk orders drive majority of high-value transactions
- Revenue dipped on [insert date] — potential inventory lag

🧠 Business Context

Retailers often track sales across multiple stores, products, and customers. This pipeline answers questions like:

Which sales qualify as bulk orders?

How do discounts affect revenue?

What are the enriched insights when combining sales with product, store, and customer metadata?

🛠️ Tech Stack

PySpark for scalable transformations

pandas for final CSV export (due to Spark write limitations on Windows)

VS Code for development

Windows 11 + JDK 17 with Hadoop workaround (winutils.exe)

GitHub for project structure and documentation

AWS Athena + S3
Google Sheets + Looker Studio

🧪 Pipeline Steps
1. Raw Data Generation
Simulated using Python and Faker
Includes sales, products, stores, and customers

2. Transformation
Applies discount logic: 10% off for quantity ≥ 3
Flags bulk orders: "Yes" if quantity ≥ 3, else "No"

3. Enrichment
Joins sales with:
    products.csv on product_id
    stores.csv on store_id
    customers.csv on customer_id
Selects final columns for analysis

4. Export
Writes sales_transformed.csv and sales_enriched.csv using pandas.
Ensures clean, single-file outputs for portfolio and downstream use.

🧬 Final Schema: sales_enriched.csv

Column	Description

    sale_id	Unique sale identifier
    date	Transaction date
    quantity	Units sold
    amount	Original sale amount
    discounted_amount	Amount after discount (if applicable)
    bulk_order	"Yes" if quantity ≥ 3, else "No"
    product_name	Name of the product
    category	Product category
    store_name	Store where sale occurred
    location	Store location
    customer_name	Full name of customer
    gender	Customer gender

⚠️ Known Limitations (Challenges)
Spark .write.csv() fails on Windows with JDK 17 due to Hadoop permission simulation.
Workaround: use pandas .to_csv() after converting Spark DataFrame.


## 📐 Architecture Overview

- **Data Ingestion**: Simulated via Python Faker
- **Storage**: AWS S3 (Raw → Curated → Analytics)
- **ETL**: PySpark
- **Modeling**: Star & Snowflake schemas using dbt
- **Warehouse**: Amazon Redshift / Snowflake
- **Orchestration**: Apache Airflow
- **Visualization**: Power BI / QuickSight

## 🎯 Goals

- Showcase modular ETL design
- Compare Star vs Snowflake schema performance
- Build reusable components for real-world data engineering

## 🔄 Data Transformation Scope

This project goes beyond basic joins to demonstrate a variety of PySpark transformations, including:

- **Filtering**: Isolating high-value transactions
- **Column Derivation**: Calculating discounted amounts and profit margins
- **Aggregation**: Summarizing sales by store, region, and product
- **Data Cleaning**: Handling nulls and duplicates
- **Type Casting**: Ensuring schema consistency for downstream modeling
- **Pivoting and Window Functions**: Advanced analytics for ranking and trend analysis

These transformations simulate real-world business logic and prepare the data for both Star and Snowflake schema modeling.

## 💡 Why This Project Matters

This project demonstrates not just technical proficiency, but the ability to think like a data engineer solving real business problems. It reflects:
- End-to-end pipeline design
- Schema-aware transformations
- Cloud-native architecture



## 📁 Folder Structure

retail-insights-lakehouse/
├── data/
│   ├── products.csv
│   ├── stores.csv
│   └── customers.csv
├── curated/
│   ├── sales_transformed.csv
│   └── sales_enriched.csv
├── notebooks/
│   └── retail_etl.py
├── README.md
└── requirements.txt

![Upload to S3](https://github.com/neeilhp/retail-insights-lakehouse>/actions/workflows/upload.yml/badge.svg)

## 🔁 Automated S3 Sync with GitHub Actions

This project includes a GitHub Actions workflow that automatically uploads curated analytics files to AWS S3 whenever changes are pushed to the `curated/` folder.

### How It Works
- ✅ Triggered on every push to `curated/**`
- ✅ Runs `upload_to_s3.py` to sync files with S3
- ✅ Uses secure AWS credentials stored in GitHub Secrets
- ✅ Keeps cloud storage aligned with latest repo state

This setup ensures reproducibility, automation, and production-readiness — key traits of a modern data engineering pipeline.


