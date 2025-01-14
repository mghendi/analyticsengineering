# Data pipeline for Suluhu with dbt, AWS, Python, and Postgres.

I created this repo as practice for dbt in a production environment. The structure mirrors an ELT batch pipeline that loads the business data in a fictional e-commerce company's (Suluhu) data warehouse and performs the transformation upon execution.

 ### Background
 
 - The project contains:
   - orders.csv: Fact table about orders gotten on their website
   - reviews.csv: Fact table on reviews given for a particular delivered product
   - shipments_deliveries.csv: Fact table on shipments and their delivery dates

![Org](https://github.com/mghendi/analyticsengineering/assets/26303032/928361ad-0cdb-4770-b7b2-33ff21e95f13)

 ### Tools

 - Python - Extract and load
 - SQL - Transformation
 - dbt - Data modeling and testing
 - PostgreSQL - Data warehouse
 - AWS S3 - Data lake
 - Docker - Infrastructure
 - Akuko - Visualization
 
 ### Process
 
 - The data was extracted from the AWS bucket using the `load_data.py` script in the [fal_scripts](https://blog.fal.ai/populate-dbt-models-with-csv-data/) directory.
 - Three main directories were created for the models i.e. `Staging`, `Intermediate` and `Marts`. The `Marts` models are materialized as tables upon running a production job in dbt.
 - The production tables have been created on the `reporting` schema.
 - Each model and script includes necessary comments detailing my thought process.

### Dashboard

- The dashboard has been created on [Akuko](https://akuko.io/)

![image](https://github.com/mghendi/suluhu/assets/26303032/9741074e-e036-4c32-a20e-f1b0031456aa)


To initialize a local instance of dbt-core for the project:
 - Build the docker image

   `docker build -t dbt-venv .`
   
  This will create a Docker image named dbt-venv.

- Run the container:

  `docker run -it --rm -v /path/to/local/code:/app dbt-venv`

- Once inside the container, Python, pip, AWS CLI, dbt-core, and boto3 can be deployed as needed.
