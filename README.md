# data2botsassessment
My response for the technical assessment of the Analytics Engineering role at Data2Bots.

 - The data was extracted from the AWS bucket using the `load_data.py` script in the [fal_scripts](https://blog.fal.ai/populate-dbt-models-with-csv-data/) directory.
 - Three main directories were created for the models i.e. `Staging`, `Intermediate` and `Marts`. The `Marts` models are materialized as tables upon running a production job in dbt.

To initialize a local instance of dbt-core for the project:
 - Build the docker image

   `docker build -t terraform-with-tools .`
   
  This will create a Docker image named terraform-with-tools.

- Run the container:

  `docker run -it --rm -v /path/to/local/code:/app terraform-with-tools`

- Once inside the container, Python, pip, AWS CLI, dbt-core, and boto3 can be deployed as needed.
