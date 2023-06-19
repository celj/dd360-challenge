# Challenge for Data Engineer position at DD360

## Setup

First, let's go to the Snowflake console and run the `setup.sql` script. This creates our warehouse, and the necessary permissions.

For replication, you need to start the containers with

```sh
docker compose up -d
```

All defaults were kept in this project, so airflow will be hosted at [`http://0.0.0.0:8080`](http://0.0.0.0:8080), and login keys are:

-   user: `airflow`
-   password: `airflow`

Before running the DAGs, establish your connection to Snowflake on `Admin > Connections` and select `Connection Type: Snowflake`. For compatibility, name your `Connection ID` as `sf`. In my case, I used a trial version of Snowflake with the following keys (which will be deactivated in the next few days for security reasons):

-   Account: `oa46453.us-central1.gcp`
-   Login: `celj`
-   Password: `dagbin-Xuztus-dypze8`
-   Role: `accountadmin`
-   Warehouse: `dd360`

Finally, go back to `DAGs` tab, and trigger the `snowflake_automation`[^1] DAG.

[^1]: Please note that this DAG runs hourly, so you may need to wait a few minutes to see the results by yourself.

To kill airflow and the containers, run

```sh
docker compose down --volumes --rmi all
```

## File Structure

All code is located in the `dags` folder. The `settings` module establishes the global configurations to scale many DAGs with the same settings. In our case, it was only necessary to set the default timezone to `UTC`. Moreover, the `utils` module contains useful functions to handle most common situations, such as moving a pandas dataframe to a Snowflake table and vice-versa. Finally, the `weather` directory contains the DAG with the same name and their utility functions in the `utils` submodule.

## Web Scraping

We may easily explore the front-end code of a website only by using the browser's developer tools. We can find that all of required data was rendered through a span element as follows:

```html
<span id="{tag}">{value}</span>
```

This way, it is quite easy to extract the data with a simple regex search as follows:

```python
r'<span id="{}">(.*?)</span>'.format(span_id)
```

Beyond that, we need to add a request status registry to keep track of the requests that failed. This way, we can retry them later or even try to contact the website's owner to fix the problem.

## Process Output JSON Data

A predefined catalog of the required cities is build as one of the first steps of the DAG. This way, we can ensure that the data is always consistent.

> We could scale this process by using a Google Sheets file as a catalog. This way, we could easily add new cities without the need to change the code.

This catalog contains the raw city name, its city id and a more readable name. This way, we can easily join the data with the weather data.[^2]

[^2]: The raw city name is used to build the URL to request the data. E.g., `https://www.meteored.mx/ciudad-de-mexico/historico` for Mexico City.

Processing turn out to be quite simple. We only need to extract the data from the JSON file and move the data to Snowflake.

Here, we build a raw table for all webscraped data: `WEATHER.RAW.RECORDS`. There is a dedicated database `WEATHER` to store all weather-related data. This way, we can easily scale the project by adding more tables to this database.

Furthermore, two schemas were created to separate the raw data from the processed data. The `RAW` schema contains the raw data, and the `STAGING` schema contains the processed data.

In the `STAGING` schema we have the following tables:

-   `CITIES`: contains the catalog of cities.
-   `REQUESTS`: contains the status of each request for every execution and city.
-   `HOURLY`: contains the successfully obtained hourly weather data for each city.
-   `SUMMARY`: contains a summary of the data for each city fetched hourly, this way we may easily observe the time series evolution of `HUMIDITY` and `TEMPERATURE`. This helps on trend identification and anomaly detection since it considers the minimum, maximum and average values for each city history.

We may consider the creation of a `PROD` schema to store the final data (as it is useful for dashboard ingestion), but this was not necessary for this project.

## Parquet Files

Parquet files are a great way to store data in a columnar format. This way, we can easily query the data without the need to load it into a database. Here, we handle data through a single task to upload parquet files to a local temporary folder. To scale this process, we could use a cloud storage service such as AWS S3 or Google Cloud Storage. We may easily dedicate a bucket for this purpose.

> Temporary directories are useful to store data into a cloud storage service such that, after every execution, the directory is deleted. Kubernetes is a great way to handle this kind of task as it deploys small computing units called pods.

## Code Integrety

To ensure that the code is working as expected, Github Actions were set up to run linting tests on every push. The `flake8` package was used to check the code style, and the `black` package was used to format the code. The `black` package was configured to follow the `PEP8` style guide.

This way, we can ensure that the code is readable and consistent. All settings are located in the `.github/workflows/lint.yml` file.

Additionally, all features were merged through dedicated branches to ensure that the `main` branch is always working as expected.

## Next Steps

Some ideas were already mentioned in the previous sections, but here is a list of possible next steps:

-   Add more cities to the catalog.
-   Implement statistical tests to detect time series anomalies.
-   Implement a dashboard to visualize the data.
    -   We may use Google Data Studio, Tableau or even a custom dashboard with React and D3.js. Of course, this depends on the use case and our available resources.
-   In the future, if we need analysts to handle their own queries to create datamarts, we may consider using `dbt` to implement more complex data pipelines. This can be easily integrated with Airflow as a git submodule executed by a `BashOperator`.
-   We may consider using Kubernetes to deploy the DAGs and the database. This way, we can easily scale the project by adding more pods to handle more cities. This is a great way to handle big data projects.
-   For data science models, we may consider build solutions to keep track of the data lineage. This way, we can easily track the data used to train a model and the data used to make predictions. This is a great way to ensure that a model is working as expected.
