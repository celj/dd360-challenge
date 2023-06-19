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

Before running the DAGs, establish your connection to Snowflake on `Admin > Connections` and select `Connection Type: Snowflake`. For compatibility, name your `Connection ID` as `sf`. In my case, I used a trial version of Snowflake with the following keys (which will be deactivated in next couple of days for security reasons):

-   Account: `oa46453.us-central1.gcp`
-   Login: `celj`
-   Password: `dagbin-Xuztus-dypze8`
-   Role: `accountadmin`
-   Warehouse: `dd360`

Finally, go back to `DAGs` tab, and trigger the `snowflake_automation`[^1] DAG.

[^1]: Please note that this DAG runs every hour.

To kill airflow and the containers, press `^C` and, then, run

```sh
docker compose down --volumes --rmi all
```
