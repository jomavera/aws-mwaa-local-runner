# Python, MySQL and Airflow for data analysis

This project allows to generate a docker container with a MySQL database and airflow server to automate analysis tasks. We can make automated queries from airflow using sqlalchemy to the MySQL database from our local environment.

## Technologies

* `Docker` for containerization.

* `Postgres` for airflow database.

* `MySQL` for datawarehouse.

* `sqlalchemy` for query the database.

* `pandas` for data manipulation.

## Steps

1. Run the following in the command line:

    `./mwaa-local-env start`

    this will build the docker image and get up the container.

2. Access the airflow server from `localhost:8080`. The user is `admin` and password `test`.

3. Run manually the "ExtractLoadTransform" DAG from the airflow UI. This will query the MySQL database and generate an excel file for each query. Since I haven't implemented a connection to a cloud storage I stored the output files in the same server as airflow. This excel files will be stored in the airflow server in the `/usr/local/airflow/` directory.

4. Get the container id to acces the saved files. It is necessary to get the container id to acces to its stored files. Run `docker ps` and save the container id for the image "amazon/mwaa-local:2_8"

5. Copy the generated answer files from docker container. To download the files run the following command for each one of the 5 answers. Replace `[X]` with 1 to 5:

      `docker cp <containerId>:/usr/local/airflow/answer_[X].xlsx /path/to/local/directory/answer_[X].xlsx`

### Queries

-	Top 10 stores per transacted amount

This is answered by a sum aggregation of transaction amount grouped by `store_id` from all transactions associated with a store. I considered transactions with all status.

- Top 10 products sold.

Since I detected that for a certain `product_sku` can be associated more than one `product_name`. I decided to use the column `product_sku` as unique identifier. I did a sum aggregation from all transactions grouped by `product_sku`. I considered transactions with all status.

- Average transacted amount per store typology and country.

This can be calculated by a average aggregation of transaction amount grouped by `typology` and `country` of the store associate with the transaction. I considered transactions with all status.

- Percentage of transactions per device type.

I made a count of all transactions and the count of transactions for each device type. With these values I calculated the percentage per device. I considered transactions with all status.

- Average time for a store to perform its 5 first transactions.

Here I considered that the time to its 5 first transactions is the time it takes to a store in each day to have 5 transactions. Here I considered for each store and each day the 5 earliest transactions. Then calculate the max time of the 5 earliest transactions for each store and day. Finally, I calculate the average time (in hours) grouped by store id. Here I considered transactions with all status.
