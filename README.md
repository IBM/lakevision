# Lakevision

Lakevision is a tool which provides insights into your Data Lakehouse, based on `Apache Iceberg` table format.

It provides all the namespaces available in your Lakehouse as well as all the tables available in each namespace, the tables' properties and schema, snapshots, partitions, sort-orders, references/tags as well as sample data. It also supports nested namespaces.

This can be very helpful in finding data layout in your Apache Iceberg based Lakehouse, details, structure of each table, location of data and metadata files, change history, and a lot more. It heavily uses pyiceberg and has very few other dependencies.

https://github.com/user-attachments/assets/7c71d61f-ffea-497a-97d0-451dec662b96



## Features

- Search and view all namespaces in your Lakehouse

- Search and view all tables in your Lakehouse

- Schema, properties, partition specs, summary of each table

- Every partition-wise record count, file count, size etc.

- Show all snapshots with details

- Graphical summary of records additions over time

- OIDC/OAuth based authentication support

- Pluggable authorization feature

- Pluggable Chat with Lakehouse capability

## How to install and use:

The easiest way is to run it with Docker.

Build the image:

a. Clone this repo and cd to lakevision directory.

b. Build:

```
docker build -t lakevision:1.0 .
```

Provide the configuration - you will need Iceberg Catalog URL, and authentication required by the catalog, AWS keys/configuration. Fill in these values in my.env file and provide it to the docker run command.

Run the docker image, like:

```
docker run --env-file my.env -it -p 8081:8081 lakevision:1.0 /app/start.sh
```

The Lakevision application is built on `SvelteKit` and `Fastapi`. If everything built fine, you would see output on command line about the backend app listening on port 8000. Launch the browser http://localhost:8081 and you would see all the details of your Lakehouse.

Tested on Linux and Mac with Iceberg REST catalog, but will support any catalog that works with pyiceberg.

## Roadmap

- Provide Chat with Lakehouse capability using LLM.

- Provide overall reports on tables with maximum snapshots, maximum partitions, maximum columns, size etc.

- Provide recommendations on optimizing table performance.

- Provide limited SQL capabilities on tables.

- Provide each partition details like name, filecount, number of records, file sizes etc. - Done

- Provide ability to see sample data based on partitions. - Done

- Add insights on tables.

- Add timetravel capability.

Much more.....

## Contributing

Contributions are very much welcome from all.
