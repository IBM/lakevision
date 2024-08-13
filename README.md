# LakeVision



Lakevision is a tool which provides insights into your Data Lakehouse, based on Apache Iceberg table format.

It provides all the namespaces available in your Lakehouse as well as all the tables available in each namespace, the tables' properties and schema, snapshots, partitions, sort-orders, references/tags as well as sample data. It also supports nested namespaces.

This can be very helpful in finding data layout in your Apache Iceberg based Lakehouse, details, structure of each table, location of data and metadata files, change history, and a lot more. It heavily uses `pyiceberg` and has very few other dependencies.

## How to install and use

The easiest way is to run it with Docker. 

1. Build the image:
    
    a. Clone this repo and cd to `lakevision` directory.
    
    b. Build:

    `docker build -t lakevision:1.0 .`

2. Provide the configuration - you will need Iceberg Catalog URL, and authentication required by the catalog, AWS keys/configuration. Fill in these values in my.env file and provide it to the docker run command.

3. Run the docker image, like:

    `docker run --env-file my.env -p 8501:8501 -it lakevision:1.0`

4. The Lakevision application is built on Streamlit and it listens on port 8501. If everything built fine, you would see output on command line about the app listening on port 8501. Launch the browser http://localhost:8501 and you would see all the details of your Lakehouse.

Tested in Linux and Mac with Iceberg REST catalog, but will support any catalog that works with pyiceberg.

## Roadmap

1. Provide each partition details like name, filecount, number of records, file sizes etc. - Done
2. Provide ability to see sample data based on partitions. - Done
3. Add insights on tables.
4. Add timetravel capability.
5. Much more.....

    
## Contributing

Contributions are very much welcome from all.


[def]: /assets/demo.mp4