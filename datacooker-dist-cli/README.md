Apache Spark based **Dist** utility to supplement **Data Cooker** ETL tool and replace 'dist-cp'/'s3-dist-cp' to copy data from external storage to Spark cluster and back.

Doesn't require strict schema nor data catalog, but can select 'columns' from underlying delimited text. Also can copy Parquet and plain text files, and can be extended to adapt other storages (as an example, there are bare-bone JDBC adapters included).
 
Historically it was used in a GIS project, so is well tested and proved by years in production.

* [Build How-to](../BUILD.md)

Your contributions are welcome. Please see the source code in the original GitHub repo at https://github.com/PastorGL/datacooker-dist
