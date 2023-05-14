# Types of Hive Tables

Apache Hive is a data warehouse infrastructure built on top of Apache Hadoop. Hive provides a SQL-like interface to query data stored in Hadoop Distributed File System (HDFS) or other data storage systems such as Amazon S3. Hive Tables are a way of organizing and structuring data in Hive. 

There are several types of Hive tables that can be created in Hive, depending on how the data is organized and stored. In this README.md file, we will discuss some of the common types of Hive tables.

## 1. Managed Tables

Managed tables are the default type of table in Hive. When a managed table is created, Hive assumes full control over the table and its data. The data is stored in a directory managed by Hive in HDFS or other storage systems. The metadata of the table, such as the schema and table properties, is also stored in the Hive Metastore. Managed tables are typically used for storing and managing structured data.

## 2. External Tables

External tables are similar to managed tables, but the data is stored outside of the Hive data warehouse. Instead, external tables point to data that is stored in an external location, such as HDFS or S3. When an external table is created, Hive only manages the metadata of the table, such as the schema and table properties. This allows external tables to reference data that is created and managed by other tools, such as Spark or Pig.

## 3. Partitioned Tables

Partitioned tables are tables that are divided into partitions based on a specific column or set of columns. Partitioning is a way of organizing data in Hive to improve query performance. By partitioning the data, Hive can avoid scanning the entire table when querying for specific data. Partitioning can also be used to create smaller, more manageable subsets of data. Hive supports both static and dynamic partitioning.

## 4. Bucketed Tables

Bucketed tables are similar to partitioned tables, but the data is divided into buckets based on a specific column or set of columns. Bucketing is another way of organizing data in Hive to improve query performance. By bucketing the data, Hive can group related data together, which can improve join performance. Bucketing can also be used to create smaller, more manageable subsets of data.

## 5. Virtual Tables

Virtual tables, also known as views, are not actual tables, but rather a virtual representation of one or more tables in Hive. Virtual tables can be used to simplify complex queries or to restrict access to sensitive data. When a virtual table is queried, Hive retrieves the data from the underlying tables and returns the results as if they came from the virtual table.

## Conclusion

Hive tables are a powerful tool for organizing and managing data in Hadoop. By understanding the different types of Hive tables, you can choose the appropriate table type for your data and improve query performance. Managed tables, external tables, partitioned tables, bucketed tables, and virtual tables all have their own unique advantages and use cases.

---

### Visuals

Here are some visuals to help you better understand the different types of Hive tables:

#### Managed Table

![Managed Table](https://i.imgur.com/gQXGLLG.png)

In a managed table, the data and metadata are both stored in the Hive Warehouse directory.

#### External Table

![External Table](https://i.imgur.com/Q8J5l4K.png)

In an external table, only the metadata is stored in Hive, while the actual data is stored outside of Hive.

#### Partitioned Table

![Partitioned Table](https://i.imgur.com/ZfX35kz.png)

In a partitioned table, the data is divided into partitions based on a specific column or set of columns.

#### Bucketed
