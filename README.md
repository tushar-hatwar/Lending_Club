# Lending_Club
Extracting, Loading and Transforming Data using python, Azure Databricks, Azure Blob Storage

# Project Name: Using Lending Loan API for Data Analysis

This project utilizes lending loan API data in the form of CSV files, which are stored in Azure Data Lake in raw form. The data is then cleaned using Azure Databricks PySpark and written back to Azure Data Lake in a cleaned format. Finally, the cleaned data is analyzed using HIVE.

## Project Architecture

The project architecture consists of the following components:

- **Lending Loan API**: This is the source of the data used in the project. The API provides data in the form of CSV files.
- **Azure Data Lake**: This is where the raw data from the Lending Loan API is stored.
- **Azure Databricks**: This is used to clean the data using PySpark.
- **HIVE**: This is used to perform data analysis on the cleaned data.

Project Flow Diagram:
<img width="716" alt="image" src="https://github.com/tushar-hatwar/Lending_Club/assets/60131764/106c0f8a-25e5-467d-bcab-0eda03827992">

The following diagram shows the architecture of the project:

```
        +----------------+           +----------------+
        |Lending Loan API|  CSV      |Azure Data Lake  |
        +----------------+---------> +----------------+
                                       |Raw Data        |
                                       +----------------+
                                               |
                                               | PySpark
                                               |
                                       +----------------+
                                       |Azure Databricks|
                                       +----------------+
                                               |
                                               | HIVE
                                               |
                                       +----------------+
                                       |Data Analysis   |
                                       +----------------+
```

## Installation and Setup

To run this project, you will need the following:

- An Azure account
- Access to the Lending Loan API
- Azure Data Lake Storage Gen1 or Gen2
- Azure Databricks workspace
- HIVE

To set up the project, follow these steps:

1. Clone the project repository to your local machine.
2. Create an Azure Data Lake Storage Gen1 or Gen2 account.
3. Copy the CSV files from the Lending Loan API to the Azure Data Lake Storage account.
4. Create an Azure Databricks workspace and import the PySpark script from the project repository.
5. Execute the PySpark script in Azure Databricks to clean the data and write it back to Azure Data Lake in a cleaned format.
6. Set up HIVE and execute queries to analyze the cleaned data.

Dataset used are present in dataset directory

## Usage

To use this project, follow these steps:

1. Set up the project as described in the Installation and Setup section.
2. Execute the PySpark script in Azure Databricks to clean the data and write it back to Azure Data Lake in a cleaned format.
3. Set up HIVE and execute queries to analyze the cleaned data.
4. Use the insights gained from the data analysis to make informed business decisions.

## Conclusion

This project demonstrates how to use the Lending Loan API for data analysis. By leveraging Azure Data Lake, Azure Databricks, and HIVE, we can efficiently process large volumes of data and gain valuable insights that can be used to make informed business decisions.


