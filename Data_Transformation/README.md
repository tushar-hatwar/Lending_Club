# Lending Score Calculation

## Introduction

The lending score is a critical metric used by lending loan firms to assess the financial health of a borrower and mitigate the risk involved in lending money. The lending score is calculated against each borrower who avails for a personal credit, which is funded by fellow lenders. It provides lenders with valuable insights into a borrower's payment history, defaulters history, and financial health. 

It is essential to calculate the lending score frequently to have an up-to-date status on the borrower's financial history. This project aims to handle the lending score calculation by performing data transformations on top of the cleaned data.

## Data Transformation

The lending score is calculated based on several factors such as the number of missed payments, credit utilization rate, credit score, and income. Therefore, we need to extract the necessary fields from the cleaned data and remove any duplicate or irrelevant data before proceeding with the calculation.

## Lending Score Calculation

The lending score is calculated using the following formula:

`Lending Score = 1000 - (number of missed payments * 10) - (credit utilization rate * 30) - (credit score * 3) + (income * 2)`

We will consider various scenarios and use cases based on the lending loan data available to us and calculate the lending score and lending grade against each borrower.

## Lending Grade Calculation

The lending grade is calculated based on the lending score range. The following table illustrates the lending score range and the corresponding lending grade.

| Lending Score Range | Lending Grade |
| ------------------- | -------------|
| 900-1000            | A+           |
| 800-899             | A            |
| 700-799             | B            |
| 600-699             | C            |
| 500-599             | D            |
| 400-499             | E            |

## Conclusion

The lending score calculation is a crucial use case in the lending loan project. By performing data transformations on top of the cleaned data, we can calculate the lending score and lending grade against each borrower. This provides lenders with valuable insights into a borrower's financial health and helps mitigate the risk involved in lending money.
