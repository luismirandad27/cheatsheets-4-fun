# SQL Cheat Sheet (Work in Progress)

## What is SQL?
SQL stands for **Structured Query Language**, it's a programming languange that help us to manage and manipulate databases. It allows us to do many things like retrieve data, insert records, delete records, create new databases and so on.

Even though it is a standard language, it has some differences between RDBMSs (Relational Database Management Systems) such as MySQL, SQL Server, Oracle, Postgres, and others.

In this Cheat Sheet, I'm going to cover the most common syntaxis and those differences between RDBMSs.

---
## 1. How to display a sample of data from a table?
```sql
SELECT * FROM Employees;
```
This is the most common way to display or retrieve data from a table. When we put the wildcard '*' we are saying that we want to retrieve all the columns available in the table.
&nbsp;
## 2. Data Types
Like every programming language, we have to deal with data types. This is a table with the list of the most common data types and the similarities between RDBMSs.

|                  | MySQL | PostgreSQL | Oracle | SQL Server |
| ---------------- | ----- | -------- | ------ | ---------- |
| BOOLEAN          | <img height="30" alt="python" src="assets/check.svg">     | <img height="30" alt="python" src="assets/check.svg">         |        |            |
| BIT              | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         |        | <img height="30" alt="python" src="assets/check.svg">           |
| INT              | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       | <img height="30" alt="python" src="assets/check.svg">           |
| INTEGER          | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       |            |
| BIGINT           | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         |        | <img height="30" alt="python" src="assets/check.svg">           |
| DECIMAL(m,d)     | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         |        | <img height="30" alt="python" src="assets/check.svg">           |
| NUMERIC(m,d)     | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       | <img height="30" alt="python" src="assets/check.svg">           |
| NUMBER(m,d)      |       |          | <img height="30" alt="python" src="assets/check.svg">       |            |
| FLOAT(m,d)       | <img height="30" alt="python" src="assets/check.svg">      |          | <img height="30" alt="python" src="assets/check.svg">       | <img height="30" alt="python" src="assets/check.svg">           |
| DOUBLE PRECISION |       | <img height="30" alt="python" src="assets/check.svg">         |        |            |
| LONG             |       | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       |            |
| CHAR(size)       | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       | <img height="30" alt="python" src="assets/check.svg">           |
| VARCHAR(size)    | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       | <img height="30" alt="python" src="assets/check.svg">           |
| VARCHAR2(size)   |       |          | <img height="30" alt="python" src="assets/check.svg">       |            |
| TEXT             |       | <img height="30" alt="python" src="assets/check.svg">         |        | <img height="30" alt="python" src="assets/check.svg">           |
| DATE             | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       | <img height="30" alt="python" src="assets/check.svg">           |
| DATETIME         | <img height="30" alt="python" src="assets/check.svg">      |          | <img height="30" alt="python" src="assets/check.svg">       | <img height="30" alt="python" src="assets/check.svg">           |
| TIMESTAMP        | <img height="30" alt="python" src="assets/check.svg">      | <img height="30" alt="python" src="assets/check.svg">         | <img height="30" alt="python" src="assets/check.svg">       |            |
| TIME             |       | <img height="30" alt="python" src="assets/check.svg">         |        | <img height="30" alt="python" src="assets/check.svg">           |

Consider:
- *m*: precision (total number of digits)
- *d*: scale (total number of digits after floating point)
&nbsp;
## 3. Comments
```sql
-- This is a single-line comment
/*
This is a multi-line comment.
*/
```
## 4. Querying tables with SQL (DML)
### a. Querying a Single Table
Retrieving data from a single table (all columns)
```sql
SELECT * FROM Employees;
```
#### Retrieving data from a single table (specific columns)
```sql
SELECT Employee_ID, First_Name, Last_Name, Email
FROM Employees;
```
### b. Sorting
#### Sorted by a single column
```sql
SELECT * 
FROM Employees
ORDER BY Hire_Date; --Ascending Sorting by Default
```
#### Sorted by a multiple column
```sql
SELECT * 
FROM Employees
ORDER BY Hire_Date ASC, Salary DESC;
```
This means that first we are sorting the employees based on the Hire Date in ascending order, if 2 (or more) employees have the same Hire Date, we are sorting those rows based on the salary in descending order.
### c. Aliases
#### Table Alias
```sql
SELECT *
FROM Employees as table_e;
```
#### Column Alias
```sql
SELECT
First_Name,
Last_Name,
Email as Employee_Email,
Hire_Date as "Employee Hire Date"
FROM Employees;
```
### d. Filtering
#### Getting the employees whose salaries are more than $1000
```sql
SELECT *
FROM Employees
WHERE Salary > 1000;
```
#### Getting the locations whose street address includes the word 'Drive'
```sql
SELECT *
FROM Locations
WHERE Street_Address LIKE '%Drive%';
```
This is a TEXT Comparison, in this case we are using a wildcard '%', these are the most common:
| Wildcard | Description                       |
| -------- | --------------------------------- |
| %        | Represent zero or more characters |
| \_       | Represents a single character     |
#### Getting the employees who don't have a manager assigned
```sql
SELECT *
FROM Employees
WHERE Manager_ID IS NULL;
```
These are examples of filtering operators (comparison, text and others). Check the following list of operators:
| Operator    | Description                            | Example                              |
| ----------- | -------------------------------------- | ------------------------------------ |
| \=          | Equal                                  | `Employee_ID = 3`               |
| \>          | Greater than                           | ` Salary > 1000 `                  |
| <           | Less than                              | ` Salary < 1000 `                  |
| >=         | Greater than or equal                  | ` Salary >= 1000 `                 |
| <=          | Less than or equal                     | ` Salary <= 1000 `                 |
| <>, !=      | Not Equal                              | ` Hire_Date <> '2023-01-02' `     |
| BETWEEN     | Between a range of values              | ` Salary BETWEEN 1000 AND 2000`    |
| LIKE        | Text operator. To search for a pattern | ` Street_Address LIKE '%Drive%' ` |
| IN          | To specify a set of multiple values    | ` Manager_ID IN (1,2,3)`          |
| IS NULL     | If the value is NULL                   | ` Manager_ID IS NULL `            |
| IS NOT NULL | If the value is not NULL               | ` Manager_ID IS NOT NULL`         |
#### Using multiple conditions on filtering
```sql
SELECT *
FROM Employees
WHERE Hire_Date = '2023-01-02' AND Salary > 1000;
```
These are the logical operators available:
| Operator | Description                                  |
| -------- | -------------------------------------------- |
| AND      | Display the row if both conditions are TRUE  |
| OR       | Display the row if any conditions are TRUE   |
| NOT      | Display the row if the condition is NOT TRUE |
### e. SQL Functions
#### STRING Functions
```sql
SELECT
    CONCAT(First_Name,' ',Last_Name) as Full_Name, --Luis Miranda
    LOWER(First_Name), --luis
    UPPER(First_Name), --LUIS
    SUBSTRING(First_Name,1,2) --Lu
FROM Employees
```
These are example of String functions, there are many functions and could vary based on the RDBMS you are using. You can check the documentation of the RDBMS you are working with.
#### NUMERIC Functions
```sql
SELECT
    Salary, --1200.459
    ROUND(Salary,1) --1200.5
FROM Employees;
```
The same as String functions, it is important to always check the documentation.
#### DATE Functions
....
#### ADVANCED Functions
Using `CASE ... WHEN`
```sql
SELECT
    Employee_ID,
    Email,
    CASE WHEN Email LIKE '%@gmail%' THEN 'Gmail'
         WHEN Email LIKE '%@outlook%' THEN 'Outlook'
         WHEN Email LIKE '%@yahoo%' THEN 'Yahoo'
         ELSE 'Others' as Email_Domain
FROM Employees;
```
Based on the RDBMS you are using, check the following functions (are important too):
- MySQL: `ISNULL`, `NULLIF`, `COALESCE`, `CAST`
- SQL Server: `ISNULL`, `ISNUMERIC`, `NULLIF`, `COALESCE`, `CAST`
- Oracle: `NVL`, `COALESCE`, `TO_CHAR`, `TO_NUMBER`, `TO_DATE`
