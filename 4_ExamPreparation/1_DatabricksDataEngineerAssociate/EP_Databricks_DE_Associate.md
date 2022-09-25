#  Databricks Data Engineer Associate
#DataEng_Roadmap/Databricks

## Topic 1: Understand how to use and the benefits of using the Databricks Lakehouse Platform and its tools
### 1 What is the *Databricks Lakehouse*?

- ACID transactions + data governance (from DWH).
- Enables BI + Machine Learning

#### 1.1 Primary Components
- **Delta tables**
    - Databricks uses the Delta Lake protocol **by default**
    - **When a Delta Table is created**:
        - Metadata is added to the *metastore* (inside the declared schema or database).
        - *Data* and *table metadata* saved to a directory.
    - All Delta Tables **have**:
        - A *Directory* containing data in **Parquet**
        - A *Sub-directory* `/_delta_log` for metadata (table versions in **Json** and **Parquet**)
- **Metastore** (is optional)
    - We can interact with tables without metastore by using **Spark APIs**

Other topics related to *Delta Tables*:
- ACID Transactions
- Data Versioning
- ETL
- Indexing

#### 1.2 Data Lakehouse vs Data Warehouse vs Data Lake
| Data Warehouse  | Data Lake  | Data Lakehouse  |
|---|---|---|
| Clean and structured data for BI Analytics  | Not for BI reporting due to its unvalidated nature  |  Low query latency and high reliability for BI/A |
|  Manage property formats | Stores data of any nature in any format  | Deals with many standard data formats  |
|   |   | ++ Indexis protocols optimized for ML and DS |

### 2 Data Science and Engineering Workspace

- For Data Analyst people -> you can use **Databricks SQL persona-based environment**

#### 2.1 Workspace
- Organize objects like *notebooks*, *libraries*, *experiments*, *queries* and *dashboards*.
- Provides access to *data*
- Provides computational resources like *clusters* and *jobs*
- Can be managed by the **workspace UI** or **Databricks REST API reference**
- You can switch between workspaces.
- You can view the **new** databricks SQL queries, dashboards and alerts. **BUT**, to view the existing ones you need to **migrate** them into the wkspace browser

#### 2.2 Clusters (Databricks Computer Resource)
- Provide unified platform for many use cases: *production ETL*, *pipelines*, *streaming analytics*, *ad-hoc analytics* and *ML*.

- **Cluster Types**:
    - <ins>*All-purpose clusters*</ins>: the user can manually terminate and restart them. **Multiple users** can share such clusters (*collaborative interactive analysis*)
    - <ins>*Job clusters*</ins>: dealed by the *Databricks job scheduler*
    
- Databricks can retain cluster config for *up to 200 all-purpose clusters* terminated in the last **30 days**, and *up to 30 job clusters* **recently terminated** (you can pin a cluster to the cluster list).

##### Creating a Cluster
- There are to types of cluster while you create it -> Single and Multiple Node
- Photon Accelerator -> accelerate Spark workload and reduce the total cost/workload
- Termination -> default after 120 min of inactivity (can be disabled)

##### Editing a Cluster
- You cannot change if it was Single or Multiple Node
- If you made at least 1 change, the cluster must be restarted

##### Restart, Terminate and Delete a Cluster
- They all start with a **cluster termination event** (also there is a *automatic termination* due inactivity)
- Cluster terminates -> VMs/Ops Memory purged, Attached Volume deleted, Networks between nodes removed
- **Restart** -> to clear cache or to reset compute environment.
- **Terminate** -> *stop* but maintain same configurations so we can use **Restart** button to set new cloud resources.
- **Delete** -> stop our cluster and remove the configurations.

#### 2.3 Notebooks

##### Attach to a cluster
- Remember that a notebook provides **cell-by-cell execution of code**
- Multiple languages can be mixed inside a notebook.

##### Running a Cell
`CTRL+ENTER` or `CTRL+RETURN`
`SHIFT+ENTER` or `SHIFT+RETURN` -> to run the cell and move to the next one

##### Setting the Default Notebook Language
- Databricks notebooks support Python, SQL, Scala and R.
- You can set a PL once you are creating the notebook but can be changed at any time.
- Python is the default language. If you change it, you will see on the top of each python cell the command `%python`

##### Create a new cell with `B` key

##### Magic Commands
- Identified by `%` character
- Only 1 magic command per cell and must be the first thing in a cell.
- **Language Magics** -> `%sql`, `%python`
- **Markdown** -> for markdown language
- **Running command** -> `%run` this is to run a notebook from another notebook (its temp views and local declarations are going to be part of the calling notebook)
	- `%run ../Includes/Classroom-Setup-0.12`

##### Databricks Utilities (`dbutils docs`)
```python
%python
#Obtain the list of files of a path
dbutils.fs.ls(path)

#Display the list of files of a path
files = dbutils.fs.ls(path)
display(files)
```
The `display()` has the following considerations:
- Rendering plots
- Maximum 1000 records
- Has an option to download the result as a CSV file

##### Downloading Notebooks
1. Download a notebook
2. Download a **collection** of notebooks (using Repos)

##### Clearing Notebook States
* **Clear** menu and select **Clear State & Clear Outputs**

### 3 Delta Tables

##### 3.1 Creating a Delta Table
```sql
CREATE TABLE IF NOT EXISTS students
	(id INT, name STRING, value DOUBLE);
```

##### 3.2 Inserting Data (`COMMIT`is not required)
```sql
INSERT INTO students VALUES (1, "Yve", 1.0);
--Inserting multiple rows in 1 INSERT
INSERT INTO students
VALUES
	(4, "Ted", 4.7),
	(5, "Tiffany", 5.5),
	(6, "Vini", 6.3)
```

##### 3.3 Querying a Delta Table
```sql
SELECT * FROM students
```
* Every `SELECT`will return the **most recent version of the table**
* Concurrent reads is limited only the **limitations** of object storage (depending on the cloud vendor).

##### 3.4 Updating Records (1st snapshot 2nd update)
```sql
UPDATE students
SET value = value + 1
WHERE name LIKE "%T"
```

##### 3.5 Deleting Records
```sql
DELETE students 
WHERE value > 6
```
* If you delete the entire table, you will see -1 as a result of the numbers of rows affected. This means an entire directory of data has been removed

##### 3.6 Merge Records
```sql
MERGE INTO students s
USING students_updated u
ON s.id = u.id
WHEN MATCHED AND u.type = "update"
	THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
	THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
	THEN INSERT *
```

##### 3.7 Dropping Table
```sql
DROP TABLE students
```

##### 3.8 Examining Table Details (Using the **Hive metastore**)
```sql
-- Show important metadata about our table (columns and partitioning)
DESCRIBE EXTENDED students

-- Another command is:
DESCRIBE DETAIL students
/* You can see the following information
* 	 format
* 	 table id
* 	 name & description
* 	 location
* 	 created and last modification date
* 	 partition columns, # files, size, properties
* 	 minReaderVersion and minWriterVersion
*/

-- To see the versions of a table:
DESCRIBE HISTORY students
/* You can see the following information
* 	 version #
* 	 userId and userName
* 	 operation: MERGE, DELETE, WRITE, ETC
* 	 operationParameters
*  and so on...
*/
```

##### 3.9 Explore Delta Lake FILES
```python
%python
display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))
```
* Records are stored in **parquet** files
* There is an additional directory called `_delta_log`
* This directory contains the transactions against the table.
* Even the main directory could hold more parquet files than `numFiles` indicates, the `_delta_log`helps us to know which parquet are valid
```python
# show the delta log Json file for 1 transaction
display(spark.sql(f"SELECT * FROM
json.`{DA.paths.user_db}/students/_delta_log/0000000000007.json`"))
'''
The result displays 2 important columns:
- add: list of all new files written
- remove: files that no longer should be included
'''
```

##### 3.10 Compacting Small Files and Indexing
* `OPTIMIZE` command helps us to combine records and rewriting results 
* We can **optionally** specify the field(s) for `ZORDER`indexing.
```sql
OPTIMIZE students
ZORDER BY id

-- To query a previous version of the table
SELECT * FROM students
VERSION AS OF 3
```

##### 3.11 Rollback Versions
```sql
RESTORE TABLE students TO VERSION AS OF 8
```

##### 3.12 Purge Old Data Files
```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN
--DRY RUN: to print out all records to be deleted
```

## Topic 2: Build ETL pipelines using Apache Spark SQL and Python
### 1. Relational Entities

#### 1.1 Creating a Schema
```sql
-- Method 1: without a location
CREATE SCHEMA IF NOT EXISTS ${da.db_name}_default_location;
-- It will store under dbfs:/user/hive/warehouse (with .db extension)

CREATE SCHEMA IF NOT EXISTS ${da.db_name}_custom_location LOCATION ‘${da.paths.working_dir}/_custom_location.db’;
```

You can check the information of the schema with `DESCRIBE SCHEMA EXTENDED …`

#### 1.2 Example using a schema
```sql
USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE example_table (width INT, length INT, height INT);

DESCRIBE DETAIL example_table;
/* 
it will store in:
dbfs:/user/hive/warehouse/schema_name.db/example_table
*/
```

Displaying the list of files of the table
```python
hive_root 	= f”dbfs:/user/hive/warehouse”
db_name		= f”{DA.db_name}”_default_location.db”
table_name 	= f”example_table”

tbl_location = f”{hive_root}/{db_name}/{table_name}”
print(tbl_location)

files 		= dbutils.fs.ls(tbl_location)
display(files)
```

If you **drop the table** the files will be deleted by the **schema remains**.

### 2. Tables
First, we need to know the difference between *managed* and *unmanaged* tables
- **Unmanaged Tables**: 
	- Spark only manages the metadata and we control the data location
	- A table is considered unmanaged if we add `path` option
	- If we drop the table, **only the metadata will be dropped**
	- Global -> available across all clusters
- **Managed Tables**:
	- Spark manages both data and metadata.
	- Global -> available across all clusters
	- If we drop the table, **both data and metadata will be dropped**

```sql
USE ${da.db_name}_default_location

-- external table
CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
	path 	= ‘${DA.paths_datasets}/flights/departuredelays.csv’,
	header	= ‘true’,
	mode 	= “FAILFAST” —-abort file parsing with RuntimeException  
);

CREATE OR REPLACE TABLE external_table LOCATION ‘{da.paths.working_dir}/external_table’ AS SELECT * FROM temp_delays;
```

- You can uso `SHOW TABLES` to list all tables

#### 2.1 Views
```sql
CREATE VIEW view_delays_abq_lax AS
	SELECT *
	FROM external_table
	WHERE origin = ‘ABQ’ AND destination = ‘LAX’;
```
- The view will appear in the `SHOW TABLES` result.

```sql
-- TEMPORARY VIEWS
CREATE TEMPORARY VIEW temp_view_delays
AS
SELECT * 
FROM external_table 
WHERE delay > 120 ORDER BY delay ASC;
```
- `SHOW TABLES` will show that the temporary view has the isTemporary flag as true but is not assigned to a database.

```sql
-- GLOBAL VIEWS
CREATE GLOBAL TEMPORARY VIEW temp_view_delays_gt
AS
SELECT * 
FROM external_table 
WHERE distance > 1000;
```
- To display the global views, you should type `SHOW TABLES in global_temp` because all global views are stored in the `global_temp` database.

**Considerations**:
- Temp vies are **tied** to a Spark Session, that means that are not accessible:
	- After restarting a cluster
	- After detaching and reattaching a cluster
	- After installing a python package (python interpreter restarts)
	- Using another notebook
- For **global** views, the cluster holds the `global_temp` database .

#### 2.2 Common Table Expression (CTEs)
```sql
-- Example 1
WITH temp_table (
	temp_column_1,
	temp_column_2,
	temp_column_3,
) AS (
	SELECT
		column_1,
		column_2,
		column_3
	FROM 
		source_table
)
SELECT *
FROM
	temp_table
WHERE
	temp_column_1 > 1;
```

```sql
-- Example 2
WITH final_temp_table AS
(
	WITH temp_table (
		temp_column_1,
		temp_column_2,
		temp_column_3,
	) AS (
	SELECT
		column_1,
		column_2,
		column_3
	FROM 
		source_table
	)
	SELECT *
	FROM
		temp_table
	WHERE
		temp_column_1 > 1
)
SELECT count(temp_column_1) AS ‘Total’ FROM final_temp_table;
```

```sql
-- Example 3
SELECT max(temp_column_1), min(temp_column_1)
FROM
(
	WITH temp_table (temp_column_1) AS (
	SELECT
		column_1
	FROM 
		source_table
	)
	SELECT temp_column_1 FROM temp_table
)
```

```sql
-- Example 4
SELECT
	(
		WITH distinct_source_table AS (
			SELECT DISTINCT origin_column FROM source table
		)
		SELECT
			count(origin_column) as ‘Total’
		FROM
			distinct_source_table
	)	AS ‘Number’
```

```sql
-- Example 5 (with CREATE VIEW)
CREATE OR REPLACE VIEW view_table
AS 
WITH origin_table(temp_column_1, temp_column_2)
	AS (SELECT column_1, column_2 FROM source_table)
	SELECT * FROM source_table
	WHERE column_1 > 100;
```

### 3. ETL Processes

#### 3.1 Querying Files
```sql
-- Querying a Single File
SELECT * FROM file_format.`/path/to/file`;

/* 
* Querying a Directory
* Consideration: the directory must have files with the same
* format and schema
*/
SELECT * FROM file_format.`/path/to/`
```
- You can read json files put in `file_format` as `json`
- There are some cases when the files lacks of standardization, so reading the files as json would not fit. In that case we can use `text` as the file format.
- If you are dealing with images or unstructured data, you can use `binaryFile` as file format. 

#### 3.2 Creating References to Files
```sql
CREATE OR REPLACE TEMP VIEW temp_view
AS SELECT * FROM json.`/path/to/`;
```

#### 3.3 Registering Tables on External Data with Read Options
```sql
CREATE TABLE sales_csv
(order_id LONG, email STRING, transactions_timestamp LONG)
USING CSV
OPTIONS (
	header 		= "true",
	delimiter	= "|"
)
LOCATION "${DA.paths.sales_csv}"
```

You can specify the following:
- The column types
- The file format
- The delimiter used
- The presence of a header
- The location of the files

*There is no data movement*
- The metadata and options passed during table declaration will be persisted to the metastore.
- It’s **important** to not change the column order (there’s no schema enforcement)
- To refresh the cache of our data, we can use `REFRESH TABLE table_name`

#### 3.4 Extracting Data from SQL Databases
```sql
CREATE TABLE table_jdbc
USING JDBC
OPTIONS (
	url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
	dbtable = "table",
	user = "user_name",
	password = "password"
)
```
- The table has been listed as `MANAGED` so there is no data stored in the dbfs.
- Could exist some significant overhead because of either:
	- Network transfer latency
	- Execution of query logic in source systems not optimized for big data queries.
	
#### 3.5 Creating Delta Tables
```sql
-- as SELECT
CREATE OR REPLACE TABLE sales AS
SELECT * FROM
parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`
-- Here we are using a infer schema information

-- When the infer schema is not correctly processed
CREATE OR REPLACE TEMP VIEW sales_temp_view
(order_id LONG, email STRING, transactions_timestamp LONG, ...)
USING CSV
OPTIONS (
	path 		= "${da.paths.datasets}/ecommerce/raw/sales-csv",
	header 		= "true",
	delimiter	= "|"
);

CREATE TABLE sales AS
	SELECT * FROM sales_temp_view;
```

```sql
-- Renaming columns
CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;
```

```sql
-- Declaring schema and making comments
CREATE OR REPLACE TABLE purchase_date (
	id STRING,
	transaction_timestamp LONG,
	price STRING,
	date DATE GENERATED ALWAYS AS (
		cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS 	DATE))
	COMMENT "generated based on `transactions_timestamp` column"
	) 
)
```

```sql
-- Adding constraints (2 types: NOT NULL and CHECK)
ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');
-- This is store in the TBLPROPERTIES field
```

```sql
-- A more complex example
CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
	SELECT *,
		cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP)
		AS DATE) first_touch_date,
		current_timestamp() updated,
		input_file_name() source_file
	FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
```

#### 3.6 Cloning Delta Lake Tables
```sql
-- Option 1: DEEP CLONE (copies data and metadata, this is incremental)
CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases
```

```sql
-- Option 2: SHALLOW CLONE (copies Delta transactions logs)
CREATE OR REPLACE TABLE purchases_clone
SHALLOW CLONE purchases
```

#### 3.7 Writing to Delta Tables

##### Overwriting
- Some benefits of **overwriting** data:
	- Is much faster because you will not list the directories recursively.
	- You can use *Time Travel* to retrieve old data.
	- It’s an atomic operation -> concurrent queries can read the data while you are deleting the table
	
```sql
-- Option 1: Using CRAS
CREATE OR REPLACE TABLE table AS
SELECT * FROM parquet.`path/file`;

-- If you want to check the previous states you can use:
DESCRIBE HISTORY table;

-- CRAS helps us to redefine the structure of the table
```

```sql
-- Option 2: Using INSERT OVERWRITE
INSERT OVERWRITE table
SELECT * FROM parquet.`path/file`;

-- If you want to check the previous states you can use:
DESCRIBE HISTORY table;

-- It will fail if we try to change the schema of the table
```

##### Appending
```sql
INSERT INTO table
SELECT * FROM parquet.`path/file`;
```

##### Merging Updates
```sql
MERGE INTO table a
USING source b
ON {merge_condition}
WHEN MATCHED THEN {matched_action}
WHEN NOT MATCHED THEN {not_matched_action}
```
- Common use case: to insert not duplicate records.

##### Copying incrementally
```sql
COPY INTO sales
FROM `path/file`
FILEFORMAT = PARQUET;
```

#### 3.8 Cleaning Data

##### Inspecting Data
```sql
-- Using count
SELECT 
	count(column_1) AS count_column_1,
	count(column_2) AS count_column_2,
	count_if(column_1 IS NULL) AS count_if_column_1
FROM
	table_name;

-- Using distinct
SELECT count(DISTINCT(*)) FROM	table_name;
SELECT count(DISTINCT(column_1)) FROM	table_name;
```

```sql
-- Validation of datasets (similar like CASE WHEN)
SELECT max(row_count) <= 1 no_duplicate_id FROM (
	SELECT column_1, count(*) AS row_count
	FROM table_name
	GROUP BY column_1
);
```

##### Date Format and Regex
```sql
SELECT *,
	date_format(date_column, “MMM d, yyyy”) AS formatted_date,
	date_format(date_column, “HH:mm:ss”) as formatted_time,
	regexp_extract(email_column, “(?<=@).+”,0) as email_domain
FROM (
	SELECT *,
		CAST(date_column_num / 1e6 AS timestamp) AS date_column
	FROM table_name
);
```

#### 3.9 Advanced SQL Transformations

##### Casting to string: for cases of having binary-encoded JSON values
```sql
CREATE OR REPLACE TEMP VIEW events_string AS
SELECT string(key), string(value)
FROM events_raw;
```

##### For data stored as string but has a dictionary format
```sql
SELECT value:element1, value: element2
FROM events_string;
```
You can use it also on the `WHERE` statement.

##### Using `schema_on_json`
```sql
CREATE OR REPLACE TEMP VIEW parsed_events AS
SELECT 
from_json(original_column,schema_on_json('{
"device":"MacOS",
"ecommerce":{
	"purchase_revenue_in_usd": 1075.5,
	"total_item_quantity": 1,
	"unique_items": 1,
},
"items":[
{"coupon":"NEWBED10", "item_id":"M_STAN_K","price_in_usd":29.9}
],
"user_id": "123456"
}')) AS json
FROM events_string;

-- You can read the info like this
CREATE OR REPLACE TEMP VIEW parsed_events_final AS
SELECT 
	json.*
FROM
	parsed_events;
```
- This helps to provide a schema to a json column that has some sub-elements empty and you can define a sub-schema for them.

##### Exploring Data Structures

**Struct**
```sql
-- Reading fields from a STRUCT field
SELECT 
	ecommerce.purchase_revenue_in_usd,
	ecommerce.total_item_quantity
FROM parsed_events_final
WHERE
	ecommerce.purchase_revenue_in_usd > 199.99;
```

**Arrays**
```sql
-- Reading fields from ARRAY field
SELECT *
FROM parsed_events_final
WHERE size(items) > 2;

-- Using Explode
SELECT
	user_id,
	device,
	explode(items) AS item
FROM parsed_events_final
WHERE size(items) > 2;
```

*Remember*:
- `collect_set`: collect unique values for a field.
- `flatten`: multiple arrays can be combined into a single array
- `array_distinct`: removes duplicate elements from an array
*Use them on aggregations*

```sql
SELECT
	user_id,
	collect_set(event_name) as event_history,
	array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM parsed_events_final
GROUP BY
	user_id
```

##### Join Tables
```sql
CREATE OR REPLACE VIEW sales_enriched AS
SELECT *
FROM (
	SELECT *, explode(items) AS item
	FROM sales
) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;
```

##### Set Operators
Not too much to mention, if you have some experience with SQL you may know `UNION`, `INTERSECT` and `MINUS`.

##### Pivot Tables
```sql
CREATE OR REPLACE TABLE transaction AS
SELECT * FROM (
	SELECT
		email,
		order_id,
		transaction_timestamp,
		item.item_id AS item_id,
		item.quantity AS quantity
	FROM sales_enriched
)	PIVOT (
	sum(quantity) FOR item_id IN (
		'P_FOAM_K',
		'M_STAN_Q',
		'P_FOAM_S',...
	)
)
);
```

##### Higher Order Functions
**Filter**
Given a lambda function, we can make some filters inside an array
```sql
SELECT
	order_id,
	items,
	FILTER(items, i -> i.item_id LIKE "%K") AS king_items
FROM sales
```

**Transform**
Given a lambda function,  transform all elements in an array
```sql
CREATE OR REPLACE TEMP VIEW king_item_revenues AS
SELECT
	order_id,
	TRANSFORM (
		items,
		k -> CAST(k.item_reveneu_in_usd * 100 AS INT)
	) AS item_revenues
FROM king_size_sales;
```

Other operations that were not covered (with examples) on the Databricks Learning Notebooks are `reduce` and `exists`.

#### 3.10 SQL UDFs
```plsql
CREATE OR REPLACE FUNCTION yelling(text STRING)
RETURNS STRING
RETURN concat(upper(text),"!!!")

SELECT yelling(food) FROM foods;
```

- SQL UDFs persists between execution environments (notebooks, DBSQL queries and jobs).
- You can view a detail of the function with `DESCRIBE FUNCTION function`.
- To see the body of the function you can use `DESCRIBE FUNCTION EXTENDED function`.
- To use SQL UDF, **a user must have `USAGE`and `SELECT` permissions**.

```plsql
CREATE FUNCTION foods_i_like(food STRING)
RETURNS STRING
RETURN CASE
	WHEN food = "beans" THEN "I love beans"
	...
	ELSE concat("I don't eat ", food)
END;
```

### 3. Some Functions with Python
```python
# Creating a database
def create_database(name_db, reset_db=True):
	import re # to clean the username
	username = spark.sql("SELECT current_user()").first()[0]
	clean_username = re.sub("[^a-zA-Z0-9]","_",username)
	db_name_final = f"{clean_username}_{name_db}"
	working_dir = f"dbfs:/user/{username}/{name_db}"
	
	# displaying the info
	print(f"username: 		{username}")
	print(f"dbname: 		{db_name_final}")
	print(f"working_dir: 	{working_dir}")

	# if reset is True we drop the DB an re-create it
	if reset:
		spark.sql(f"DROP DATABASE IF EXISTS {db_name_final} CASCADE")
		dbutils.fs.rm(working_dir, True)
	
	spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name_final} LOCATION '{working_dir}/{db_name_final}.db'")
	spark.sql(f"USE {db_name_final}")

# calling the function
create_database("luism_database")
```

```python
# Python method for displaying query results

def query_or_make_demo_table(table_name):
	try:
		display(spark.sql(f"SELECT * FROM {table_name}"))
		print(f"Displayed results for the table {table_name}")
	except:
		spark.sql(f"CREATE TABLE {table_name} (id INT, name STRING, value DOUBLE, state STRING")
		spark.sql(f"""INSERT INTO {table_name}
					   VALUES (....)
					""")
		display(spark.sql(f"SELECT * FROM {table_name}"))
		print(f"{table_name} created") 
```