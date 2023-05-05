#  Databricks Data Analyst Associate

## Topic 1: Describe Databricks SQL and its capabilities

### <ins>Databricks SQL (users, benefits, queries, dashboards, compute)<ins>

**Users**: 
- Data Analyst
- Data Scientists
- BI Developers: Love working on SQL
- Report Consumers: Minimal technical barriers.

**SQL Analytics Workspace**:
- Simplified Controls
- SQL-Only
- Query Builder & Dashboarding
- Here we are expecting the data store in *Hive Tables*.
- **Check Table of Cluster Size**

**Compute: SQL Endpoint Configuration**
- Name
- Cluster Size
- Auto Stop: Default (120 minutes of inactivity)
- If the cluster is down, someone that run a query will re-launch the cluster
- Multi-Cluster Load Balancing (Min/Max)
- Photon
- Tags

*Monitoring*
- How busy my SQL Endpoint is.

**Query**
- Results can be used in a visualization.
- 1 query can create multiple visualizations.
- You can Format your Query (Ctrl+Shift+F).
- **Refresh Query Schedule**

*Query History*
- Keeping track of who is working on the SQL endpoint and which queries they created.
- Columns: Query | SQL Endpoint | Started At | Duration | User
- You can see the Spark Execution (Spark UI)

**Dashboards**
- Each visualization can be added into a Dashboard.
- Each Dashboard can have multiple queries in it and you can schedule the dashboard.
- If you want to connect your Data to an external BI tool (Power BI), you can get the Server Info from SQL Endpoint/Connection Details.

**Alerts**
- Related to a Query
- Making validation (WHEN column is ... trigger alert)
- Email Template and add Destinations

### <ins>Integrations (Partner Connect, data ingestion, other BI tools)<ins>

**Databricks Partner Connect**

**Dedicated ecosystem of integrations** that allows users to easily connect with popular data ingestion, BI partner products.

- Take less than 6 clicks to make integration.
- No context / page switches.
- **Partner API**: clusters launched automatically.
- If an account doesn't exits -> creates trial account with the technology partner
- What normally requires?: *SQL warehouse endpoint*, *service principal* and *PAT*.

*Requirements*
- Databricks account **Premium** / **Enterprise Plan**
- Databricks worskpace **E2** version
- New connection demands use the workspace admin.
- For Partnr Connect Tasks, use the workspace admin or the user who has at least the workspace access (for SQL the databricks SQL access as well).

*Steps*
1. Allow users to access partner-generated databases and table (*Data Ingestion partners*)
2. Create access token (cloud based: Partner Connect creates the token, desktop-based: use the personal access token from Databricks.)
*Only admins can make token replacement*
Recomendation: create token for service principals not for workspace users.
3. Allow SQL warehouse to access external data.

**Partners for Data Ingestion**: arcion, fivetran, hevo, rivery
**Partners for BI Tools**: Hex, Power BI, preset, sigma, tableau, thoughSpot.

### <ins>Lakehouse (medallion architecture, streaming data)<ins>

**Medallion architecture**
![](assets/DeltaLakeArchitecture.png)

a. Bronze (raw) Layer (INSERT/UPDATE from sources)
- Raw Data with long retention (unvalidated data)
- Avoid error-prone parsing
- Appended
- Batch + Streaming

b. Silver (validated) Layer (DELETE from sources)
- Some cleanup applied (validation and deduplication)
- Queryable
- Joins, filtering and aggregations.

c. Gold (enriched) Layer (MERGE/OVERWRITE)
- Cleaned data, ready for consumption
- Read with Spark or Presto

**Streaming Data**

*Delta Live Tables*
- Manages task orchestration, cluster management, monitoring, data quality and error handling.

*Delta Live Tables Datasets*
| Dataset type       | How are records processed through defined queries?                                                                                                                                                                                  |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Streaming table    | Each record is processed exactly once. This assumes an append-only source (perfect for low-latency demand).                                                                                                                                                          |
| Materialized views (Live Table) | Records are processed as required to return accurate results for the current data state. Materialized views should be used for data sources with updates, deletions, or aggregations, and for change data capture processing (CDC). |
| Views              | Records are processed each time the view is queried. Use views for intermediate transformations and data quality checks that should not be published to public datasets.                                                            |

To process a Delta Live Table Queries you must add all your SQL files into a pipeline.

<ins>Bronze Delta Live Table<ins>

```sql
CREATE OR REFRESH LIVE TABLE clickstream_raw
COMMENT "The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
AS SELECT * FROM json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`;
```

<ins>Silver Delta Live Table<ins>

```sql
CREATE OR REFRESH LIVE TABLE clickstream_prepared(
  CONSTRAINT valid_current_page EXPECT (current_page_title IS NOT NULL),
  CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Wikipedia clickstream data cleaned and prepared for analysis."
AS SELECT
  curr_title AS current_page_title,
  CAST(n AS INT) AS click_count,
  prev_title AS previous_page_title
FROM live.clickstream_raw;
```

<ins>Gold Delta Live Table<ins>

```sql
CREATE OR REFRESH LIVE TABLE top_spark_referers
COMMENT "A table containing the top pages linking to the Apache Spark page."
AS SELECT
  previous_page_title as referrer,
  click_count
FROM live.clickstream_prepared
WHERE current_page_title = 'Apache_Spark'
ORDER BY click_count DESC
LIMIT 10;
```

*Structured Streaming*
Performs the computation incremenetally and continuosly updates

## Topic 2: Manage data with Databricks tools and best practices

### <ins>Delta Lake (Basics, Benefits)<ins>

**Basics**
- Open Source SW that extends *Parquet* files with a file-based transaction log for **ACID Transactions**

*Ways to ingesting data to Delta Lake*
- Delta Live Tables
- COPY INTO
- Auto Loader
- Add Data UI
- Incrementally/One-time converstion of parquet files to Delta Lake
- Third-party partners

*Updating Delta Lake Tables*
- MERGE support
- Overwriting support

Example 1 (Replace Where clause):
```python
spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", True)

(replace_data.write
  .mode("overwrite")
  .option("replaceWhere", "start_date >= '2017-01-01' AND end_date <= '2017-01-31'")
  .save("/tmp/delta/events")
)
```

Example 2 (Dynamic Partition):
```sql
SET spark.sql.sources.partitionOverwriteMode=dynamic;
INSERT OVERWRITE TABLE default.people10m SELECT * FROM morePeople;
```

- You can manually/automatically update your table schema without rewriting data
- **Column mapping** helps to rename/delete columns without rewriting data

*Incremental and streaming workloads on Delta Lake*
- Table streaming reads and writes
- Using CDF
- Enable idempotent writes

*Query previous versions of a table*
```sql
--check table history
DESCRIBE HISTORY table;

--see a previous version
SELECT * FROM table VERSION AS OF 1;

--restore a table to a previous version
RESTORE TABLE table TO VERSION AS OF 2;
```

*Delta Lake schema enhancements*
- Delta Lake schema validations (columns must exists in target, columns data types must match, column name must match only by case)
- Constraints

```sql
CREATE TABLE people10m (
  id INT NOT NULL,
  firstName STRING,
  middleName STRING NOT NULL,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) USING DELTA;

ALTER TABLE people10m ALTER COLUMN middleName DROP NOT NULL;
ALTER TABLE people10m ALTER COLUMN ssn SET NOT NULL;
ALTER TABLE people10m ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01');
ALTER TABLE people10m DROP CONSTRAINT dateWithinRange;

--Review constraints
DESCRIBE DETAIL people10m;
SHOW TBLPROPERTIES people10m;
```

- Generated columns
```sql
CREATE TABLE events(
eventId BIGINT,
data STRING,
eventType STRING,
eventTime TIMESTAMP,
year INT GENERATED ALWAYS AS (YEAR(eventTime)),
month INT GENERATED ALWAYS AS (MONTH(eventTime)),
day INT GENERATED ALWAYS AS (DAY(eventTime))
)
PARTITIONED BY (eventType, year, month, day)
```

- Set Custom Metadata
```sql
ALTER TABLE default.people10m SET TBLPROPERTIES ('department' = 'accounting', 'delta.appendOnly' = 'true');

-- Show the table's properties.
SHOW TBLPROPERTIES default.people10m;

-- Show just the 'department' table property.
SHOW TBLPROPERTIES default.people10m ('department');
```

*Managing files and indexing data*
- Z-Order Indexing (technique to colocate related information in the same set of files).
```sql
OPTIMIZE events
WHERE date >= current_timestamp() - INTERVAL 1 day
ZORDER BY (eventType) --common query used column / high cardinality
```
- Compact data files with Optimize
```sql
OPTIMIZE delta.`/data/events`
```
It uses *Bin-packing* optimization (idempotent)
- Remove unused data with Vacuum (**default retention: 7 days**)
Be careful, it only removes data files not log files. This are deleted automatically and asynchronously after checkpoints operations (Default retentation 30 days).
```sql
VACUUM eventsTable   -- vacuum files not required by versions older than the default retention period

VACUUM '/data/events' -- vacuum files in path-based table

VACUUM delta.`/data/events/`

VACUUM delta.`/data/events/` RETAIN 100 HOURS  -- vacuum files not required by versions more than 100 hours old

VACUUM eventsTable DRY RUN    -- do dry run to get the list of files to be deleted
```

### <ins>Storage and Management (tables, databases, views, Data Explorer)<ins>

**Metastore**
Place where you can store all the metadata that define your data objects in the lakehouse.

Types:

a. *Unity Catalog Metastore*: centralized access control, **auditing**, **lineage** and **data discovery**. 
- Can be across multiple workspaces.
- Users cannot have access to the UC metastore initially (grants must added by the admin)

b. *Built-in Hive Metastore (legacy)*
- This only support **1 single catalog**
- Less centralized
- A cluster allows all users to access all data managed by the legacy metastore (unless of the *table access control* enabling).
- **Recommend: upgrade to UC**

c. *External Hive Metastore*

**Data Objects in Databricks Lakehouse**

![](assets/HirearchyTable.jpeg)

a. **Catalog**: group of databases<br/>
b. **Database** (or Schema): group of objects (tables + views + functions)
- `LOCATION` attribute define the default location for data of all tables registered.

c. **Table**: collection of rows and columns
- All tables created by default are **Delta Tables**

<ins>Table Types<ins>

c.1. Managed Table (Supports DELTA)
- **Third level of organization**
- Data stored in a new directory *in the mestastore*.
- *No need to use `LOCATION` clause*

```sql
--Examples
CREATE TABLE table_name AS SELECT * FROM another_table;
CREATE TABLE table_name (field_name1 INT, field_name2 STRING);
```

c.2. External Table (unmanaged tables)
- **Third level of organization**
- *Outside the metastore*
- `DROP TABLE` does not delete the data!
- Cloning does not move the data.
- `delta, csv, json, avro, parquet, orc, text`

```sql
-- Example 1:
CREATE TABLE table_name
USING DELTA
LOCATION '/path/to/existing/data'

-- Example 2:
CREATE TABLE table_name
(field_name1 INT, field_name2 STRING)
LOCATION '/path/to/empty/directory'

--Create table with external location
CREATE TABLE table1
    LOCATION 's3://<bucket>/<table_dir>';

--Create table with external location + storage credential
CREATE TABLE table1
    LOCATION 's3://<bucket>/<table_dir>'
    WITH CREDENTIAL <credential-name>;
```

d. **View**: saved query against one or more tables
- *Temporary View*: not registered to a schema or catalog.
    - Notebooks/Jobs: notebook/script level of scope
    - Databricks SQL: query level of scope
    - **Global Temporary Views**: cluster level

e. **Function**: logic the returns *scalar* value or *set of rows*.

**Data Explorer**
- Schema info: display schemas
- Table Details and properties: sample data, table details, table history.
    - Most frequent querys (30 days) -> must have `SELECT`, `USE SCHEMA`, `USE CATALOG` permissions.
    - Create quick query
    - Create quick dashboard
- Admin: change/view owners
- Grant/Revoke permissions
- Query History
- Manage Storage Credentials

### <ins>Security (table ownership, PII data)<ins>

**Ownership**

`GRANT` and `REVOKE`:
- Include `CREATE`,`MODIFY`, `SELECT`, `USAGE`, etc.
- Permissions can be granted to users, groups or both.
```sql
GRANT ALL PRIVILEGES ON TABLE <table_name> TO <group_name>;
```

Show Owners:

```sql
DESCRIBE TABLE EXTENDED <catalog>.<schema>.<table_name>;
DESCRIBE CATALOG EXTENDED <catalog>;
```

Transfer Ownership:
```sql
ALTER TABLE <table_name> OWNER TO <principal>;
ALTER TABLE <catalog_name> OWNER TO <principal>;
```

Dynamic Views:
```sql
-- Column Level
CREATE VIEW sales_redacted AS
SELECT
  user_id,
  CASE WHEN
    is_account_group_member('auditors') THEN email
    ELSE 'REDACTED'
  END AS email,
  country,
  product,
  total
FROM sales_raw
```

```sql
 CREATE VIEW sales_redacted AS
 SELECT
   user_id,
   country,
   product,
   total
 FROM sales_raw
 WHERE
   CASE
     WHEN is_account_group_member('managers') THEN TRUE
     ELSE total <= 1000000
   END;
```

Some functions for this examples are:
- `current_user()`
- `is_account_group_member()`: account-level group
- `is_member()`: workspace level group

**PII data**

By the way:
- GDPR stands for *General Data Protection Regulation*
- CCPA stands for *California Consumer Privacy Act*

ACID transactions allow us to locate and remove personally idenfiable information (PII).

*Data Model for compliance*
- **Pseudonymization** (Reversible tokenization of PII)

*Point Deletes*
- Data Skipping optimizations built in
- Use Z-order on fields that we use on `DELETE` operations.

## Topic 3: Use Structured Query Language (SQL) to complete tasks in the Lakehouse

### <ins>Basic SQL<ins>

**Data Types**
- SQL Data type link: https://docs.databricks.com/sql/language-manual/sql-ref-datatype-rules.html
- Check the precedence list
- Null can be promoted to any other type
- *Implicit downcasting*: casts a wider type to a narrower type (DOUBLE -> FLOAT)
- *Implicit crosscasting*: from one type family to other.

```sql
-- Example
SELECT a.date, b.product_type, sum(a.total) as total_sales
FROM marketing.sales as a
JOIN production.productions as b on a.product_id = b.product_id
WHERE b.product_type IN ('PS5 GAMES','XBOX ONE GAMES')
GROUP BY a.date, b.product_type
HAVING sum(a.total) > 10000;
```

### <ins>Complex Data<ins>

**Nested Data Objects**

```sql
-- Example 1
SELECT
    raw:title,
    RAW:production.store_name,
    raw:production.store_stock,
FROM games_data
```

```sql
-- Example 2
SELECT
    raw:title,
    raw:['TITLE'], --this won't work (return null) case sensitive
    raw:production['store_name'],
    raw:production['store_stock']
FROM games_data
```

Nested Object with arrays
```sql
SELECT
    raw:top_clients[0],
    raw:top_clients[0].client_id,
    raw:top_clients[0].client_name,
    raw:top_clients[*].client_name --array with only one field
FROM games_data
```

Casting data
```sql
-- Example 1 (simple casting)
SELECT
    raw:production.store_products[0].price::double
FROM games_data;

-- Example 2 (complex casting)
SELECT
    from_json(raw:production, 'store_name string, store_stock int, store_products array<string>')
FROM games_data;
```

**Grouping Sets**
```sql
SELECT city, car_model, sum(quantity) AS sum
FROM dealer
GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
ORDER BY city;
```

**ROLLUP**
```sql
-- like GROUP BY GROUPING SETS ((city, car_model), (city), ())
SELECT city, car_model, sum(quantity) AS sum
FROM dealer
GROUP BY city, car_model WITH ROLLUP
ORDER BY city, car_model;
```

**Cube**
```sql
-- like GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
SELECT city, car_model, sum(quantity) AS sum
FROM dealer
GROUP BY city, car_model WITH CUBE
ORDER BY city, car_model;
```

**get first and last row**
```sql
SELECT FIRST(age IGNORE NULLS), LAST(id), SUM(id) FROM person;
```

Link: https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-groupby.html

**Windows**
```sql
SELECT name, dept, RANK() OVER (PARTITION BY dept ORDER BY salary) AS rank
FROM employees;
```

Review window functions: `RANK()`, `DENSE_RANK()`, `PERCENT_RANK()`,`ROW_NUMBER()`.

Review analytical window functions: `CUME_DIST()`, `LAG()`, `LEAD()`

### <ins>SQL in the Lakehouse<ins>

**ANSI SQL**

Set the spark cluster with this parameter
`spark.sql.ansi.enabled=true`

Some operators:
- `CAST(string_col AS <ANSI SQL data type>)`
- `element_at()`
- `to_date()`
- `to_timestamp()`
- `to_unix_timestamp()`
- `unix_timestamp()`
- `try_cast()`
- `try_divide()`

`date_format()`
```sql
SELECT date_format(date '1970-01-01', 'M'); --1
SELECT date_format(date '1970-12-01', 'L'); --12
SELECT date_format(date '1970-01-01', 'd MMM'); --1 Jan
```

`from_unixtimestamp()`
```sql
SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss'); --1969-12-31 16:00:00
```

`to_unix_timestamp()`
```sql
SELECT to_unix_timestamp('2016-04-08', 'yyyy-MM-dd'); --1460098800
```

`unix_timestamp()`
```sql
SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd'); --1460041200
```

`timestamp()`
```sql
SELECT timestamp('2020-04-30 12:25:13.45'); --2020-04-30 12:25:13.45
```

More Built-in functions: https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html

**Lambda Functions**
```sql
SELECT array_sort(array(5, 6, 1),
                (left, right) -> CASE WHEN left < right THEN -1
                                      WHEN left > right THEN 1 ELSE 0 END);
```

Keep in mind that for array handling are many methods: 
`array_append`, `array_compact`, `array_distinct`, `array_except`, `array_intersect`, `array_remove`, `array_union`, `sort_array`

**User Defined Functions (UDF)**

```sql
CREATE FUNCTION convert_f_to_c(unit STRING, temp DOUBLE)
RETURNS DOUBLE
RETURN CASE
  WHEN unit = "F" THEN (temp - 32) * (5/9)
  ELSE temp
END;

SELECT convert_f_to_c(unit, temp) AS c_temp
FROM tv_temp;
```

**Query History and Query Profile**

- *Query History* (in the sidebar): we can see the execution summary. We can cancel a query if it's running.
- *Query Profile*: execution details. **Not available for the query cache**
    - We can see a *tree view* or *graph view*
    - Most common operations in a query execution plan:
        - *Scan*: Data was read from a datasource and output as rows.
        - *Join*
        - *Union*: 
        - *Shuffle*: Data was redistributed or repartitioned.
        - *Hash / Sort*: Rows were grouped by a key and evaluated using an aggregate function such as SUM, COUNT, or MAX within each group.
        - *Filter*:
        - *(Reused) Exchange*: A Shuffle or Broadcast Exchange.
        - *Collect Limit*: The number of rows returned was truncated by using a LIMIT statement.
        - *Take Ordered And Project*: The top N rows of the query result were returned.
- *Query Caching*:
    - Local cache: comes from the cluster. If it is restarted or stopped the cache will be cleaned.
    - Remote result cache: serverless-only cache system that persist the query results in a cloud storage (lifecycle 24 hours)
    - Delta caching: local SSD caching.

## Topic 4: Create production-grade data visualizations and dashboards

# Coming Soon!
