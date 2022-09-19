#  Databricks Data Engineer Associate
#databricks #exampreparation #dataengineer

## Topic 1: *Understand how to use and the benefits of using the Databricks Lakehouse Platform and its tools*
- - - -
### 1. What is the *Databricks Lakehouse*?

- ACID transactions + data governance (from DWH).
- Enables BI + Machine Learning

#### <ins>Primary Components</ins>
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

#### <ins>Data Lakehouse vs Data Warehouse vs Data Lake</ins>
| Data Warehouse  | Data Lake  | Data Lakehouse  |
|---|---|---|
| Clean and structured data for BI Analytics  | Not for BI reporting due to its unvalidated nature  |  Low query latency and high reliability for BI/A |
|  Manage property formats | Stores data of any nature in any format  | Deals with many standard data formats  |
|   |   | ++ Indexis protocols optimized for ML and DS |

### 2. Data Science and Engineering Workspace

- For Data Analyst people -> you can use **Databricks SQL persona-based environment**

#### <ins>Workspace</ins>
- Organize objects like *notebooks*, *libraries*, *experiments*, *queries* and *dashboards*.
- Provides access to *data*
- Provides computational resources like *clusters* and *jobs*
- Can be managed by the **workspace UI** or **Databricks REST API reference**
- You can switch between workspaces.
- You can view the **new** databricks SQL queries, dashboards and alerts. **BUT**, to view the existing ones you need to **migrate** them into the wkspace browser

#### CLUSTERS (Databricks Computer Resource)
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

#### NOTEBOOKS

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

### 3. Delta Tables

##### 1. Creating a Delta Table
```sql
CREATE TABLE IF NOT EXISTS students
	(id INT, name STRING, value DOUBLE);
```

##### 2. Inserting Data (`COMMIT`is not required)
```sql
INSERT INTO students VALUES (1, "Yve", 1.0);
--Inserting multiple rows in 1 INSERT
INSERT INTO students
VALUES
	(4, "Ted", 4.7),
	(5, "Tiffany", 5.5),
	(6, "Vini", 6.3)
```

##### 3. Querying a Delta Table
```sql
SELECT * FROM students
```
* Every `SELECT`will return the **most recent version of the table**
* Concurrent reads is limited only the **limitations** of object storage (depending on the cloud vendor).

##### 4. Updating Records (1st snapshot 2nd update)
```sql
UPDATE students
SET value = value + 1
WHERE name LIKE "%T"
```

##### 5. Deleting Records
```sql
DELETE students 
WHERE value > 6
```
* If you delete the entire table, you will see -1 as a result of the numbers of rows affected. This means an entire directory of data has been removed

##### 6. Merge Records
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

##### 7. Dropping Table
```sql
DROP TABLE students
```

##### 8. Examining Table Details (Using the **Hive metastore**)
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

##### 9. Explore Delta Lake FILES
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

##### 10. Compacting Small Files and Indexing
* `OPTIMIZE` command helps us to combine records and rewriting results 
* We can **optionally** specify the field(s) for `ZORDER`indexing.
```sql
OPTIMIZE students
ZORDER BY id

-- To query a previous version of the table
SELECT * FROM students
VERSION AS OF 3
```

##### 11. Rollback Versions
```sql
RESTORE TABLE students TO VERSION AS OF 8
```

##### 12. Purge Old Data Files
```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN
--DRY RUN: to print out all records to be deleted
```