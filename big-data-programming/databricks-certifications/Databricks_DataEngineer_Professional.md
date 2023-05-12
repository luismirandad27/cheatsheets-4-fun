#  Databricks Data Engineer Professional Cheatsheet (In Progress)

## **Topic 1: Understand how to use and the benefits of using the Databricks platform and its tools**

<h3 style="font-weight:bold;"> a. Platform </h3>
<h4 style="font-style:italic;"> a.1. Notebooks </h4>

<ins>Capabilities<ins>

- Programming Languages: *Python*, *SQL*, *Scala* and *R*.
- Export as `.html` or `.ipynb`.
- Environment customization.
- Scheduled jobs with multi-notebook workflows.
- Integration with Git-based repository.
- Build/Share dashboards
- Run **Delta Live Tables** pipeline

<ins>Notebooks Management<ins>

**Link**: https://docs.databricks.com/notebooks/notebooks-manage.html

- New(+) / Notebook
- For Control Access: **Premium Plan or above** (Workspace access level)
    - No Permissions
    - Can Read -> View Cells + Comment + use `%run`/workflows
    - Can Run -> Attach/Detach
    - Can Edit -> Edit Cells
    - Can Manage -> Change Permissions
- To review Notebooks attached to a cluster: **Cluster Page**/*Notebooks* tab

<ins> Coding in Notebooks<ins>

**Link**: https://docs.databricks.com/notebooks/notebooks-code.html

- You can change the default language
- You can override the language to execute on each cell.
- *Some magic commands:*
    - `%sh`: run shell code (`-e` to fail a cell if non-zero exit status)
    - `%fs`: to use the `dbutils` filesystem commands
    - `%md`: for markdown
- A SQL result are automatically available as Python DataFrame (`_sqldf`)
    - Databricks Runtime 13.0 (and above) support **IPython's output caching system**

<ins>Notebook Outputs<ins>

**Link**: https://docs.databricks.com/notebooks/notebook-outputs.html

- Results maximum 10,000 rows (2 MB)

<ins>Scheduled Notebook Jobs<ins>

**Link**: https://docs.databricks.com/notebooks/schedule-notebook-jobs.html

- If you don't allow the creation of a new cluster after triggering job, Databricks will used the current cluster the notebook is attached to.

<ins>Collaborate using Databricks notebooks<ins>

**Link**: https://docs.databricks.com/notebooks/notebooks-collaborate.html

<ins>Export/Import Notebooks<ins>

- Source File: .scala, .py, .sql, .r
- HTML
- .dbc (Databricks archive)
- Jupyter notebook (.ipynb)
- RMarkdown (.Rmd)

<ins>Test Databricks notebooks <ins>

**Link**: https://docs.databricks.com/notebooks/test-notebooks.html

- You can use the built-in Python `unittest` package
- Using Databricks widgets
- Schedule tests to run automatically (with email notification)
- Using Databricks Repos:
Example:

```python
%sh

pwd
ls
python -m unittest -v shared_test.py
```

<ins>Python environment management<ins>

**Link**: https://docs.databricks.com/libraries/index.html#notebook-scoped-libraries

<ins>Databricks Widget<ins>

**Link**: https://docs.databricks.com/notebooks/widgets.html

*4 Types:*
- `text`: input a value in a text box.
- `dropdown`
- `combobox`
- `multiselect`

```python
dbutils.widgets.dropdown("database", "default", [database[0] for database in spark.catalog.listDatabases()])
dbutils.widgets.text("table", "")
```

```sql
SHOW TABLES IN ${database};

SELECT *
FROM ${database}.${table}
LIMIT 100;
```

```python
# Get a widget variable
dbutils.widgets.get("database")

# Remove widget variable
dbutils.widgets.remove("database")
dbutils.widgets.removeAll()
```

*Considerations*:
- Cannot use widgets to pass arguments between different languages (only when you click on Run All).

*Configure Widget Settings*:
- **Run Notebook**: new value, entire notebook is rerun.
- **Run Accessed Commands**: new value, cells that return that widget are rerun
- **Do Nothing**

*Running a Notebooks with widgets*
```bash
%run /path/to/notebook $X="10" $Y="1"
```

<ins>Run a Notebook from another notebook<ins>

**Link**: https://docs.databricks.com/notebooks/notebook-workflows.html

```bash
%run ./shared-code-notebook
```
or
```python
# on calle notebook
dbutils.notebook.exit("returnValue")

# on caller notebook
returnValue = dbutils.notebook.run("notebook-name",60, {"argument":"data",...})
```
fancy way to handle errors
```python
# Errors throw a WorkflowException.

def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries > max_retries:
        raise e
      else:
        print("Retrying error", e)
        num_retries += 1

run_with_retry("LOCATION_OF_CALLEE_NOTEBOOK", 60, max_retries = 5)
```

<h4 style="font-style:italic;"> a.2. Clusters </h4>

**Link**: https://docs.databricks.com/clusters/configure.html

<ins>Cluster Access Mode (Not supported in Clusters API)<ins>

- Single User
- Shared
    - credential passthrough is not supported.
    - spark-submit not supported
    - cannot use UDFs
- No Isolation Shared
- Custom 

<ins>Cluster Node Type<ins>

- Driver Node
    - maintains state information of all notebooks attached
    - maintains the **SparkContext**
    - coordinates with the Spark executors.
- Worker Node

<ins>Autoscalling<ins>

- Not available for `spark-submit`.
- Customize the "scale down" with `spark.databricks.aggressiveWindowDownS`

<ins>Other considerations<ins>

- *After editing a cluster*, all the notebooks/jobs remain attached.
- *After editing a cluster*, libraries remain installed.
- Only **running**/**terminated** clustes can be edited.
- *Cloning a cluster* doesn't clone the **cluster permissions**,**installed libraries** and **attached notebooks**.
- *When restarting a cluster*, Databricks re-creates the cluster with the same ID, automatically installs the libraries and reattaches the notebooks.
- *Cluster information* through **Apache Spark UI**
    - Cluster logs (event logs (60 days retention), driver/worker log (**Standard output**, **standard error**, **Log4j logs**), init-script logs)
    - Monitoring performance: *Ganglia* (refreshed every 15 minutes) or *Datadog*

<ins>Cluster features<ins>

- 2 types: *All-purpose* and *job* clusters
    - *All-purpose*: shared by multiple users.
    - *Job*: terminate when your job ends.
- Cluster modes: *Multi Node* and *Single Node*
- Pools: reduce cluster start and scale-up by maintaining a set of available instances (reduce cost).

<h4 style="font-style:italic;"> a.3. Jobs </h4>

**Link**: https://docs.databricks.com/workflows/index.html#what-is-databricks-jobs

- You can create and run jobs using **Jobs UI**, **Databricks CLI** or by invoking **Jobs API**.
- Create jobs only in **Data Science & Engineering** or **Machine Learning** workspace.
- 1 workspace = 1000 CONCURRENT task runs. (`429 Too Many Requests`).
- 1 workspace can create 10'000 jobs in an hour.
- 1 job = group of tasks
- You can run:
    - Notebooks
    - JARS
    - Delta Live Table pipelines
    - Python, Scala, Spark Submit
    - Java Applications
    - Databricks SQL queries, alerts and dashboards
- Run jobs with **job cluster**, **all-purpose cluster** or **SQL warehouse**.

<h4 style="font-style:italic;"> a.4. Databricks SQL </h4>

*Please review the Databricks Certified Data Analyst Associate Cheatsheet*.

<h4 style="font-style:italic;"> a.5. Relational Entities </h4>

Useful Link: https://faun.pub/relational-entities-on-databricks-2a46c31518ce

- Referring to `Databases`, `Tables` and `Views`.

*Please review the Databricks Certified Data Analyst Associate Cheatsheet*.
(**Storage and Management**)

<h4 style="font-style:italic;"> a.6. Repos </h4>

<ins>Databricks Repo Capabilities<ins>

- Clone, push and pull from remote Git repo.
- Create and manage branches.
- Create and edit notebooks.
- Compare differences upon commit.
- Git reset

<ins>Git Provider<ins>

- Create pull request
- Resolve merge conflicts
- Merge or delete branches
- Rebase a branch

Check the API documentation to use some endpoint to interact with your Repos: https://docs.databricks.com/api-explorer/workspace/repos/create

```bash
POST /api/2.0/repos -> create repo
Request:
{
  "url":...,
  "provider":...,
  "path":...,
}
GET /api/2.0/repos -> get repos
GET /api/2.0/repos/{repo_id} -> get repo by id
PATH /api/2.0/repos/{repo_id} -> update repo (to a different branch or tag)
DELETE /api/2.0/repos/{repo_id} -> delete repo
```

<h3 style="font-weight:bold;"> b. Apache Spark </h3>

<h4 style="font-style:italic;"> b.1. Spark Architecture </h4>

**Link**: https://medium.com/p/5a2a6a304bec

![](assets/DE-SparkArchitecture.webp)

<ins>Driver Program<ins>

- Calls the main program of an application and creates the `SparkContext`.
- Coordinates with the spark applications, run by the Workers.

<ins>Cluster Manager<ins>

- Allocate resources accross the Spark applications (Hadoop YARN, Apache Mesos and Standalone Scheduler).

<ins>Worker Nodes<ins>

- Slaves nodes whose job is to basically execute the tasks.

<h4 style="font-style:italic;"> b.2. PySpark and DataFrame API </h4>

*Please review the Databricks Associate Developer for Apache Spark (Python) Cheatsheet*.

<h3 style="font-weight:bold;"> c. Delta Lake </h3>

<h4 style="font-style:italic;"> c.1. SQL-based Delta API </h4>

The **SQL-based Delta API** provides a simplified syntax for creating *tables*, *inserting data*, *updating data*, *deleting data*, and *querying data* using SQL statements. It supports common SQL operations such as `SELECT`, `JOIN`, `GROUP BY`, and `ORDER BY`, as well as more advanced operations such as window functions and complex joins.

**Syntax SQL Documentation**: https://docs.databricks.com/sql/language-manual/index.html

<h4 style="font-style:italic;"> c.2. Basic Architecture </h4>

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

<h4 style="font-style:italic;"> c.3. Core Functions </h4>

1. ACID Transactions
2. Schema Enforcement: Delta Lake automatically detects and handles schema changes, making it easy to evolve data over time.
3. Time Travel
4. Upserts and Deletes.
5. DML Operations
6. Metadata Management
7. Data Compaction and Optimization (Z-order and data skipping).

<h3 style="font-weight:bold;"> d. Databricks CLI </h3>

<h4 style="font-style:italic;"> Deploying notebook-based workflows </h4>

**Link**: https://docs.databricks.com/dev-tools/cli/index.html

Install the CLI
```bash
pip install databricks-cli
```
*To authenticate for running Databricks CLI commands, get a Personal Access Token.*
```bash
databricks congigure --token
Databricks Host (): https://<instance-name>.cloud.databricks.com
Token: 
```

<ins>Cluster<ins>
```bash
databricks clusters list
databricks clusters start --cluster-id <id>
```

*Output in JSON format*
```bash
databricks clusters list --output JSON | jq '[ .clusters[] | {name: .cluster_name, id: .cluster_id}]'
```

<ins>Dbfs<ins>

```bash
databricks fs cp <localFile> <db.fileLocation> --overwrite
databricks fs ls dbfs:/...
```

<ins>Secrets<ins>

```bash
databricks secrets create-scope --score bookstore-dev
databricks secrets put --scope <scope-name> --key dbpass --string-value 12345
databricks secrets list --scope bookstore-dev
```

<ins>Jobs (Workflows)<ins>

**Helper:**
```bash
databricks jobs -h
```

**Create a Job:**
```bash
databricks jobs create --json-file create-job.json
```
*create-job.json*
```json
{
  "name": "new-job",
  "existing_cluster_id": "123341414-414",
  "notebook_task":{
    "notebook_path":"/Users/something@email.com/Notebook"
  },
  "email_notifications":{
    "on_success": [
      "something@email.com"
    ],
    "on_failure": [
      "something@email.com"
    ]
  }
}
```
*return*
```json
{"job_id": 246}
```

**Delete a Job**
```bash
databricks jobs delete --job-id 246
```

**Get a Job Information**
```bash
databricks jobs get --job-id 246
```

**Get all Jobs**
```bash
[For Available Jobs]: databricks jobs list
[For ALL Jobs]: databricks jobs list --all
```

**Reset a Job**
```bash
databricks jobs reset --job-id 246 --json-file reset-job.json
```

*create-job.json*
```json
{
  "job_id" : 246,
  "name": "new-name-job",
  "existing_cluster_id": "123341414-414",
  "notebook_task":{
    "notebook_path":"/Users/something@email.com/Notebook"
  },
  "email_notifications":{
    "on_success": [
      "something@email.com"
    ],
    "on_failure": [
      "something@email.com"
    ]
  }
}
```

**Run a Job**
```bash
databricks jobs run-now --job-id 246
```
*Return*
```json
{
  "run_id": 111,
  "number_in_job": 1
}
```

<h3 style="font-weight:bold;"> e. Databricks REST API </h3>

<h4 style="font-style:italic;"> Configure and trigger production pipelines </h4>

*Every API requires a Bearer Token, so first we need to create a Personal Access Token.*

**Link**: https://docs.databricks.com/api-explorer/workspace/introduction

**Create a new job**
```bash
[POST] /api/2.1/jobs/create
```
*Request*
```json
{
  "format":"SINGLE_TASK/MULTI_TASK",
  "continuous": {
    "pause_status": "PAUSE"
  },
  "name" : "MyJob",
  "job_clusters": ["..."],
  "email_notifications": ["..."],
  "tags": "{}",
  "tasks": [
      {
        "dbt_task": {},
        "spark_python_task":{
            "python_file":"string",
            "source": "WORKSPACE",
            "parameters":[]
            ...
        }
      }
  ],
  "schedule": {
      "quartz_cron_expression": "",
      "timezone_id": "",
      "pause_status":"PAUSED"
  }
}
```
*Response*
```json
{
  "job_id": 0
}
```

**Delete a job**
```bash
POST /api/2.1/jobs/delete
```
*Request*
```json
{
  "job_id": 0
}
```

**Get a job**
```bash
GET /api/2.1/jobs/get?job_id=0
```

**List all job**
```bash
GET /api/2.1/jobs/list
```

**Change settings of job**
```bash
POST /api/2.1/jobs/reset
```
*The request has the same syntax of the POST create plus the "job_id" field*

**Trigger a new job run**
```bash
POST /api/2.1/jobs/run-now
```
*Request*
```json
{
  "job_id": 0,
  "notebook_params": {},
  "jar_params": {},
  "spark_submit_params":{},
  "python_params":{}
  ...
}
```
*Response*
```json
{
  "run_id": 0,
  "number_in_job": 0
}
```

**Cancel a Job**
```bash
POST /api/2.1/jobs/runs/cancel
```
*Request*
```json
{
  "run_id": 0
}
```

**Cancel a all active runs of a Job**
```bash
POST /api/2.1/jobs/runs/cancel-all
```
*Request*
```json
{
  "job_id": 0
}
```

**Delete a job run**
```bash
POST /api/2.1/jobs/runs/delete
```
*Request*
```json
{
  "run_id": 0
}
```

*and so on...*

**Get a single job run**
```bash
GET /api/2.1/jobs/runs/get?run_id=0&include_history=true
```

**Get metadata (output) for a single run**
```bash
GET /api/2.1/jobs/runs/get-output?run_id=0
```

**List runs for a job**
```bash
GET /api/2.1/jobs/runs/list?active_only=True&job_id=123...
```

**Repair a job run**
```bash
POST /api/2.1/jobs/runs/repair
```
*Request*
```json
{
  "pipelines_params":{},
  "python_params":[],
  "notebook_params":{},
  "jar_params":[],
  "latest_repair_id":0,
  "run_id":0
}
```
*Response*
```json
{
  "repair_id": 0
}
```

**Trigger One-time run**
```bash
POST /api/2.1/jobs/runs/submit
```

*and so on..*