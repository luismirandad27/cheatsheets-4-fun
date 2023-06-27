#  dbt Analytics Engineering Cheatsheet
This cheatsheet is based dbt courses available in https://courses.getdbt.com/collections

## *dbt Fundamentals* course

#### 1. Ways to use dbt
- `dbt cloud`: online IDE and interface to run dbt on schedule
- `dbt core`: command line tool (to run locally)

#### 2. Data Platforms
- dbt works in a **transformation layer** (ELT schema)
- Some data platforms to ingretate with dbt are: Snowflake, BigQuery, Databricks, Redshift, etc.

#### 3. Dbt Cloud IDE
- `Develop` menu (Classic / new IDE - beta)
    - Version Control: you can view the current branch and the documentation as well.
    - File Explorer: if you want to make some editing (renaming, copying files), users must have **more than** a READONLY access.
    - Text Editor
    - Results Pane: you can see the *preview*, can *compile* and *build*. As well, you can view your results, compiled code and the **lineage** of the workflow.
    - Command Line Bar
- `Deploy` menu
    - Run History
    - View jobs
    - Envornments
    - Data Sources
- `Documentation`
- `Account Settings`
- `User Settings`
- `Email Notifications`

### 4. **Modeling**
- Located in `/model` folder.
- dbt handles the DDL and DML

```sql
-- Materializing a model into a table (Jinja template)
-- Default materialization is View
{{ config (
    materialized="table"
)}}
```

Running dbt
```bash
dbt run
```

Running an specific model
```bash
dbt run --select dim_mymodel
```

Materialize a model and its downstream models
```bash
dbt run --select dim_mymodel+
```

- Part of the modularity of your pipeline is to create multiple **stages** before feeding your last table.

Calling a previous model (reference)
```sql
-- Using the ref MACRO
-- This function compiles to the name of the db object it has been created in correspondant dev. environment.
-- Also builds a lineage graph.
WITH my_previous_stage_model AS
(
    SELECT *
    FROM {{ ref('stg_my_previous_stage') }}
)
SELECT * FROM ...
```

**Traditional Modeling**
- Star Schema
- Kimball
- Data Vault

**Denormalized Modeling**
- Agile analytics
- Ad hoc analytics

**Model Naming Conventions**
- *Sources (src)*
    - the raw data that has already loaded
- *Staging (stg)*
    - clean and standarize the data
    - one to one with source tables
- *Intermediate (int)*
    - models between staging and final models
    - always built on staging models
- *Fact (fct)*
    - things that are occuring or have occurred (events, clicks, votes)
- *Dimension (dim)*
    - people, place, or thing, ...

### 5. **Sources**
- Direct table references
- Avoid changing manually the location or the naming of the table
- Configuration would be only once in the `.yml` file.
- You can visualize raw tables in your lineage

```sql
{{ source('schema','table_name') }}
-- select * from schema.table_name
```
