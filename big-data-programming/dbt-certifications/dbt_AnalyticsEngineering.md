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

*Adding source freshness*
- To find out how fresh is the source

```yml
version: 2

sources:
  - name: source_name
    database: db_name
    schema: schema_name
    tables:
      - name: table_name
        loaded_at_field: updated_at --refers to the column to analyze
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
```

Execute source freshness verification
```bash
dbt source freshness
```

### 6. **Testing**
- Run tests in development while you write code
- Schedule tests to run in production

2 type of testing:
- Singular tests: specific test cases
- Generic tests: writing a couple lines in the YAML file
    - unique
    - not_null
    - accepted_values
    - relationships

- Aditional testing can be imported through packages or write your custom generic tests.


*Generic tests sample*
```yml
models:
  - name: stg_example
    columns: 
      - name: column_id
        tests:
          - unique
          - not_null
      - status: column_status
        tests:
          - accepted_values:
            values:
                - status_1
                - status_2
                ...
```

Running a dbt testing (all generic and singular tests)
```bash
dbt test --select stg_example
```

*Singular test*
Located in /tests
```sql
/*
Make your query that gives some kind of results.
To make the test fail you have to design your query
in terms to get the records that not follow your rule.
*/
```

Running dbt all generic testing
```bash
dbt test --select test_type:generic
```

Running dbt all singular testing
```bash
dbt test --select test_type:singular
```

**Using `dbt build`**

This is the best option for making testing and running.

```bash
dbt build
```

Steps:
1. Testing the sources
2. Running stage layer
3. Testing stage layer
4. Running dim/core layer
5. Testing dim/core layer

### 7. **Documentation**

- DAG is automatically generated to show flow of data from source to final models.
- You can add your own text descriptions, directly in the dbt project.

```yml
models:
  - name: stg_example
    description: Staged order data from database.
    columns: 
      - name: id
        description: Primary key for something.
      - name: status
        description: '{{ doc("something_status") }}'
```

Creating a .md file
```md
{% docs order_status %}
	
One of the following values: 

| status         | definition                                       |
|----------------|--------------------------------------------------|
| placed         | Order placed, not yet shipped                    |
| shipped        | Order has been shipped, not yet been delivered   |
| completed      | Order has been received by customers             |
| return pending | Customer indicated they want to return this item |
| returned       | Item has been returned                           |

{% enddocs %}
```

How to generate the docs:
```bash
dbt docs generate
```

The generated documentation includes the following:
- Lineage Graph
- Model, source and columns descriptions
- Generic tests added to a column
- The underlying SQL code

### 8. **Deployment**

- Dedicated production branch (known as the default branch)
- Dedicated production schema (`dbt_production`)
- Run any dbt command on schedule

*How to create your Deployment Environment*
- General Settings: the dbt version
- Data Warehouse Connection
- Deployment Credentials:
  **Use a separate data warehouse account**
  **The schema should be different**

*Scheduling a job*
- Select the deployment environment
- Set the dbt commands to be executed.
- Set the triggers 


Example of good dbt commands order:
```bash
dbt depts #for dependencies
dbt run
dbt test
dbt docs generate
```

## *Jinja, Macros and Packages * course

### **1. Jinja**
- Python based templating language
- Set the foundations for Macros

The basics
```sql
{# #} -- This is for commenting jinja code
{{ ... }} -- These will print text to the rendered file
{% %} -- This is used for jinja statements 
```

Dictionaries
```sql
{% set person = {
    'student_id': '123456789',
    'student_name': 'Luis Miguel Miranda'
}%}

-- Rendering the name of the student
{{ person['student_name'] }}

```

Lists
```sql
{% set my_fav_videogames = ['Fifa','Call of Duty','Battlefield']}

-- Rendering the name of the second element
{{ my_fav_videogames[1] }}

```

If/else statements
```sql
{% set my_grade = 90 %}

{% if my_grade > 95 %}
  Oh wow! You are a smart guy!
{% else %}
  Oh keep going!
{% endif %}
```

For Loops
```sql
{% set my_fav_videogames = ['Fifa','Call of Duty','Battlefield']}

{% for my_videogame in my_fav_videogames %}
  I love playing {{ my_videogame }}
{% endfor %}
```

Macros
```sql
{% macro display_stud_name (name, is_male = '1') %}

  {% if is_male == '0' %}
    Her name is {{ name }}
  {% else %}
    His name is {{ name }}
  {% endif %}

{% endmacro %}
```

### **2. Macros**

- Write generic logic once
- Re-use the logic throughout your project
- Packages allow you to import macros other developers wrote
- Think about DRY vs Readable Code
  - DRY stands for "Don't Repeat Yourself"
  - Means to abstract away the logic into macros

```sql
{% macro convert_cents_to_dollar(column_name, num_decimal) %}
  round(1.0 * {{ column_name }} / 100, {{ num_decimal}})
{% endmacro %}
```

### **3. Packages**

We need to create `packages.yml` inside the project folder

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.0
  - package: <github link>
    revision: master
  - local: sub-project
```

in the command line we must run the following
```bash
dbt deps
```

### **4. More Advanced Macro and Packages**

- grant_select macro
```sql
{% macro grant_select(schema=target.schema, role=target.role) %}

  {% set sql %}
  grant usage on schema {{ schema }} to role {{ role }};
  grant select on all tables in schema {{ schema }} to role {{ role }};
  grant select on all views in schema {{ schema }} to role {{ role }};
  {% endset %}

  {{ log('Granting select on all tables and views in schema ' ~ target.schema ~ ' to role ' ~ role, info=True) }}
  {% do run_query(sql) %}
  {{ log('Privileges granted', info=True) }}

{% endmacro %}
```

- We are using the `run_query` macro to run SQL commands
  - After executing the macro, it will create a file type called agate.
- To display some logs, we can use the `log` macro
- Other interesting functions:
  - `dbt_utils.get_relations_by_prefix(database, schema, prefix)`
- The `target` contains the information about the connection to the warehouse.

How to run a macro independently
```bash
dbt run-operation grante_select
```

## *Analyses and Seeds* course

### **1. Analyses**

- SQL files in the analyses folder.
- Support Jinja
- Can be compiled with `dbt compile`
- One off queries and training queries (exploration)
- Auditing / refactoring

### **2. Seeds**

- **csv** files in the data folder
- Build a table from a small amount of data in a csv file
- Build these tables with `dbt seed`
- To refer these tables you can use the `{{ref()}}` function.
  - The name of the parameter is gonna be the name of the seed file.
- Examples:
  - Country codes
  - Employee id/emails (if the data is really small)

## *Advanced Materializations* course

5 types: tables, views, ephemeral, incremental, snapshot.

You can configure for every model by updating the `dbt_projects.yml`

```yml
models:
  model_name:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

### **Ephemeral**

- Does not exist in the database
- Reusable code snippet
- Interpolated as a CTE
- If you created previously a table or a view and you want to change it into an ephemeral, it won't delete the view/table
- Recommended only for very light-weight transformation

### **Incremental**

- Historical data doesn't change.
```yml
{{
  config(
    materialized= 'incremental'
  )
}}
```

How to identify new rows to just add
```sql
with table_events as (
  select * from {{ source('database','table_name') }}
  {% if is_incremental() %}
  where date_column >= (select max(max_column_date) from {{ this }})
  {% endif %}
)
```

4 things to do when we change to incremental.
- model already exist as an object
- the object should be a table
- the model was configured with `materialized = incremnetal`
- the `--full-refresh` will recreate the entire table.

If we make a wider range of time for our incremental table, we could get some duplicate records. We have to make one change

```yml
{{
  config(
    materialized= 'incremental',
    unique_key = 'column_name_id' 
  )
}}
```

It's going to perform a MERGE instead of a INSERT.

### **Snapshot**

- Implement SCD Type 2 over mutable source tables.
- We can identify the lifetime of each row by the columns `dbt_valid_from` and `dbt_valid_to`.
- We can preserve the current state of the records.

```sql
{% snapshot orders_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',

      strategy='check',
      check_cols = ['column_1','column_2']
    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}
```
2 strategies:
- Timestamp: and define the `updated_at` column
- Check: and define the `check_cols`
