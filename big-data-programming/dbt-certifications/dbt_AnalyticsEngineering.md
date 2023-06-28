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