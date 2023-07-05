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
    alias = 'my_alias_table_name',
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
        columns:
          - name: column_name
            tests:
              - unique
              - not_null
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
  - Extra fields: `dbt_scd_id` and `dbt_updated_at` (both internals).
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
  - You can use `check_cols = 'all'` to track all columns
  - In that case, consider using a surrogate key to condense many columns into a single column.

`invalidate_hard_deletes = True` helps us to track the rows that no longer exist.

*Best practices*:
- Snapshot your source data
- Include as many columns as possible
- Avoid joins in your snapshot query
- Limit the amount of transformation

## *Advanced Testing* course

- Tests = assertions

*Testing techniques*
1. Interactive / Ad-hoc queries: query to test the PK
2. Standalone saved query: saving the query test into the project
3. Expected results: This adds context and information to the tests.

*Testing Strategies*
- Test on a Schedule: standalone test w/expected results

*Good test*: automated, fast, reliable, informative, focused.

*What to test*
- Contents of the data
- Constrains of the table
- The grain of the table

```yml
- unique
- not_null
- accepted_values
- other packages
```

- Between 2 objects:
  - Compare values in one model to a source of truth in another model.
  - Ensure data has neither been erroneously added or remove

  ```yml
  - relationship
  - dbt_utils.equality
  - dbt_expectations.expect_table
  ```

- Testing the freshness of raw source data
`dbt source freshness`
- Making some notification when the raw data is not up to date
- Temporary testing: to check efficiently refactoring (`audit_helper`)

*Where to locate your tests*
1. On every models package -> using generic tests in the schema.yml file
  - This can be triggered by using `dbt tests` or `dbt build`
  - This can be executed in development and production job.
2. Creating specific tests -> using sql files in the /tests folder
  - Must be referenced in the specific schema.yml
  - This can be triggered by using `dbt tests` or `dbt build`
  - This can be executed in development and production job.
3. Source freshness:
  - Should be specified on the models folder where we are using the raw data
  - Going to be triggered with `dbt source freshness`
  - Use it on production jobs
4. Testing over the entire project:
  - configured in the `dbt_project.yml` file.
  - the test will act on the whole project to test wether you have defined tests (or documentation).
  - Run it with `dbt run-operation`
  - When: development adhoc or during continuous integration.

```yml
models:
  project_name:
    +required_tests: {"unique.*|not_null": 2}
    marts:
      +required_tests: {"relationship.*": 1}
      core:
        +materialized: table
    staging:
      +materialized: view
```

- the first required test will be executed
- the second required test will be executed if we set the test in the yml file of the marts folder.

To execute the required tests:

```bash
dbt run-operation required_tests
```

To avoid passing a required test for an specific model
```sql
{{ config(required_tests=None) }}
```

*When to test*
- Test in development phase
- Run tests automatcally as an approval/CI
- Manual testing vs automated testing
1. Test while adding or modifying dbt code -> use dbt build
2. Test while deploying your data in production -> if the test fail, we have to make a rollback.
3. Test while opening pull requests (CI) -> if the `dbt build --models state:modified+` failed, mark PR as failed, don't allow PR to be merged.
4. Test in QA branch before your dbt code reaches main

*Testing commands*
- dbt test: run tests defined on models, sources, snapshots, seeds.
```bash
dbt test 
dbt test --select one_specific_model
dbt test --select folder.sub_folder.* //running all models in package
dbt test --select test_type:singular
dbt test --select test_type:generic
dbt test --exclude source:* //only models
dbt test --store-failures
```

Other examples
```bash
dbt test -s source:*
dbt build --fail-fast
```

How to store test failures in the database
```bash
dbt test -s my_model --store-failures
```

it's going to create a table in the data warehouse with the observed records

### **Singular Tests**

Create a sql file in the /tests folder

```sql
SELECT
  amount
FROM {{ ref('stg_mymodel') }}
WHERE
  amount <= 0
```

```bash
dbt test -s my_stgtest.sql
```

### **From Singular Tests to Generic Test**
Tip: create a subfolder /generic to store your generic test;

```sql
{% test  assert_column_is_greater_than_zero(model,column_name) %}
SELECT
  {{column_name}}
FROM {{ model }}
WHERE
  {{column_name}} <= 0
{% endtest %}
```

Add the generic test into the schema.yml file
```yml
- name: model_name
  columns:
    - name: column_1
      tests:
        - assert_column_is_greater_than_zero
```

### **Using dbt Packages**

Most of the built-in test macros are located in dbt_utils package.

```yml
- name: model_name
  tests:
    - dbt_utils.expression_is_true:
      expression: "mycolumn > 5"

  columns:
    - name: column_1
      tests:
        - unique
```

Another one is dbt_expectations package
```yml
- name: model_name
  tests:
    - dbt_utils.expression_is_true:
      expression: "mycolumn > 5"

  columns:
    - name: column_1
      tests:
        - assert_column_is_greater_than_zero
        - dbt_expectations.expect_column_values_to_be_between:
            min_value: 5
            row_condition: "column_2 is not null"
            strictly: True
        - dbt_expectations.expect_column_values_to_be_between: 
          # this will perform what the dbt_utils.expression-is-true test does!
              min_value: 0
              row_condition: "column_2 is not null" 
              strictly: false
          - dbt_expectations.expect_column_mean_to_be_between: 
          # this will perform what our singular and generic tests do!
              min_value: 1
              group_by: [customer_id] 
              row_condition: "order_id is not null" # (Optional)
              strictly: false
```

Finally another one is audit_helper. You can create a new analyses for this evaluations.

```sql
{% set dbt_relation = ref('my_model') %}

{{
  audit_helper.compare_relations(
    a_relation = old_etl_relation,
    b_relation = dbt_relation,
    primary_key = "column_id"
  )
}}

```

Comparing column values
```sql

{% macro audit_helper_compare_column_values() %} -- to use this macro, replace the table references and primary key and dbt run-operation audit_helper_compare_column_values
{%- set columns_to_compare=adapter.get_columns_in_relation(ref('orders__deprecated'))  -%}

{% set old_etl_relation_query %}
    select * from {{ ref('orders__deprecated') }}
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('orders') }}
{% endset %}

{% if execute %}
    {% for column in columns_to_compare %}
        {{ log('Comparing column "' ~ column.name ~'"', info=True) }}
        {% set audit_query = audit_helper.compare_column_values(
                a_query=old_etl_relation_query,
                b_query=new_etl_relation_query,
                primary_key="order_id",
                column_to_compare=column.name
        ) %}

        {% set audit_results = run_query(audit_query) %}

        {% do log(audit_results.column_names, info=True) %}
        {% for row in audit_results.rows %}
            {% do log(row.values(), info=True) %}
        {% endfor %}
    {% endfor %}
{% endif %}

{% endmacro %}

```

### **Testing Configurations**

Types of **model** configurations:
- materialized
- tags
- schema
- persist_docs

Types of **test** configurations:
- severity and threshold
- where
- limit
- store failures

```yml
tests:
  - not_null:
    config:
      limit: 10  # limit of return records that not cover the test
      where: "updated_at > '2023-06-01'"
      severity: error # could be warn as well
      warn_if: ">50"
      error_if: ">100"
      store_failures: true
      schema: test_failures
```

In generic test file
```sql
{{
  config(
    severity = "warn"
  )
}}

SELECT *
FROM ...
```

Can be configured in the yml files, config blocks, or macros-tests/generic or dbt_project yml file.

*Test configuration at the project level*
On `dbt_proyect.yml`
```yml
tests:
  project_name:
    +severity: warn
    +store_failures: true
```

## *Advanced Deployment with dbt Cloud* course

Environment: dbt version + git branch + data location
  - Development: 
    - dbt version: env. setting (v1.3)
    - git: IDE interface (feat/scores)
    - data: profile setting (dbt_luis)
  - Deployment:
    - dbt version: env. setting (v1.3)
    - git: env. setting (main)
    - data: env. setting (company)
    - +Jobs -> sequence of dbt commands / scheduled or triggered
      - Includes Run Results

### *Deployment architecture*

- Direct Promotion - One Trunk
  - New feature branches from the main branch
  - Each feature branch open a pull request to main branch
- Indirect Promotion - Many Trunk
  - New feature branches from an intermediate branch
  - Each feature branch open a pull request to the intermediate branch
  - Last PR from intermediate branch to main branch

### *Direct Promotion*

- Go to Deploy/Environments
- Create a New Environment
  - Create a new job
    - Select the environment
    - Target Name: prod
    - Add the dbt commands
    - Triggers

### *Indirect Promotion*
- Go to Deploy/Environments
- Create a New Environment
  - Check only run on a custom branch
    - Put the intermediate branch
    - Add the proper schema for this intermediate branch
  - Create a new job
    - Select the environment
    - Target Name: prod --> this is useful for the parameters you may in your project
    - Add the dbt commands
    - Triggers

In the end, you may have 3 environments:
- Development
- Production
- QA (the intermediate "branch")

### *DAG*

The entire lineage of our project. We can see the following
- elements by type: models, seeds, snapshots, sources, tests, analyses, exposures, etc.
- elements by packages
- elements by tags
- `--select`: 
  - `+model`: the model and its predecessors
  - `model+`: the model and its successors
  - `+model1+ +model2`: make 2 selects once
  - `+model1+,+model2`: see the share parents
- `--exclude`: 
  - `+model`: the model and its predecessors
  - `model+`: the model and its successors

### *dbt build*
- run and test all nodes in DAG order
- also includes snapshots and seeds
- skip nodes downstream of a test failure

### *Job types*
- Standard job: build the entire project, leverage incremental logic, typically daily
- Full refresh: build entire project, rebuild incremental nodes from scratch, typically weekly
- Time sensitive: build a subset of your DAG, helpful to refresh models for specific part of the business
- Fresh build (1.1+): check if sources is updated
  `dbt build --select source_status:fresher+`

### *Triggering Jobs*

- On schedule
  - CRON:
    - * * * * * (minute hour day month day-week-0to7)
    - "*"" any value
    - "," value list separator
    - "-" range of values
    - "-" step values

  ```
    */30 6-23 * * 1-5 
    Every 30 minutes for hours between 6 am and 11pm UTC from Monday to Friday

    15,45 0-4,8-23 * * 0 (sunday, running from midnight to 4 am only minute 15 and 45 and then from 8 am to 11 pm)
  ```

- Via Webhooks
  Run on Pull Requests
- dbt Cloud API
  - Get the API token from Account Settings
  - Get the API token from Service Tokens (you can set the permissions).
  ```bash
    POST https://cloud.getdbt.com/api/v2/accounts/{{account_id}}/jobs/{{job_id}}/run

    Header: {"Authorization":"Token <your token>"}
    
    Body: {"cause":"Triggered via API"}

  ```

### *Review Job Run*
- Details like Run Timing, Model Timing, artifacts (files generated by dbt like json files, compiled sql)
- Commands ran by dbt

### *Continuous Integration*

Basically we are creating a Job (from a new environment) that run on Pull Request (CI purpose)

- Basic CI:
  - Run on Pull Request
  - Execute dbt run
  - Execute dbt test
- Slim CI:
  - Run on Pull Request and manage only the changes
  - dbt run -s state:modified+
  - dbt test -s state:modified+

### *Custom Environment*

- *Target Name*: if no target name provided, it will be set with "default"

dbt normally joins the target schema and the custom schema to create a new schema for our models sceham, however we can customize it. To do that we need to overwrite the `generate_schema_name` macro.

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}

  {%- set default_schema = target.schema -%}
  {%- if custom_schema_name is none -%}

      {{ default_schema }}

  -- If not env_var('DBT_MY_ENV','') set in the variables group from the environment,
  -- We can use target.name instead
  {%- elif env_var('DBT_MY_ENV','') == 'prod' -%}

      {{ custom_schema_name | trim }}


  {%- else -%}

      {{ default_schema }}_{{ custom_schema_name | trim }}

  {%- endif -%}

{%- endmacro %}
```

---
## Additional Concepts Relevant for the Certification

## a. *Metrics*

Link: *[dbt Metrics](https://docs.getdbt.com/docs/build/metrics)*

It's basically an aggregation over a table that supports multiple dimensions. They will appear as nodes in the DAG diagram y we can create them by using YAML files.

```yml
version 2:

models: 
  ...

metrics:
  - name: my_metric
    label: Information about the metric
    description: "My Awesome Metric!"

    calculation_period: count # you can use others like, count_distinct, derived, etc.
    expression: column_name
    
    # We can use derived for our custom calculations
    # calculation_period: derived
    expression: "{{ metric('column_1') }} / {{ metric('column_2') }}"

    dimensions:
     - dimension_1
    
    ...
```

Querying a metric (requires `dbt_metrics` package)
```sql
select *
from {{
  metrics.calculate(
    metric('my_metric'),
    grain = 'month',
    dimensions = ['dimen_1','dimen_2']
  )
}}
```

## b. *Hooks and Operations*

Link: *[Hooks and Operations](https://docs.getdbt.com/docs/build/hooks-operations)*

*Hooks*

- `pre-hook`: executed *before* a model/seed/snapshot is built.
- `post-hook`: executed *after* a model/seed/snapshot is built.
- `on-run-start`: executed at the *start* of `dbt run/seed/snapshot`.
- `on-run-end`: executed at the *end* of `dbt run/seed/snapshot`.

```sql
-- This is a model sql file
{{
  config(
    post_hook = [
      "alter table {{ this }} ..."
    ]
  )
}}
```

*Operations*

Macros that can run using `run-operation` commnad. A very convenient way to run macros without running a model.

```bash
dbt run-operation my_macro --args '{arg1: value1}'
```

## c. *Python models*

Supported in dbt v1.3.

Link: *[Python models](https://docs.getdbt.com/docs/build/python-models)*

Characteristics:
- Include an adapter for a data platform that supports fully featured Python runtime (the python model is rexecuted remotely by the data platform).
- Python models use DataFrames. Instead of a final `select` the model retuns a final DataFrame (lazy evaluation).
- We need to define a function named `model(dbt, session)`. In Snowflake, this model can return a Snowpark or pandas DataFrame.

```python
def model(dbt, session):

  dbt.config(
    materialized="table",
    packages = ["numpy==1.23.1", "scikit-learn"]
    )

  df_model_1 = dbt.ref("upstream_model_name")

  df_model_2 = dbt.source("upstream_source_name","table_name")

```

## d. *Connection profiles*

Link: *[Connection profiles](https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles#understanding-threads)*

A profile consists of **targets**. Each target specify the type of warehouse, the credentials, and other configurations.

- Profile name: should be the same one like in `dbt_projects.yml`
- target: default target
  - type
  - warehouse credentials
  - schema
  - threads

*To validate your warehouse credentials*
```bash
dbt debug
```

*How to use a target in your dbt command*
```bash
dbt run --target
```

The number of threads represents the maximum number of paths through the graph dbt may work on at once. Increasing the number of threads can minimize the run time of your projects (default is 4).

*Other ways to use the profiles*

```bash
dbt run --profiles-dir path/to/directory
```
or by overriding the environmental variable
```bash
$ export DBT_PROFILES_DIR=path/to/directory
```
or by updating the file from `~/.dbt/` location.

## e. *Using aliases*

Link: [Using Aliases](https://docs.getdbt.com/docs/build/custom-aliases)

- Remember if there is an ambiguity, dbt will check it and present with an error. To solve the issue, one way is to use custom schema

## f. *Project Variables*

Link: [Project Variables](https://docs.getdbt.com/docs/build/project-variables)

We can define in 2 ways: in `dbt_project.yml` or command line

In `dbt_project.yml` file
```yml
name: my_dbt_project
version: 1.0.0

config-version: 2

vars:
  # The `start_date` variable will be accessible in all resources
  start_date: '2016-06-01'

  # The `platforms` variable is only accessible to resources in the my_dbt_project project
  my_dbt_project:
    platforms: ['web', 'mobile']

  # The `app_ids` variable is only accessible to resources in the snowplow package
  snowplow:
    app_ids: ['marketing', 'app', 'landing-page']

```

In command line:
```bash
dbt run --vars '{"key":"value"}'
```

*Review the variable precedence*

## g. Environment Variables

Link: [Environment Variables](https://docs.getdbt.com/docs/build/environment-variables)

Environment variables in dbt Cloud must be prefixed with either `DBT_` or `DBT_ENV_SECRET_`

Precedence order (lowest to highest):

1. the optional default argument supplied to the `env_var` Jinja function in code
2. a project-wide default value, which can be overridden at
3. the environment level, which can in turn be overridden again at
4. the job level (job override) or in the IDE for an individual dev (personal override).

## *A good cheatsheet for dbt commands*

I found this link very useful!
https://medium.com/indiciumtech/17-dbt-commands-you-should-start-using-today-581998dbf8f0

Also, we have the dbt documentation
https://docs.getdbt.com/reference/node-selection/methods#the-source_status-method