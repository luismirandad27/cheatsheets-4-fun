# Databricks Associate Developer for Apache Spark (Python) Cheatsheet

## Creating Spark Data Frames with Pyspark

### 1. Creating a Data Frame from List

#### From Integer List
```python
#Defining a list with integer elements
lst_ages = [10,12,13,14]
#Creating df based on the integer list
df = spark.createDataFrame(lst_ages,"int")
```
#### From String List
```python
#Defining a list with string elements
lst_names = ["peter","mark","luis"]
#Creating df based on the integer list
df = spark.createDataFrame(lst_names,"string")
```
#### Using Pyspark SQL Data Types
```python
#Importing pyspark data types library
from pyspark.sql.types import StringType, IntegerType
```
```python
#Creating df based on the integer list / string list
df_ages = spark.createDataFrame(lst_ages,IntegerType())
df_names = spark.createDataFrame(lst_names,StringType())
```
### 2. Creating a Multi-Column Data Frame using List
```python
lst_ages_mc = [(12,),(24,),(10,)]

df_ages = spark.createDataFrame(lst_ages_mc)
##C: DataFrame[_1: bigint]
df_ages = spark.createDataFrame(lst_ages_mc,'age int')
##C: DataFrame[age: int]

#Mult-Column Data Frame
lst_users = [(1,'Scoot'),(2,'Donald'),(3,'Mickey')]
df_users = spark.createDataFrame(lst_users,"user_id int, user_name string")
```

### 3. Creating Data Frame using Pyspark `Row`
```python
#Importing Pyspark Row
from pyspark.sql import Row
```

#### Convert List of Lists to Data Frame
```python
lst_users = [[1,'Scoot'],[2,'Ronald'],[3,'Mathew']]
rows_user = [Row(*user) for user in lst_users]
spark.createDataFrame(rows_user,'user_id int, user_name string')
##C: DataFrame[user_id: int, user_name: string]
```

#### Convert List of Tuples to Data Frame
```python
lst_users = [(1,'Scoot'),(2,'Ronald'),(3,'Mathew')]
rows_user = [Row(*user) for user in lst_users]
spark.createDataFrame(rows_user,'user_id int, user_name string')
##C: DataFrame[user_id: int, user_name: string]
```

#### Convert List of Dicts to Data Frame
```python
lst_users = [
    {'user_id':1, 'user_name':'peter'},
    {'user_id':2, 'user_name':'marc'},
    {'user_id':3, 'user_name':'luis'}
            ]

rows_user = [Row(**user) for user in lst_users]
spark.createDataFrame(rows_user)
```

### 4. Pyspark Data Types
#### Defining a list of dictionaries
```python
import datetime

users = [
    {
        "id": 1,
        "first_name": "Luis",
        "last_name" : "Miranda",
        "email": "lmirandad27@gmail.com",
        "is_graduated": False,
        "gpa": 3.8,
        "student_from": datetime.date(2022,5,8),
        "last_updated_ts": datetime.datetime(2021,2,10,1,15,0)
    },
    {
        "id": 2,
        "first_name": "Peter",
        "last_name" : "Parker",
        "email": "pparker199@hotmail.com",
        "is_graduated": False,
        "amount_paid": 4.0,
        "student_from": datetime.date(2022,5,9),
        "last_updated_ts": datetime.datetime(2022,8,10,1,15,0)
    }
]
```
#### Creating Data Frame
```python
users_df = spark.createDataFrame([Row(**user) for user in users])
```
#### Showing the data structure
```python
users_df.printSchema()
```
#### Showing first N rows
```python
users_df.show()
```
#### Making samples (subdataset)
```python
#withReplacement: False (the row will appear once) / True (the row will appear more than one)
#fraction: % from the total that is going to be sampled.
#seed
users_df.sample(withReplacement = False,fraction = 0.5,seed=1234)
```
#### Showing columns and data types
```python
users_df.columns
#['id','first_name','last_name','email','is_graduated','gpa','student_from','last_updated_ts']
users_df.dtypes
'''
    [
        ('id','bigint'),
        ('first_name','string'),
        ('last_name','string'),
        ('email','string'),
        ('is_graduated','boolean'),
        ('gpa','double'),
        ('student_from','date'),
        ('last_updated_ts','timestamp')
    ]
'''
```
#### Specifying Schema of Data Frame (I)
```python
user_schema = '''
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_graduated BOOLEAN,
    gpa FLOAT,
    student_from DATE,
    last_updated_ts TIMESTAMP
'''
spark.createDataFrame(users,user_schema)
#or
spark.createDataFrame(users,schema = user_schema)
```
#### Specifying Schema of Data Frame (II)
Before dealing with `StructType`, let's the list of data types offers by Pyspark:

|          |          |
| -------- | -------- |
| StringType     | ShortType     |
| ArrayType     | IntegerType     |
| MapType     | LongType     |
| StructType     | FloatType     |
| DateType     | DoubleType     |
| TimestampType     | DecimalType     |
| BooleanType     | ByteType     |
| CalendarIntervalType     | HiveStringType     |
| BinaryType     | ObjectType     |
| NumericType     | NullType     |

```python
from pyspark.sql.types import *
```
```python
user_schema = StructType([
    StructField('id',IntegerType()),
    StructField('first_name',StringType()),
    StructField('last_name',StringType()),
    StructField('email',StringType()),
    StructField('is_gradudate',BooleanType()),
    StructField('gpa',FloatType()),
    StructField('student_from',DateType()), StructField('last_updated_ts',TimestampType()),
])

spark.createDataFrame(users,schema = user_schema)
```

### 5. Creating Data Frame with Pandas

#### Creating Data Frame with Pandas
```python
import pandas as pd
```
```python
#using the 'users' dictionary previously set
users_df = spark.createDataFrame(pd.DataFrame(users))
```

### 6. Special Data Types
- Special Types: Array, Struct, Map
- List and Dicts can be implicitly converted to Spark ARRAY and MAP respectively

#### `Array` data type

```python
users = [
    {
        "id":1,
        "first_name":"luis",
        "last_name":"miranda",
        "email":"lmirandad27@gmail.com",
        "phone_numbers":["+1 213 313 4124", "+1 344 231 3213"]
    }
]

users_df = spark.createDataFrame([Row(**user) for user in users])

users_df.printSchema()
'''
    |-- phone_numbers: array(nullable = true)
    |    |-- element: string (containsNull = true)
'''

users_df.dtypes
'''
    ('phone_numbers','array<string>')
'''
```

#### `Array` Type: Using `explode` and `explode_outer`
```python
#Using explode and explode_outer
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, explode_outer

#using col
users_df.\
    select('id',col('phone_numbers')[0].alias('mobile')),col('phone_numbers')[1].alias('home')

#explode -> ignore null or empty values
users_df.\
    withColumn('phone_numbers',explode('phone_numbers')).\
    drop('phone_numbers')

#explode_outer -> not ignore null or empty values
users_df.\
    withColumn('phone_numbers',explode_outer('phone_numbers')).\
    drop('phone_numbers')
```

#### `Map` Type
```python
users = [
    {
        "id":1,
        "first_name":"luis",
        "last_name":"miranda",
        "email":"lmirandad27@gmail.com",
        "phone_numbers":{"mobile":"+1 213 313 4124","home":"+1 344 231 3213"}
    }
]

users_df = spark.createDataFrame([Row(**user) for user in users])

users_df.printSchema()
'''
    |-- phone_numbers: map(nullable = true)
    |    |-- key: string
    |    |-- value: string (valueContainsNull = true)
'''

users_df.dtypes
'''
    ('phone_numbers','map<string,string>')
'''
```

#### `Map` Type: Using `explode` and `explode_outer`
```python
#Using explode and explode_outer
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, explode_outer

#using col
users_df.\
    select('id',col('phone_numbers')['mobile'].alias('mobile')),col('phone_numbers')['home'].alias('home')

#explode -> ignore null or empty values
users_df.\
    select('id',explode('phone_numbers')).\
    withColumnRenamed('key','phone_type').\
    withColumnRenamed('value','phone_number')

#explode_outer -> not ignore null or empty values
users_df.\
   select('id',explode_outer('phone_numbers')).\
    withColumnRenamed('key','phone_type').\
    withColumnRenamed('value','phone_number')
```

#### `Struct` Type
```python
users = [
    {
        "id":1,
        "first_name":"luis",
        "last_name":"miranda",
        "email":"lmirandad27@gmail.com",
        "phone_numbers":Row(mobile="+1 213 313 4124",home="+1 344 231 3213"}
    }
]

users_df = spark.createDataFrame([Row(**user) for user in users])

users_df.printSchema()
'''
    |-- phone_numbers: struct
    |    |-- mobile:  string
    |    |-- home:    string
'''

users_df.dtypes
'''
    ('phone_numbers','struct<mobile:string,home:string>')
'''
```

#### `Struct` Type: Using `col` (`explode` and `explode_outer` cannot be used)
```python
#Using explode and explode_outer
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, explode_outer

#using col
users_df.\
    select('id','phone_numbers.mobile','phone_numbers.home')

users_df.\
    select('id','phone_numbers.*')

users_df.\
    select('id',col('phone_numbers')['mobile'].alias('mobile'),col('phone_numbers')['home'].alias('home'))
```

---
## Selecting and Renaming Spark Data Frames
###### Creating Data Frame
```python
import datetime
import pandas as pd
```
```python

#Creating list of dictionaries
gamers = [
    {
        "user_id":1,
        "user_name":"Luis Miguel",
        "user_lastname":"Miranda",
        "contact_info":Row(mobile="+1 312 312 3132", home="+1 999 888 1233"),
        "videogames":["fifa 22","pes 22"],
        "birth_date":datetime.date(1993,3,1),
        "last_update":datetime.datetime(2021,2,10,1,14,0),
        "current_account_balance":68.9
    },
    {
        "user_id":2,
        "user_name":"Jhon",
        "user_lastname":"Perez",
        "contact_info":Row(home="+1 999 888 1233"),
        "videogames":["fifa 22","diablo immortal"]
        "birth_date":datetime.date(2000,6,27),
        "last_update":datetime.datetime(2021,2,10,1,14,0),
        "current_account_balance":120.88
    }
]
```
###### Disabling Apache Arrow in Pyspark for Pandas To/From Conversion
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled",False)
```

###### Creating Data Frame
```python
gamers_df = spark.createDataFrame(pd.DataFrame(gamers))
```

### Narrow/Wide Transformations

Data Frames are *immutable*, we cannot change them once created. With *transformations*, we are creating a new dataframe (keep in mind this). There are 2 types of transformations:

**Narrow Transformations**: doesn't result in *shuffling*. Than means that all the transformation is operating in the each partition (any data movement will not occur).

**Wide Transformations**: result in *shuffling*. Means that, in the operation, many input partitions are being used to create different output partitions (writing on disk).

In the following table, we could see the available functions for each type:



| Narrow Transformation | Wide Transformation |
| -------- | -------- |
| `df.select`     | `df.distinct`     |
| `df.filter`     | `df.union`     |
| `df.withColumn`     | `df.join`     |
| `df.withColumnRenamed`     | `df.groupBy`     |
| `df.drop`     | `df.sort`-`df.orderBy`     |

### 1. Querying DataFrames

#### `select` function

```python
#Selecting all columns (*)
gamers_df.select('*')

#Selecting specific columns of the df
gamers_df.select('user_id','user_name','user_lastname')

#Selecting specific columns of the df as a list
gamers_df.select(['user_id','user_name','user_lastname'])

#or (using * to retrieve the elements)
cols = ['user_id','user_name','user_lastname']
gamers_df.select(*cols)

#Selecting specific columns using Pyspark DataFrame Columns
gamers_df.select(gamers_df['user_id'],gamers_df['user_name'],gamers_df['user_lastname'])
```

#### `alias` function
```python
#DataFrame alias
gamers_df.alias('g').select('g.*')
gamers_df.alias('g').select('g.user_id')
gamers_df.alias('g').select(g['user_id'])

#DataFrame column alias
from pyspark.sql.functions import col
gamers_df.select(col('user_id'),col('user_name'),col('user_lastname'))

#Combining all ways
gamers_df.alias('g').select('g.user_id',g['user_name'],col('user_lastname'))
```

#### `concat` and `lit` functions
```python
from pyspark.sql.functions import concat, lit
```
```python
gamers_df.select(
    col('user_id'),
    concat(col('user_name'),lit(', '),col('user_lastname')).alias('full_name')
)

#With Pyspark DataFrame Columns
gamers_df.select(
    col('user_id'),
    concat(gamers_df['user_name'],lit(', '),gamers_df['user_lastname']).alias('full_name')
)
```

#### More about `lit` function
```python
gamers_df.select('current_account_balance'+25) #ERROR: not same type

gamers_df.select('current_account_balance'+'25') #ERROR: there isn´t any column called current_account_balance25

gamers_df.select('user_id','current_account_balance'+lit(2.0),lit(2.0)+lit(2.0))
# 1 | Null | 4.0
# 2 | Null | 4.0
# Null result because Spark cannot performe arithmetics operation on noncompatible types

#Correct Way
gamers_df.select(col('current_account_balance') + lit(2.0)) #same type (this works)
```
#### `selectExpr` function
```python
#selectExpr works (in some cases like select function)
gamers_df.selectExpr('*')
gamers_df.alias('u').selectExpr('u.*')
gamers_df.selectExpr('user_id','user_name')
gamers_df.selectExpr(['user_id','user_name'])

#and in other cases don't (DOESN'T WORK WITH col and Pyspark DataFrame Columns)
gamers_df.selectExpr(
    'user_id',
    "concat(user_name,', ',user_lastname) AS full_name"    
)

gamers_df.alias('g').selectExpr(
    'g.user_id',
    "concat(g.user_name,', ',g.user_lastname) AS full_name"    
)
```
#### `col` function
```python
#making select from a list
user_id = col('user_id') #Column<user_id>
gamers_df.select(user_id)
```

#### `date_format` function 
```python
from pyspark.sql.functions import date_format
```
```python
gamers_df.select(date_format('birth_date','yyyyMMdd').alias('birthdate_formatted'))
#birth_date_formatted: 19930301 (for user 1)
```
#### `cast` function 
```python
from pyspark.sql.functions import cast
```
```python
#Casting a date to integer number
gamers_df.select(date_format('birth_date','yyyyMMdd').cast('int').alias('birthdate_cast'))
#birth_date_formatted: 20210114

#For example: using Spark Column Object
birthdate_cast = date_format('birth_date','yyyyMMdd').cast('int').alias('birthdate_cast')
gamers_df.select(birthdate_cast)
```

### 2. Renaming Data Frame Columns (or Expressions)
What are the ways that we can rename columns/expressions:
- Using `alias` on `select`.
- Add columns using `withColumn` (*row-level transformation*).
- Rename column using `withColumnRenamed`.
- Rename a bunch of columns using `toDF`

#### `withColumn` function
```python
#The second parameter must be a Pyspark Column
gamers_df.\
    select('users_id','user_name','user_lastname').\
    withColumn('full_name',concat('user_name',lit(', '),'user_lastname'))

#To Count the number of videogames the user ownes we can use size function (for Array type)
from pyspark.sql.functions import size
gamers_df.\
    select('users_id','user_name','user_lastname').\
    withColumn('videogames_count',size('videogames'))

gamers_df.\
    select('users_id','user_name','user_lastname').\
    withColumn('videogames_count',size(gamers_df['videogames']))

#Combining withColumn and alias
gamers_df.\
    withColumn('full_name',concat('user_name',lit(', '),'user_lastname')).\
    select(
        col('user_id').alias('user_identification'),
        size(gamers_df['videogames']).alias('videogames_count'),
        'user_fullname'
    )
```

#### `withColumnRenamed` function
```python
gamers_df.\
    select('users_id','user_name','user_lastname').\
    withColumnRenamed('user_id','identification').\
    withColumnRenamed('user_name','first_name').\
    withColumnRenamed('user_lastname','last_name')
```

#### `toDF` function (renaming and reordering multiple columns)
```python
#setting columns from original list
original_columns = ['user_id','user_name','user_lastname']

#setting columns from final list
final_columns = ['id','name','lastname','phones','videogames']

#we can use toDF after select function
gamers_df.\
    select(original_columns).\
    toDF(final_columns)
# | id | name | lastname | phones | videogames |
```
---
## Manipulating Columns on Spark Data Frames
### 1. Categories

These are the categories of functions offered by `pyspark.sql.functions`

* String Manipulation Functions
    * Case conversion: `lower` and `upper`
    * Getting length: `length`
    * Substrings: `substring` and `split`
    * Trimming: `trim`,`ltrim` and `rtrim`
    * Padding: `lpad` and `rpad`
    * Concatenating string: `concat` and `concat_ws`
* Data Manipulation Functions
    * Current date and time: `current_date` and `current_timestamp`
    * Date Arithmetic: `date_add`, `date_sub`, `datediff`, `months_between`, `add_months`, `nextday`
    * Begin/End Date/Time: `last_day`, `trunc`, `date_trunc`
    * Formatting: `date_format`
    * Extracting Info: `dateofyear`,`dateofmonth`,`dateofweek`,`year` and `month`
* Aggregate Functions
    * `count`, `countDistinct`, `sum`, `avg`, `min` and `max`
* Other Functions
    * `CASE`/`WHEN`, `cast` and array functions

You can check more about each function on the official documentation:
https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#module-pyspark.sql.functions

### 2. How to get some help on Spark Functions?
#### Use `help`
```python
help(date_format)
```

#### *Remember*...
There are some functions (methods) that need to be called from a Column Type Object and not just from a simple string, for example...
```python
#Using upper and desc for the example
from pyspark.sql.functions import upper, desc, col, lit

students = [
            (1,"Luis","Miranda",3.4,"peru","+1 236 999 9999"),
            (2,"Jhon","Perez",3.8,"peru","+1 778 999 9999")
           ]

students_df = spark.\
            createDataFrame(students,schema="""
                student_id INT,
                student_name STRING,
                student_lastname STRING,
                student_gpa FLOAT,
                student_country STRING,
                student_phone STRING
            """)

#Using upper function
students_df.select(upper('student_name'),upper('student_lastname'))#this OK

students_df.select(upper(col('student_name')),upper(col('student_lastname'))#this OK too

#However, Using desc function
students_df.\
    orderBy('student_id'.desc()) #str has no attribute 'desc'

students_df.\
    orderBy(col('student_id').desc()) #This is OK

#Using lit function
students_df.\
    select(concat(col('student_name'),", ",col('student_lastname'))) #This cause an error
                   
students_df.\
    select(concat(col('student_name'),lit(", "),col('student_lastname'))) #lit generates a Pyspark Column Type     
```
### 3. String Manipulation Functions
```python
# concat function
from pyspark.sql.functions import concat,col,lit

students.\
    select(concat(col('student_name'),lit(', '),col('student_last_name')).alias('full_name'))

#conversion and length
from pyspark.sql.functions import initcap, upper, lower, length

students.\
    select('student_country').\
    withColumn('student_country_u',upper(col('student_country'))).\
    withColumn('student_country_l',lower(col('student_country'))).\
    withColumn('student_country_i',initicap(col('student_country'))).\
    withColumn('student_country_len',length(col('student_country')))
# Results: peru | PERU | peru | Peru | 4 (for 1 example)
```

#### Substrings
```python
from pyspark.sql.functions import substring, split, lit, explode

#creating a dummy dataframe
l = [('X', )]
df = spark.createDataFrame(l,"dummy STRING")

#substring function
df.select(substring(lit("Hello World"),7,5))#World
df.select(substring(lit("Hello World"),-5,5))#World

#split function
df.select(split(lit("Hello World, What's up?")," "))
#Result: ["Hello","World,","What's","up?"]

df.select(explode(split(lit("Hello World, What's up?")," ")))
#Result:
#------
#Hello
#World,
#What's
#up?

df.select(explode(split(lit("Hello World, What's up?")," "))[1])
#Result: "World,"
```
#### Padding
Typically to build fixed length values or records
```python
from pyspark.sql.functions import lpad,rpad,lit

#creating a dummy dataframe
l = [('X', )]
df = spark.createDataFrame(l,"dummy STRING")

df.select(lpad(lit("Hello"),10,"-"))
#Result: -----Hello
df.select(lpad(lit("Hello"),6,"-"))
#Result: -Hello
```

#### Trimming
To remove unnecessary characters from fixed length records.
We can use `expr` or `selectExpr` to use SparkSQL based trim functions.
```python
from pyspark.sql.functions import col, trim, ltrim, rtrim, expr
l = [("    Hello.    ",)]
df = spark.createDataFrame(l).toDF("dummy")

#using pyspark functions
df.withColumn("ltrim",ltrim(col("dummy")))\
  .withColumn("rtrim",rtrim(col("dummy")))\
  .withColumn("trim",trim(col("dummy")))
#Result: "Hello.    " | "    Hello." | "Hello."

#using Spark SQL based functions
df.withColumn("ltrim",expr("ltrim(dummy)"))\
  .withColumn("rtrim",expr("rtrim('.',rtrim(dummy))"))\
  .withColumn("trim",trim(col("dummy")))
#Result: "Hello.    " | "   Hello" | "Hello."

#one last option using Spark SQL based functions
df.withColumn("ltrim",expr("LEADING ' ' FROM dummy "))\
  .withColumn("rtrim",expr("TRAILING '.' FROM rtrim(dummy)"))\
  .withColumn("trim",expr("BOTH ' ' FROM dummy"))
#Result: "Hello.    " | "   Hello" | "Hello."
```

### 4. Date and Time Manipulation Functions
#### The Basics
```python
l = [('X', )]
df = spark.createDataFrame(l).toDF("dummy")

#Get current Date and Time
from pyspark.sql.functions import current_date, current_timestamp

df.select(current_date()) #2022-06-29
df.select(current_timestamp()) #2022-06-29 21:27:10.23

#Converting STRING to Date or Time
from pyspark.sql.functions import lit, to_date, to_timestamp

df.select(to_date(lit('20220629','yyyyMMdd')).alias('to_date')) #2022-06-29
df.select(to_timestamp(lit('20220629 2120','yyyyMMdd HHmm')).alias('to_timestamp')) #2022-06-29 21:20:00
```

#### Date/Time Arithmetic
```python
datetimes = [
        ("2022-02-28", "2022-02-28 10:20:00.123")
    ]

datetimeDF = spark.createDataFrame(datetimes,schema="date STRING, time STRING")

#adding and substracting days
from pyspark.sql.functions import date_add, date_sub
datetimeDF.\
    withColumn("date_add_date",date_add("date",10)).\ #2022-03-10
    withColumn("date_add_time",date_add("time",10)).\ #2022-03-10
    withColumn("date_sub_date",date_sub("date",10)).\ #2022-02-18
    withColumn("date_sub_time",date_sub("time",10))   #2022-02-18

#difference between dates
from pyspark.sql.functions import datediff, current_date, current_timestamp
datetimeDF.\
    withColumn("datediff_date",datediff(current_date(),"date")).\
    withColumn("datediff_time",datediff(current_timestamp(),"time"))

#months operations
from pyspark.sql.functions import months_between, add_months, round
datetimeDF.\
    withColumn("months_between_d",round(months_between(current_date(),"date"),2)).\
    withColumn("months_between_t",round(months_between(current_timestamp(),"time"),2)).\
    withColumn("add_months_d",add_months("date",3)).\
    withColumn("add_months_t",add_months("time",3))
```

#### Trunc functions on Date/Time
```python
#using trunc
from pyspark.sql.functions import trunc

datetimeDF.\
    withColumn("date_trunc",trunc("date","MM")).\ #2022-02-01
    withColumn("time_trunc",trunc("time","yy"))   #2022-01-01

#using date_trunc
from pyspark.sql.functions import date_trunc

datetimeDF.\
    withColumn("date_dt",date_trunc("HOUR","date")).\ #2022-02-28 00:00:00
    withColumn("time_dt",trunc("HOUR","time")).\   #2022-02-28 10:00:00.00
    withColumn("time_dt1",trunc("dd","time")).\   #2022-02-28 00:00:00.00
```

#### Extracting Date/Time functions
```python
from pyspark.sql import functions
datetimeDF.\
    select(
        current_timestamp().alias('current_timestamp'),
        year(current_timestamp()).alias('year'),
        month(current_timestamp()).alias('month'),
        dayofmonth(current_timestamp()).alias('dayofmonth'),
        hour(current_timestamp()).alias('hour'),
        minute(current_timestamp()).alias('minute'),
        second(current_timestamp()).alias('second')
    )
```
#### Using `to_date` and `to_timestamp`
```python
from pyspark.sql.functions import to_date, to_timestamp

#to_date
df.select(
    to_date(lit('20220629'),'yyyyMMdd').alias('to_date'),
    to_date(lit('2022061'),'yyyyDDD').alias('to_date'), #2022-03-02
    to_date(lit('29/06/2022'),'dd/MM/yyyy').alias('to_date'),
    to_date(lit('29-Jun-2022'),'dd-MMM-yyyy').alias('to_date'),
    to_date(lit('29-June-2022'),'dd-MMMM-yyyy').alias('to_date')
)

#to_timestamp
df.select(
    to_timestamp(lit('29-Jun-2022 17:30:15'),'dd-MMM-yyyy HH:mm:ss').alias('to_date'),
    to_timestamp(lit('29-Jun-2022 17:30:15.123'),'dd-MMM-yyyy HH:mm:ss.SSS').alias('to_date')
)
```

#### Using `date_format`
```python
from pyspark.sql.functions import date_format
datetimeDF.\
    withColumn("day_format1",date_format("date","yyyyMMddHHmmss")).\
    withColumn("day_format2",date_format("date","MMM d, yyyy")).\
    withColumn("day_format3",date_format("date","EE")).\ #Fri
    withColumn("day_format4",date_format("date","EEEE")) #Friday
```

#### Using `unix_timestamp` and `from_unixtime`
Unix Time Format is an integer and started from January 1st 1970. We can convert from unix timestamp to regular date/timestamp and viceversa

**Important**: Spark 3.0 and later doesn't support conversion to unix_timestamp with format yyyy-MM-dd HH-mm-ss.SSS due to the 'SSS'.

```python
from pyspark.sql.functions import unix_timestamp, from_unixtime, col

#using unix_timestamp (converts from strint to timestamp)
datetimeDF.\
    withColumn("unix_date_id",unix_timestamp(col('dateString').cast('string'),"yyyyMMdd")).\
    withColumn("unix_date",unix_timestamp("date","yyyyMMdd")).\
    withColumn("unix_time",unix_timestamp("time")) #with format: yyyy-MM-dd HH:mm:ss

#using from_timestamp (convert from timestamp to string)
unixtime = [(139356180, )]
unixtimeDF = spark.createDataFrame(unixtime).toDF("unixtime")

unixtimeDF.\
    withColumn("date",from_unixtime("unixtime","yyyyMMdd")).\
    withColumn("time",from_unixtime("unixtime")) 
#both columns will be string
```

### 5. Dealing with Nulls
#### using `coalesce`
```python
from pyspark.sql.functions import coalesce,col,expr

students_df.\
    withColumn("gpa1",coalesce("gpa",lit(0))).\
    withColumn("gpa2",coalesce(col("gpa").cast("int"),lit(0))).\
    withColumn("gpa3",expr("nvl(gpa,0)")).\
    withColumn("gpa3",expr("nvl(nullif(gpa,''),0)")).\
```

### 6. Case / When statements
#### Option 1: using `expr`
```python
grades = [(1,20),(2,14),(3,15),(4,8)]
grades_df = spark.createDataFrame(grades,schema="id INT, grade INT")

from pyspark.sql.functions import expr

grades_df.\
    withColumn("grade_type",expr("""
                                    CASE WHEN grade between 0 and 10 then 'failed'
                                    WHEN grade between 11 and 14 then 'good! you can do it better'
                                    ELSE 'GOOD JOB!"""))
```

#### Option 2: using `when` and `otherwise`
```python
grades = [(1,20),(2,14),(3,15),(4,8)]
grades_df = spark.createDataFrame(grades,schema="id INT, grade INT")

from pyspark.sql.functions import when, lit, col

grades_df.\
    withColumn("grade_type",when(col("grade").between(0,10),lit("failed")).\
                            .when(col("grade").between(11,14),lit("need to study more")).\
                            .otherwise(lit("GOOD JOB!")))
```
---
## Filtering Spark Data Frames
###### Creating Data Frame
```python
import datetime
from pyspark.sql import Row

#Creating list of dictionaries
gamers = [
    {
        "user_id":1,
        "user_name":"Luis Miguel",
        "user_lastname":"Miranda",
        "contact_info":Row(mobile="+1 312 312 3132", home="+1 999 888 1233"),
        "favorite_videogames":["fifa 22","pes 22"],
        "birth_date":datetime.date(1993,3,1),
        "last_update":datetime.datetime(2021,2,10,1,14,0),
        "account_balance":100.10,
        "is_premium_customer":True,
        "nationality": "Peru"
    },
    {
        "user_id":2,
        "user_name":"Mario Alonso",
        "user_lastname":"Miranda",
        "contact_info":Row(home="+1 999 888 1233"),
        "favorite_videogames":["fifa 22","diablo immortal"]
        "birth_date":datetime.date(2000,6,27),
        "last_update":datetime.datetime(2021,2,10,1,14,0),
        "account_balance":999.21,
        "is_premium_customer":True,
        "nationality": "Peru"
    }
]

import panda as pd

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled',False)

gamers_df = spark.createDataFrame(pd.DataFrame(gamers))
```

### 1. Overview

1. To make filters we can use `where` and `filter` functions
2. Both are synonyms and we can use *SQL Style* or *Non SQL Style* filters
3. For *SQL Style* we have to use `col` function

Let´s see some examples
```python
from pyspark.sql.functions import col

#Using col function
gamers_df.filter(col("user_id") == 2)
gamers_df.where(col("user_id") == 2)

#Using string condition
gamers_df.filter("user_id == 2")
gamers_df.where("user_id == 2")

#Other way
gamers_df.createOrReplaceTempView("gamers_view")
spark.sql("""
        SELECT *
        FROM gamers_view
        WHERE gamer_id = 2
""")
```

### 2. Conditions and Operators
```python
# -- Equals condition --
gamers_df.filter(col("is_premium_customer") == True)
gamers_df.filter(col("user_name") == "Luis")
gamers_df.filter("is_premium_customer = False") #Spark SQL condition
gamers_df.createOrReplaceTempView("gamers_view")
spark.sql("""
        SELECT * FROM gamers_view
        WHERE is_premium_customer = False
    """)

# Validate if value is NaN
gamers_df.select('account_balance',isnan('account_balance'))

# -- Not Equals condition --
gamers_df.filter(col("account_balance")!=1000.0)
gamers_df.filter(col("account_balance").isNull())

gamers_df.filter((col('account_balance')!=1000.0) | (col('account_balance').isNull()))
gamers_df.filter((col('account_balance')!=1000.0) | (col('account_balance') != ''))
gamers_df.filter("account_balance != 1000.0 OR account_balance IS NULL")
                 
# -- Between condition --
gamers_df.filter(col("account_balance").between(500,1500))
gamers_df.filter("account_balance BETWEEN 500 AND 1500")
                 
# -- Null and Not Null Condition --
gamers_df.filter(col('is_premium_customer').isNull())
gamers_df.filter("is_premium_customer IS NULL")

gamers_df.filter(col('is_premium_customer').isNotNull())
gamers_df.filter("is_premium_customer IS NOT NULL")
                 
# -- Boolean Operators
gamers_df.filter((col('account_balance')!=1000.0) | (col('account_balance') != '')
gamers_df.filter("account_balance != 1000.0 OR account_balance IS NULL")
                 
# -- isIn Operator --
gamers_df.filter(col('nationality').isIn('Peru','Canada',''))#empty values
gamers_df.filter("nationality IN ('Peru','Canada','')")
gamers_df.filter("nationality IN ('Peru','Canada','',NULL)") #NULL will not work!
                 
# -- Greater Than / Less Than Operators
gamers_df.filter((col('account_balance')>100) & (isnan(col('account_balance'))==False))
gamers_df.filter((col('account_balance')<1000) & (isnan(col('account_balance'))==False))
gamers_df.filter((col('account_balance')>=100) & (isnan(col('account_balance'))==False))
gamers_df.filter((col('account_balance')<=1000) & (isnan(col('account_balance'))==False))
                 
gamers_df.filter('account_balance < 1000 AND account_balance IS NOT NULL')
gamers_df.filter('account_balance > 100 AND account_balance IS NOT NULL')
gamers_df.filter('account_balance <= 1000 AND account_balance IS NOT NULL')
gamers_df.filter('account_balance >= 100 AND account_balance IS NOT NULL')
                 
#Remember: AND (&) OR (|)
```

---
## Droping Columns and Rows of Spark Data Frames
### Overview
#### `drop` function
```python
from pyspark.sql.functions import drop

#dropping a single column
gamers_df.drop('last_updated_ts')
gamers_df.drop(col('last_updated_ts'))
gamers_df.drop(gamers_df['last_updated_ts'])
gamers_df.drop(col('date_of_birth')) #if doesn't exist, it will be ignored

#droping multiple columns
gamers_df.drop('first_name','last_name','email')
gamers_df.drop(col('first_name'),col('last_name'),col('email'))#this will fail - if some columns does not exist, they will be ignored

#droping using a list
columns_to_drop = ['first_name','last_name','email']
gamers_df.drop(columns_to_drop) #this will fail, because the parameter must be a string not a list
gamers_df.drop(*columns_to_drop)
```

#### `distinct` and `dropDuplicates` functions (for rows)
```python
#drop exact duplicates rows
gamers_df.distinct()

#drop duplicates based on a column or columns
gamers_df.dropDuplicates('id')#This will fail because parameter must be a seq type object (list or array)
gamers_df.dropDuplicates(['id'])
gamers_df.dropDuplicates(['id','first_name'])
```

#### Dropping Null based Record
```python
gamers_df.na.drop('all') #drop rows that all columns are null values
gamers_df.na.drop('any') #drop rows that at least 1 column is null
gamers_df.na.drop(tresh = 2) #drop rows that have less than 'tresh' non-null values (this overwrite the 'how' parameter)
gamers_df.na.drop(how = 'all',subset = ['id','email'])
gamers_df.na.drop(how = 'any',subset = ['id','email'])
```

---
## Sorting Data in Spark Data Frames
#### `sort` function 
```python
from pyspark.sql.functions import col

#sorting ascending order by a column
gamers_df.sort('first_name')
gamers_df.sort(gamers_df.first_name)
gamers_df.sort(gamers_df['first_name'])
gamers_df.sort(col('first_name'))

##using an array column
from pyspark.sql.functions import size
gamers_df.\
    select('id','first_name','last_name','favorite_videogames').\
    withColumn('no_of_videogames',size('favorite_videogames')).\
    sort('no_of_videogames')

##setting descending sorting
from pyspark.sql.functions import desc
gamers_df.sort('first_name',ascending=False)
gamers_df.sort(desc('first_name'))
gamers_df.sort(gamers_df['first_name'].desc())
gamers_df.\
    select('id','first_name','last_name','favorite_videogames').\
    withColumn('no_of_videogames',size('favorite_videogames')).\
    sort('no_of_videogames',ascending = False)
gamers_df.\
    select('id','first_name','last_name','favorite_videogames').\
    withColumn('no_of_videogames',size('favorite_videogames')).\
    sort(col('no_of_videogames').desc())
```

#### Dealing with Null values (feat `orderBy` function)

- If we have null values, with an ascending order, nulls go first
- If we have null values, with an descending order, nulls go last

```python
#other options to deal with natural ordering and 
gamers_df.orderBy(col('last_purchase_from').asc_nulls_last())
gamers_df.orderBy(col('last_purchase_from').desc_nulls_first())

gamers_df.orderBy(gamers_df['last_purchase_from'].asc_nulls_last())
gamers_df.orderBy(gamers_df['last_purchase_from'].desc_nulls_first())
```

#### Some other examples
```python
gamers_df.\
    sort(gamers_df['first_name'],gamers_df['last_name'].desc())

from pyspark.sql.functions import desc
gamers_df.\
    sort(gamers_df['first_name'],desc(gamers_df['last_name']))
gamers_df.\
    sort('first_name',desc('last_name'))
gamers_df.\
    sort(['first_name','last_name'],ascending = [1,0])
```

#### Prioritized Sorting on DataFrames
```python
from pyspark.sql.functions import col, when

when_statement = when(col('is_premium_customer')== True,0).otherwise(1)

gamers_df.orderBy(when_statement,col('first_name').desc())

#OR
from pyspark.sql.functions import expr

when_statement = expr("""
            CASE WHEN is_premium_customer = True THEN 0
                 ELSE 1 
            END
""")

gamers_df.sort(when_statements,col('first_name').desc())
```

## Performing Aggregations on Spark Data Frames

### Overview
What are the commom aggregate functions available?
- `count`
- `sum`
- `min`
- `max`
- `avg`

#### `count` function
```python
from pyspark.sql.functions import count
# count(*)
gamers_orders_df.select(count(*))
# count(*) using GroupBy
gamers_orders_df.groupBy('purchase_date').agg(count('*'))

# Function count on data frame is action. It will trigger execution.
gamers_orders_df.count()

# count is transformation (wide).
# Execution will be triggered when we perform actions such as show
gamers_orders_df.select(count("*")).show()
```

#### using `groupBy` function 
```python
#1st option
from pyspark.sql.functions import min, max, sum, avg
gamers_orders_df.groupBy('purchase_date').agg(min('total_amount')) #groupBy parameter could be a *lst_cols

lst_columns_group = ['purchase_date']
gamers_orders_df.groupBy(*lst_columns_group).agg(min('total_amount'))

#2nd option
gamers_orders_df.groupBy().min() #min() is a method from a GroupData Object 
gamers_orders_df.groupBy().min('total_amount') #min() just accepts numeric columns

lst_numeric_cols = ['id','total_amount']
gamers_orders_df.groupBy('purchase_date').min(*lst_numeric_cols)

#other way
gamers_orders_df_grouped = gamers_orders_df.groupBy('customer_id')

gamers_orders_df_grouped.\
            count().\
            withColumnRenamed('count','no_orders_per_customer_id')

gamers_orders_df_grouped.\
                sum()#it will sum only the numerical columns

gamers_orders_df_grouped.\
                avg('total_amount').\
                withColumnRenamed('avg(total_amount)','avg_total_amount')
```

#### `agg` function
```python
gamers_orders_df_grouped = gamers_orders_df.groupBy('customer_id')

gamers_orders_df_grouped = gamers_orders_df.groupBy('customer_id').\
        sum('total_amount','total_taxes')

gamers_orders_df_grouped.\
    agg(sum('total_amount'),sum('total_taxes'))

gamers_orders_df_grouped.\
    agg(sum('total_amount').alias('total_amount_per_id'),sum('total_taxes').alias('total_taxes_per_id'))

from pyspark.sql.functions import round

gamers_orders_df_grouped.\
    agg(sum('total_amount').alias('total_amount_per_id'),round(sum('total_taxes'),2).alias('total_taxes_per_id'))

gamers_orders_df_grouped.\
    agg({'total_amount':'sum','total_taxes':'sum'}).\
    toDF('customer_id','total_amount_per_customer','total_taxes_per_customer').\
    withColumn('total_amount_per_customer',round('total_amount_per_customer',2))

#we can apply filters before making the aggregations
gamers_orders_df.filter('videogame_genre == "ACTION"').agg(sum('total_amount'))
```
---
## Joining on Spark Data Frame

#### `join` function
```python
gamers_orders_df.\
    join(gamers_df, gamers_orders_df.customer_id == gamers_df.id)

gamers_orders_df.\
    join(gamers_df,'customer_id') #Only works when both df have the same column name to join

gamers_orders_df.\
    join(gamers_df,gamers_orders_df.customer_id == gamers_df.id).\
    select(gamers_orders_df['*'], gamers_df["first_name"],gamers_df["last_name"])

#using alias
gamers_orders_df.alias('go').\
    join(gamers_df.alias('g'),gamers_orders_df.customer_id == gamers_df.id).\
    select('go.*','g.first_name','g.last_name')

#using groupBy
from pyspark.sql.functions import count, concat, lit
gamers_orders_df.alias('go').\
    join(gamers_df.alias('g'),gamers_orders_df.customer_id == gamers_df.id).\
    select('go.*',concat('g.first_name',lit(','),'g.last_name').alias('full_name')).\
    groupBy('full_name').\
    count()

gamers_df.\
    withColumnRenamed('id','customer_id').\
    join(gamers_orders_df, 'customer_id').\
    groupBy(gamers_orders_df['customer_id']).\
    count()
```

#### outer `join` function
```python
gamers_orders_df.\
    join(gamers_df, gamers_orders_df.customer_id == gamers_df.id, 'left')

gamers_orders_df.\
    join(gamers_df, gamers_orders_df.customer_id == gamers_df.id, 'left_outer')

gamers_orders_df.\
    join(gamers_df, gamers_orders_df.customer_id == gamers_df.id, 'leftouter')

#Avoiding the nulls rows from the right table
gamers_df.alias('customer').\
    join(gamers_orders_df.alias('orders'),gamers_df.id == gamers_orders_df.customer_id,'left').\
    filter('orders.order_id IS NOT NULL').\
    select('orders.*','customer.first_name','customer.last_name')

#using groupBy and aggregations
from pyspark.sql.functions import sum, when
gamers_df.alias('customer').\
    join(gamers_orders_df.alias('orders'),gamers_df.id == gamers_orders_df.customer_id,'left').\
    groupBy('customer_id.id').\
    agg(sum(when(gamers_orders_df['order_id'].isNotNull(),1).otherwise(lit(0))).alias('total_orders')).\
    orderBy('customer_id.id')

#using expr
from pyspark.sql.functions import expr
gamers_df.alias('customer').\
    join(gamers_orders_df.alias('orders'),gamers_df.id == gamers_orders_df.customer_id,'left').\
    groupBy('customer_id.id').\
    agg(sum(expr(
            """
                CASE WHEN orders.order_id IS NULL THEN 0 ELSE 1 END
            """
        )).alias('total_orders')
    ).\
    orderBy('customer_id.id')
```
###### The same will work with using `right`,`right_outer` or `rightouter` on the third parameter 
#### `full` join function
```python
gamers_source1_df.\
    join(gamers_source2_df, 'email','full')

#Full outer join can be express as a left with right join united
gamers_source1_df.\
    join(gamers_source2_df, 'email', 'left').\
    union(
        gamers_source1_df.\
            join(gamers_source2_df, 'email', 'right')
    )

from pyspark.sql.functions import coalesce
gamers_source1_df.\
    join(gamers_source2_df, 'email', 'full').\
    select(
        coalesce(gamers_source1_df['first_name'],gamers_source2_df['first_name']).alias('first_name'),
        coalesce(gamers_source1_df['last_name'],gamers_source2_df['last_name']).alias('last_name')
    )

gamers_source1_df.alias('s1').\
    join(gamers_source2_df.alias('s2'), 'email', 'full').\
    select(
        coalesce('s1.first_name','s2.first_name').alias('first_name'),
        coalesce('s1.last_name','s2.last_name').alias('last_name')
    )
```

#### Broadcast join
Some important things to mention
- Also known as *map side* or *replication join*
- The **smaller data** is going to be broadcasted (replicated) to all workers (executors) in the cluster
- How to set the size of the smaller data to be broadcasted? Use `spark.sql.autoBroadcastJoinThreshold`
- What can we do if the size of the smaller data is greater than the parameter? Use `broadcast` function
- How can we disable this parameter? Set `spark.sql.autoBroadcastJoinThreshold` to 0
- Broadcaste disable == Reduce side join
```python
spark.conf.set('spark.sql.autoBroadcastJoinThreshold') #setting to 10MB
spark.conf.set('spark.sql.autoBroadcastJoinThreshold','0') #disabling broadcasting
spark.conf.set('spark.sql.autoBroadcastJoinThreshold','10485760') 

from pyspark.sql.functions import broadcast
broadcast(smaller_size_df).join(big_size_df,'column_to_join')
```

#### `crossJoin` function
Cross Join = Cartesian Product
```python
df1.\
    crossJoin(df2)

df1.\
    join(df2,how='cross')
```

---
## Reading Data from Files to Spark Data Frame
#### Basic Examples
```python
#Read CSV file
order_schema = """
            order_id INT,
            order_date TIMESTAMP,
            order_gamer_id INT,
            order_status STRING
         """

gamers_orders_df = spark.read.schema(order_schema).csv('/path/orders')

#Reading Json Files
gamers_orders_df = spark.read.json('/path/orders.json')
```

#### Trick: How to convert a json file to a parquet file with read/write in a while
```python
input_dir = '--' #directory of the inputs
outputs_dir = '--' #directory of the outputs

for file_details in dbutils.ls(input_dir):
    #if we are working on a git repository already cloned 
    #or
    #the file name is not a sql file (.sql)
    if not ('.git' in file_details.path | files_details.path.endswith('sql')):
        print(f'converting data in {file_details.path} folder from json to parquet')
        data_set_dir = file_details.path.split('/')[-2]
        #read the json file
        df_read = spark.read.json(file_details.path)
        #write to a parquet file
        df_read.coalesce(1).write.parquet(f'{output_dir}/{data_set_dir}', mode = 'overwrite')
```

#### Trick: How to convert a csv file with comma separator to csv file with pipe separator
```python
input_dir = '--' #directory of the inputs
outputs_dir = '--' #directory of the outputs

for file_details in dbutils.ls(input_dir):
    #if we are working on a git repository already cloned 
    #or
    #the file name is not a sql file (.sql)
    if not ('.git' in file_details.path | files_details.path.endswith('sql')):
        print(f'converting data in {file_details.path} folder from json to parquet')
        #read the json file
        df_read = spark.read.csv(file_details.path)
        #obtain folder name
        folder_name = file_details.path.split('/')[-2]
        #write to a parquet file
        df_read.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/{folder_name}',sep='|')
```

- `format` is to define the file input format and `load` is to define the file path
- Supported file formats: `csv`, `text`, `json`, `parquet`, `orc`
- Other commom files: `xml`, `avro`
- We can read compressed files

### 1 .Reading CSV Files
Some important things:
- We can explicitly specify the schema as `string` or using `StructType`.
- We can read csv that are delimited by different characters or symbols.
- Your csv has header? use the `header` parameter in the `options` function.
- `inferSchema`: spark will assume the column data types based on the file.

```python
#Alternative 1:
spark.read.csv('path')
#Alternative 2:
spark.read.format('csv').load('path')
```

#### Specifying schema
```python
schema_df = """Setting_schema (COLUMN DATATYPE,...)"""
#Alternative 1:
spark.read.schema(schema_df).csv('path_to_file')
#Alternative 2:
spark.read.csv('path_to_file',schema = schema_df)

#Using StructType and StructField and Spark Data Types
from pyspark.sql.functions import StructType, StructField, IntegerType, TimestampType, StringType

#Using StructType
schema_df = StructType([
                StructField('order_id',IntegerType(),nullable = False),
                StructField('order_date',TimestampType(),nullable = False),
                StructField('order_gamer_id',IntegerType(), nullable = False),
                StructField('order_status',StringType(), nullable = False)
            ]) 

spark.read.schema(schema_df).csv('path_to_file')
spark.read.csv('path_to_file',schema = schema_df)
```

#### Using `toDF` and `inferSchema`
```python
columns = ['order_id','order_date','order_gamer_id','order_status']

spark.read.option('inferSchema',True).csv('path_to_file')
#or
spark.read.csv('path_to_file',inferSchema = True)

spark.read.option('inferSchema',True).csv('path_to_file').toDF('order_id','order_dat','order_gamer_id','order_status')
#or
spark.read.option('inferSchema',True).csv('path_to_file').toDF(*columns)
```

#### Specifying delimiter
```python
schema_df = """Setting_schema (COLUMN DATATYPE,...)"""

spark.read.schema(schema_df).csv('path_to_file', sep = '|')
#or
spark.read.csv('path_to_file', sep = '|', schema = schema_df)
```

#### Using `option` and `options`
```python
columns = ['order_id','order_date','order_gamer_id','order_status']

orders = spark. \
            read. \
            csv(
                'path_to_file',
                header = None,
                inferSchema = True
            ).\
          toDF(*columns)

#using format
orders = spark. \
            read. \
            format('csv'). \
            load(
                'path_to_file',
                sep = '|',
                header = None,
                inferSchema = True
            ).\
          toDF(*columns)

#using option
orders = spark.\
         read.\
         option('sep','|'),
         option('header',None),
         option('inferSchema',True).\
         csv('path_to_file').\
         toDF(*columns)

#using options
orders = spark.\
         read.\
         options(sep = '|', header= None, inferSchema = True).\
         csv('path_to_file').\
         toDF(*columns)

#the options values can be set as a dictionary and can be used with **
options = {
    'sep':'|',
    'header':None,
    'inferSchema':True
}

orders = spark.\
         read.\
         options(**options).\
         csv('path_to_file').\
         toDF(*columns)
#or
orders = spark.\
         read.\
         options(**options).\ 
         format('csv').\
         load('path_to_file').\
         toDF(*columns)
```

### 2. Reading Json Files

#### Reading a Basic Json File
```python
orders = spark.read.json('path_to_file')
#or
orders = spark.read.format('json').load('path_to_file')
```
**You can use the same syntax for setting a schema (with string or Struct Type)**

### Is it reasonable to use inferSchema?
- The engine will need to make a preview read of the entire file just to infer the schema (this will cost time and processing)
- When we specified a schema, the data won't be read until the data frame is created.
- Schema can be inferred by default for files of Json, Parquet and Orc files (using metadata).
- inferSchema = True -> spark will generate default columns name (c0_, c1_, etc).
- inferSchema = True and csv contains header -> the column name are going to be inherited (if no header, we can use toDF function)

### 3. Reading Parquet Files
```python
orders = spark.read.parquet('path_to_file')
#or
orders = spark.read.format('parquet').load('path_to_file')
```

#### Some considerations for the schema
```python
schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""
spark.read.schema(schema).parquet('path_to_file')
# This will fail because INT64 is unsupported due to the size of the numeric values
```
```python
schema = """
    order_id BIGINT,
    order_date TIMESTAMP,
    order_customer_id BIGINT,
    order_status STRING
"""
spark.read.schema(schema).parquet('path_to_file')
# This will file due that we cannot cast timestamp from string of a parquet file
```
###### Best Solution:
```python
from pyspark.sql.functions import StructType, StructField, IntegerType, StringType
schema = StructType([
        StructField('order_id',IntegerType()),
        StructField('order_date',StringType()),
        StructField('order_gamer_id',IntegerType()),
        StructField('order_status',StringType()),
])

gamer_order_df = spark.read.schema(schema).parquet('path_to_file')
#Finally, we cast the string date to timestamp
from pyspark.sql.functions import col
gamer_order_df = gamer_order_df.withColumn('order_date',col('order_date').cast('timestamp'))
```
## Writing Files from Spark Data Frame to Files
### Overview
- We can use the direct API's like `json` and `csv` under `df.write`.
- Other way, we can use `format` and `save` under `df.write`.
- Same as read, we can use `option` and `options` command.
- Support file formats: `csv`,`json`,`text`,`parquet`,`orc` (other commom file formats: `xml`,`avro`)

**Some Considerations**
- Be sure about the permissions you grant to the targe location.
- Define is you are goint to overwrite or append properly.
- Put compression as a consideration.

**Mode Types**
- `overwrite`: overwrite the data
- `append`: append the data
- `ignore`: ignore the operation
- `errorifexists`: stop the process in runtime

### 1. Writing Spark Data Frame into CSV Files
```python

#create a data frame
from pyspark.sql.functions import Row
gamers = [{"Fill data"}]
gamers_df = spark.createDataFrame([Row(**gamer) for gamer in gamers])

#writing to csv files (basic)
gamers_df.write.csv('path_to_file')
#or
gamers_df.write.format('csv').save('path_to_file')

#if you want to check your files
dbutils.fs.ls('path_to_file')

#if you want to remove all the files inside the folder
dbutils.fs.rm('path_to_file',recurse=True)
```

#### Specifying header and write mode 1 just ONE file
```python
gamers_df.\
    coalesce(1).\
    write.\
    format('csv').\
    save('path_to_file',mode = 'overwrite', header = True)
```

#### Using Compression
```python
gamers_df.\
    coalesce(1).\
    write.\
    save(
        'path_to_file',
        mode = 'overwrite',
        compression = 'gzip',
        header = True
    )
#most commom: snappy
gamers_df.\
    coalesce(1).\
    write.\
    save(
        'path_to_file',
        mode = 'overwrite',
        compression = 'snappy',
        header = True
    )
```

#### Specifying delimiter
```python
df_read = spark.read.csv('path_to_file')

df_read.write.mode('overwrite').csv('path_to_file', sep = '|')
```

#### Specifying Writing options
```python
gamers_df.\
    coalesce(1).\
    write.\
    mode('overwrite').\
    option('compression','snappy').\
    option('header',True).\
    option('sep','|').\
    csv('path_to_file')

options = {
        'sep':'|',
        'header':True,
        'compression':'snappy'
        }

gamers_df.\
    coalesce(1).\
    write.\
    mode('overwrite').\
    options(**options).\
    csv('path_to_file')
```

### 2. Writing Spark Data Frame into Json Files
```python
#using the API
gamers_df.\
    coalesce(1).\
    write.\
    json(
        'path_to_file',
        mode = 'overwrite',
        compression = 'snappy'
    )
#using format
gamers_df.\
    coalesce(1).\
    write.\
    format('json').\
    save('path_to_file',mode='overwrite',compression = 'snappy')
```

### 3. Writing Spark Data Frame into Parquet Files
```python
#using the API
gamers_df.\
    coalesce(1).\
    write.\
    parquet(
        'path_to_file',
        mode = 'overwrite',
        compression = 'none'
    )
#using format
spark.conf.set('spark.sql.parquet.compression.codec','none')
gamers_df.\
    coalesce(1).\
    write.\
    format('parquet').\
    save('path_to_file',mode='overwrite')
```

So...
We see different ways to write into files
* `courses_df.write.mode(saveMode).file_format(path_to_folder)`
* `courses_df.write.file_format(path_to_folder, mode=saveMode)`
* `courses_df.write.mode(saveMode).format('file_format').save(path_to_folder)`
* `courses_df.write.format('file_format').save(path_to_folder, mode=saveMode)`

Remember that there 2 important modes to write (overwrite and append) ... there are more (ignore, errorifexists)

### You may notice a `coalesce` on every write...

- `coalesce` is used to reduced number of partitions to deal with as part of downstream processing (with this, we avoid shuffling)
- `repartition` is used to make a *fully shuffle*, the data to **higher or lower number of partitions**

#### Some useful things (with `coalesce`)
```python
df = spark.read.csv('path_to_bigfile', header = True, inferSchema = True) #for example this df has 93 partitioned files

#get number of partitions
df.rdd.getNumPartitions() 

#reducing to a number of partitions (just test purposes)
df.coalesce(16).rdd.getNumPartitions()

df.coalesce(186).rdd.getNumPartitions() #this won't work (spark will try to partition until the max size the data had - 93 )
```
#### Some useful things (with `repartition`)
```python
df.repartition(16).rdd.getNumPartitions() #this cost much time

#partition by columns
df.repartition(16,'Year','Month').rdd.getNumPartitions() #this cost much time
```

## Partitioning Spark Data Frames

Consider that the function `partitionBy` works for parquet and csv files (not for json files).

#### Partitioning by Single Column
```python
from pyspark.sql.functions import date_format

gamers_orders = spark.read.json('path_to_file')

#partition by Date
gamers_orders.\
    withColumn('order_date',date_format('order_date','yyyyMMdd')).\
    coalesce(1).\
    partitionBy('order_date').\
    parquet('path_to_file')

#partition by Month
gamers_orders.\
    withColumn('order_month',date_format('order_date','yyyyMM')).\
    coalesce(1).\
    partitionBy('order_month').\
    parquet('path_to_file')
```

#### Partitioning by Multiple Columns
```python
from pyspark.sql.functions import date_format

gamers_orders = spark.read.json('path_to_file')

#partition by Date
gamers_orders.\
    withColumn('order_date',date_format('order_date','dd')).\
    withColumn('order_month',date_format('order_date','MM')).\
    withColumn('order_year',date_format('order_date','yyyy')).\
    coalesce(1).\
    partitionBy('order_year','order_month','order_date').\
    parquet('path_to_file')
```

#### Reading Files that are partitioned 
```python
#using databricks datasets (airlines)
spark.read.csv('dbfs:/databrics-assets/asa/airlines',header = True).\
    filter('Year = 2004').\
    count() #7129270

spark.read.csv('dbfs:/databrics-assets/asa/airlines/Year=2004',header = True).count() #7129270

#other way
airline_df = spark.read.csv('dbfs:/databrics-assets/asa/airlines',header = True)
airline_df.createOrReplaceTempView("airlines")

spark.sql('''
    SELECT COUNT(1) FROM airlines WHERE year = 2004
    ''').show()
```

## Spark User Defined Functions (UDF)
Until now, we know that all SQL functions come from `pyspark.sql.functions`. However, a user can setup their own customize functions using UDFs.

**What are the steps?**
1. Develop the required logic using Python as programming language.
2. Register the function with `spark.udf.register` and assign it to a variable
3. Variable can be used as part of `select` or `filter` that come from Data Frame APIs
4. Remember, when we register a UDF, we register it with a name. That name can be used inside a `selectExpr` or `spark.sql` query.

### Registering and using a UDF function

```python
#Remember the order of the parameters for the udf.register
#(name_of_udf, function, return type)

#this UDF convert a date into a int value with the format YYYYMMDD
dc = spark.udf.register('date_convert', lambda d:
                        int(d[:10].replace('-','')))

#other way:
from pyspark.sql.types import IntegerType

def obtain_date (strVariable):
    return int(strVariable[:10].replace('-',''))

dc = spark.udf.register('date_convert',obtain_date, IntegerType())
```

### Invoking the udf function

#### using `select` function

```python
gamers_orders = spark.read.json('path_to_file')
gamers_orders.select(dc('order_date').alias('order_date'))
```

#### using `filter` function

```python
gamers_orders.filter(dc('order_date') == '20220708')
```

#### using `groupBy` function

```python
from pyspark.sql.functions import count
gamers_orders.\
    groupBy(dc('order_date').alias('order_date')).\
    count().\
    withColumnRenamed('count','order_count')
```

#### using `selectExpr` and `spark.sql`
```python
#selectExpr
gamers_orders.selectExpr("""order_id,
                            date_convert(order_date) as order_date""")
#spark.sql
gamers_orders.createOrReplaceTempView('orders')

spark.sql('''
    SELECT o.*, date_convert(order_date) as order_date 
    FROM orders AS o
''')
```

#### Another approach: data cleaning
```python
#define a function that clean data string (remove leading and trailing whitespaces)

def data_cleaning(c):
    return c.strip() if c.strip() != '\\N' else None

data_cleaning = spark.udf_register('data_cleaning',data_cleaning)

from pyspark.sql.functions import col
orders_df.select(
    data_cleaning(col('order_id')).alias('order_id'),
    data_cleaning(col('order_status')).alias('order_status')
)

orders_df.createOrReplaceTempView('orders')
spark.sql('''
    SELECT o.*,data_cleaning(o.order_status) AS order_status
    FROM orders AS o
''')
```

###### Reference
> Raju, D. Databricks Certified Associate Developer - Apache Spark 2022 [Online Course]. Udemy
> https://www.udemy.com/course/databricks-certified-associate-developer-for-apache-spark/



