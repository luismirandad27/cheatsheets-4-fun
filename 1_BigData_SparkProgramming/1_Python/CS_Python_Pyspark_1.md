# Pyspark Cheatsheet

## Creating of a Pyspark Data Frame

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
lst_names = ["luis","miguel","miranda"]
#Creating df based on the integer list
df = spark.createDataFrame(lst_names,"string")
```
#### Using Pyspark Data Types
```python
#Importing pyspark data types library
from pyspark.sql.types import StringType,IntegerType
```
```python
#Creating df based on the integer list
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

lst_users = [(1,'Scoot'),(2,'Donald'),(3,'Mickey')]
df_users = spark.createDataFrame(lst_users,"user_id int, user_name string")
```

### 3. Creating Data Frame using Pyspark `Row`
```python
#Importing Pyspark Row
from pyspark.sql import Row
```

#### Convert List of List to Data Frame
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
    {'user_id':1, 'user_name':'luismi'},
    {'user_id':2, 'user_name':'miranda'},
    {'user_id':3, 'user_name':'dulanto'}
            ]
rows_user = [Row(**user) for user in lst_users]
spark.createDataFrame(rows_user)
```

### 4. Pyspark Data Types
#### Defining a list of dictionaries
```python
import datetime
```
```python
users = [
    {
        "id": 1,
        "first_name": "Luis",
        "last_name" : "Miranda",
        "email": "lmirandad27@gmail.com",
        "is_customer": True,
        "amount_paid": 1200.38,
        "customer_from": datetime.date(2021,1,15),
        "last_updated_ts": datetime.datetime(2021,2,10,1,15,0)
    },
    {
        "id": 2,
        "first_name": "Gianella",
        "last_name" : "Palacios",
        "email": "gianella194@hotmail.com",
        "is_customer": False,
        "amount_paid": 1290.38,
        "customer_from": datetime.date(2022,10,15),
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
#### Showing columns and data types
```python
users_df.columns
#['id','first_name','last_name','email','is_customer','amount_paid','customer_from','last_updated_ts']
users_df.dtypes
'''
    [
        ('id','bigint'),
        ('first_name','string'),
        ('last_name','string'),
        ('email','string'),
        ('is_customer','boolean'),
        ('amount_paid','double'),
        ('customer_from','date'),
        ('last_updatet_ts','timestamp')
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
    is_customer BOOLEAN,
    amount_paid FLOAT,
    customer_from DATE,
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
    StructField('is_customer',BooleanType()),
    StructField('amount_paid',FloatType()),
    StructField('customer_from',DateType()), StructField('last_updated_ts',TimestampType()),
])

spark.createDataFrame(users,schema = user_schema)
```

### 5. Creating Data Frame with Pandas
#### Defining a List of dictionaries
```python
import datetime
```
```python
users = [
    {
        "id": 1,
        "first_name": "Luis",
        "last_name" : "Miranda",
        "email": "lmirandad27@gmail.com",
        "is_customer": True,
        "amount_paid": 1200.38,
        "customer_from": datetime.date(2021,1,15),
        "last_updated_ts": datetime.datetime(2021,2,10,1,15,0)
    },
    {
        "id": 2,
        "first_name": "Gianella",
        "last_name" : "Palacios",
        "email": "gianella194@hotmail.com",
        "is_customer": False,
        "amount_paid": 1290.38,
        "customer_from": datetime.date(2022,10,15),
        "last_updated_ts": datetime.datetime(2022,8,10,1,15,0)
    }
]
```
#### Creating Data Frame with Pandas
```python
import pandas as pd
```
```python
users_df = spark.createDataFrame(pd.DataFrame(users))
```

### 6. Special Data Types
- Special Types: Array, Struct, Map
- List and Dicts can be implicitly converted to Spark ARRAY and MAP respectively

#### `Array` data type

```python
import datetime
```
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
```
```python
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
import datetime
```
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
```
```python
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
import datetime
```
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
```
```python
#using col

users_df.\
    select('id','phone_numbers.mobile','phone_numbers.home')

users_df.\
    select('id','phone_numbers.*')

users_df.\
    select('id',col('phone_numbers')['mobile'].alias('mobile'),col('phone_numbers')['home'].alias('home'))
```

---
## Selecting and Renaming DataFrames
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
        "total_amount":100.10
    },
    {
        "user_id":2,
        "user_name":"Mario Alonso",
        "user_lastname":"Miranda",
        "contact_info":Row(home="+1 999 888 1233"),
        "videogames":["fifa 22","diablo immortal"]
        "birth_date":datetime.date(2000,6,27),
        "last_update":datetime.datetime(2021,2,10,1,14,0),
        "total_amount":999.21
    }
]
```
###### Disabling Apache Arrow in Pyspark for Pandas To/From Conversion
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled",False)
```

###### Creating DataFrame
```python
gamers_df = spark.createDataFrame(pd.DataFrame(gamers))
```

### Narrow/Wide Transformations

Data structures are *immutable*, we cannot change them once created. The way we can modify this DataFrame is with *transformations*. There are 2 types:

**Narrow Transformations**: doesn't result in *shuffling*. Than means that all the transformation is operating in the each partition (any data movement will not occur). Also, Spark will make a process called *pipelining*, that means that every narrow operation will be executed *in-memory*.

**Wide Transformations**: result in *shuffling*. Means that, in the operation, many input partitions are being used to create different output partitions (writing on disk).

In the following table, we could see the functions available for each type:



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
#Combining all variations
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

#### More about `lit` function (type Pyspark Object Column)
```python
gamers_df.select('total_amount'+25) #ERROR: not same type

gamers_df.select('total_amount'+'25') #ERROR: there isn´t any column called total_amount25

gamers_df.select('user_id','total_amount'+lit(2.0),lit(2.0)+lit(2.0))
# 1 | Null | 4.0
# 2 | Null | 4.0
# Null result because Spark cannot performe arithmetics operation on noncompatible types

#Right Way
gamers_df.select(col('total_amount') + lit(2.0)) #same type
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

### 2. Renaming Pyspark DataFrame Columns (or Expressions)
What are the ways that we can rename columns/expressions:
- Using `alias` on `select`.
- Add or rename column using `withColumn` (*row-level transformation*).
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
## Manipulating Columns
### 1. Categories of Functions

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
    * `CASE`/`WHEN`, `cast` and others...

### 2. How to get some help on Spark Functions?
#### Use `help`
```python
help(date_format)
```

#### *Remember*...
There are some functions that need to be called with a parameter as a Column Pyspark Type and not just a simple string, for example...
```python
#Using upper and desc for the example
from pyspark.sql.functions import upper, desc, col, lit

students = [
            (1,"Luis","Miranda",3.4,"peru","+1 236 999 9999"),
            (2,"Gianella","Palacios",3.8,"peru","+1 778 999 9999"),
            (3,"Mario","Miranda",3.4,"peru","+51 999 999 999")
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
To remove unnecessart characters from fixed length records.
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

#### Using `unix_timestamp` and `from_timestamp`
It is an integer and started from January 1st 1970. We can convert from unix timestamp to regular date/timestamp and viceversa

Important observation: Spark 3.0 and later doesn't support conversion to unix_timestamp with format yyyy-MM-dd HH-mm-ss.SSS due to the 'SSS'

```python
from pyspark.sql.functions import unix_timestamp, from_timestamp, col

#using unix_timestamp
datetimeDF.\
    withColumn("unix_date_id",unix_timestamp(col('dateString').cast('string'),"yyyyMMdd")).\
    withColumn("unix_date",unix_timestamp("date","yyyyMMdd")).\
    withColumn("unix_time",unix_timestamp("time")) #with format: yyyy-MM-dd HH:mm:ss

#using from_timestamp
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

### 6. CASE / WHEN
#### Option 1: using `expr`
```python
grades = [(1,20),(2,14),(3,15),(4,8)]
grades_df = spark.createDataFrame(grades,schema="id INT, grade INT")

from pyspark.sql.functions import expr

grades_df.\
    withColumn("grade_type",expr("""
                                    CASE WHEN grade between 0 and 10 then 'failed'
                                    WHEN grade between 11 and 14 then 'need to study more'
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
## Filtering Pyspark DataFrame
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
        "videogames":["fifa 22","pes 22"],
        "birth_date":datetime.date(1993,3,1),
        "last_update":datetime.datetime(2021,2,10,1,14,0),
        "total_amount":100.10,
        "is_videogame_fan":True,
        "nationality": "Peru"
    },
    {
        "user_id":2,
        "user_name":"Mario Alonso",
        "user_lastname":"Miranda",
        "contact_info":Row(home="+1 999 888 1233"),
        "videogames":["fifa 22","diablo immortal"]
        "birth_date":datetime.date(2000,6,27),
        "last_update":datetime.datetime(2021,2,10,1,14,0),
        "total_amount":999.21,
        "is_videogame_fan":True,
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
gamers_df.filter(col("is_videogame_fan") == True)
gamers_df.filter(col("user_name") == "Luis")
gamers_df.filter("is_videogame_fan = 'false'") #Spark SQL condition
gamers_df.createOrReplaceTempView("gamers_view")
spark.sql("""
        SELECT * FROM gamers_view
        WHERE is_videogame_fan = 'false'
    """)

# Validate if value is NaN
gamers_df.select('total_amount',isnan('total_amount'))

# -- Not Equals condition --
gamers_df.filter(col("total_amount")!=1000.0)
gamers_df.filter(col("total_amount").isNull())

gamers_df.filter((col('total_amount')!=1000.0) | (col('total_amount').isNull()))
gamers_df.filter((col('total_amount')!=1000.0) | (col('total_amount') != ''))
gamers_df.filter("total_amount != 1000.0 OR total_amount IS NULL")
                 
# -- Between condition --
gamers_df.filter(col("total_amount").between(500,1500))
gamers_df.filter("total_amount BETWEEN 500 AND 1500")
                 
# -- Null and Not Null Condition --
gamers_df.filter(col('is_videogame_fan').isNull())
gamers_df.filter("is_videogame_fan IS NULL")

gamers_df.filter(col('is_videogame_fan').isNotNull())
gamers_df.filter("is_videogame_fan IS NOT NULL")
                 
# -- Boolean Operators
gamers_df.filter((col('total_amount')!=1000.0) | (col('total_amount') != '')
gamers_df.filter("total_amount != 1000.0 OR total_amount IS NULL")
                 
# -- isIn Operator --
gamers_df.filter(col('nationality').isIn('Peru','Canada',''))#empty values
gamers_df.filter("nationality IN ('Peru','Canada','')")
gamers_df.filter("nationality IN ('Peru','Canada','',NULL)") #NULL will not work!
                 
# -- Greater Than / Less Than Operators
gamers_df.filter(col('total_amount')>100 & isnan(col('total_amount'))==False)
gamers_df.filter(col('total_amount')<1000 & isnan(col('total_amount'))==False)
gamers_df.filter(col('total_amount')>=100 & isnan(col('total_amount'))==False)
gamers_df.filter(col('total_amount')<=1000 & isnan(col('total_amount'))==False)
                 
gamers_df.filter('total_amount < 1000 AND total_amount IS NOT NULL')
gamers_df.filter('total_amount > 100 AND total_amount IS NOT NULL')
gamers_df.filter('total_amount <= 1000 AND total_amount IS NOT NULL')
gamers_df.filter('total_amount >= 100 AND total_amount IS NOT NULL')
                 
#Remember: AND (&) OR (|)
```

###### Reference
> Raju, D. Databricks Certified Associate Developer - Apache Spark 2022 [Online Course]. Udemy
> https://www.udemy.com/course/databricks-certified-associate-developer-for-apache-spark/



