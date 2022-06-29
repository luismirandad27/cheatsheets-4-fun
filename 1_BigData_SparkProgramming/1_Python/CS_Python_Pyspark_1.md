# Pyspark Cheatsheet

## Creation of a Pyspark Data Frame

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
## Selecting and Renaming DataFrames <a name="introduction"></a>
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
###### Reference
> Raju, D. Databricks Certified Associate Developer - Apache Spark 2022 [Online Course]. Udemy
> https://www.udemy.com/course/databricks-certified-associate-developer-for-apache-spark/



