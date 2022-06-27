# Pyspark Cheatsheet
## The Basics: Creating Data Frames

##### 1. Creating a Data Frame from List

###### From Integer List
```python=
#Defining a list with integer elements
lst_ages = [10,12,13,14]
#Creating df based on the integer list
df = spark.createDataFrame(lst_ages,"int")
```
###### From String List
```python=
#Defining a list with string elements
lst_names = ["luis","miguel","miranda"]
#Creating df based on the integer list
df = spark.createDataFrame(lst_names,"string")
```
###### Using Pyspark Data Types
```python=
#Importing pyspark data types library
from pyspark.sql.types import StringType,IntegerType
#Creating df based on the integer list
df_ages = spark.createDataFrame(lst_ages,IntegerType())
df_names = spark.createDataFrame(lst_names,StringType())
```
##### 2. Creating a Multi-Column Data Frame using List
```python=
lst_ages_mc = [(12,),(24,),(10,)]

df_ages = spark.createDataFrame(lst_ages_mc)
##C: DataFrame[_1: bigint]
df_ages = spark.createDataFrame(lst_ages_mc,'age int')
##C: DataFrame[age: int]

lst_users = [(1,'Scoot'),(2,'Donald'),(3,'Mickey')]
df_users = spark.createDataFrame(lst_users,"user_id int, user_name string")
```

##### 3. Creating Data Frame using Pyspark Row
```python=
#Importing Pyspark Row
from pyspark.sql import Row
```

###### Convert List of List to Data Frame
```python=
lst_users = [[1,'Scoot'],[2,'Ronald'],[3,'Mathew']]
rows_user = [Row(*user) for user in lst_users]
spark.createDataFrame(rows_user,'user_id int, user_name string')
##C: DataFrame[user_id: int, user_name: string]
```

###### Convert List of Tuples to Data Frame
```python=
lst_users = [(1,'Scoot'),(2,'Ronald'),(3,'Mathew')]
rows_user = [Row(*user) for user in lst_users]
spark.createDataFrame(rows_user,'user_id int, user_name string')
##C: DataFrame[user_id: int, user_name: string]
```

###### Convert List of Dicts to Data Frame
```python=
lst_users = [
    {'user_id':1, 'user_name':'luismi'},
    {'user_id':2, 'user_name':'miranda'},
    {'user_id':3, 'user_name':'dulanto'}
            ]
rows_user = [Row(**user) for user in lst_users]
spark.createDataFrame(rows_user)
```

##### 4. Pyspark Data Types
###### Defining a list of dictionaries
```python=
import datetime

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
###### Creating Data Frame
```python=
users_df = spark.createDataFrame([Row(**user) for user in users])
```
###### Showing the data structure
```python=
users_df.printSchema()
```
###### Showing first N rows
```python=
users_df.show()
```
###### Showing columns and data types
```python=
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
###### Specifying Schema of Data Frame (I)
```python=
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
###### Specifying Schema of Data Frame (II)
```python=
from pyspark.sql.types import *

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

##### 5. Creating Data Frame with Pandas
###### Defining a List of dictionaries
```python=
import datetime

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
###### Creating Data Frame with Pandas
```python=
import pandas as pd
users_df = spark.createDataFrame(pd.DataFrame(users))
```

##### 6. Special Data Types
- Special Types: Array, Struct, Map
- List and Dicts can be implicitly converted to Spark ARRAY and MAP respectively

###### Array data type

```python=
import datetime

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

###### Array Type: Using explode/explode_outer
```python=
#Using explode and explode_outer
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, explode_outer

#using col

users_df.\
    select('id',col('phone_numbers')[0].alias('mobile')),col('phone_numbers')[1].alias('home').\
    show()

#explode -> ignore null or empty values
users_df.\
    withColumn('phone_numbers',explode('phone_numbers')).\
    drop('phone_numbers').\
    show()

#explode_outer -> not ignore null or empty values
users_df.\
    withColumn('phone_numbers',explode_outer('phone_numbers')).\
    drop('phone_numbers').\
    show()
```

###### Map Type
```python=
import datetime

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

###### Map Type: Using explode/explode_outer
```python=
#Using explode and explode_outer
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, explode_outer

#using col

users_df.\
    select('id',col('phone_numbers')['mobile'].alias('mobile')),col('phone_numbers')['home'].alias('home').\
    show()

#explode -> ignore null or empty values
users_df.\
    select('id',explode('phone_numbers')).\
    withColumnRenamed('key','phone_type').\
    withColumnRenamed('value','phone_number').\
    show()

#explode_outer -> not ignore null or empty values
users_df.\
   select('id',explode_outer('phone_numbers')).\
    withColumnRenamed('key','phone_type').\
    withColumnRenamed('value','phone_number').\
    show()
```

###### Struct Type
```python=
import datetime

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

###### Struct Type: Using col (explode and explode_outer cannot be used)
```python=
#Using explode and explode_outer
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, explode_outer

#using col

users_df.\
    select('id','phone_numbers.mobile','phone_numbers.home').\
    show()

users_df.\
    select('id','phone_numbers.*').\
    show()

users_df.\
    select('id',col('phone_numbers')['mobile'].alias('mobile'),col('phone_numbers')['home'].alias('home')).\
    show()
```