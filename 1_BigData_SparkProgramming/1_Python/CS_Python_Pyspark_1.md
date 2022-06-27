# <span style='color:darkblue'>Pyspark Cheatsheet</span>
## The Basics

##### 1. Creating a Data Frame

###### From Integer List
```python    
#Defining a list with integer elements
lst_ages = [10,12,13,14]
#Creating df based on the integer list
df = spark.createDataFrame(lst_ages,"int")
```
###### From String List
```python    
#Defining a list with string elements
lst_names = ["luis","miguel","miranda"]
#Creating df based on the integer list
df = spark.createDataFrame(lst_names,"string")
```


