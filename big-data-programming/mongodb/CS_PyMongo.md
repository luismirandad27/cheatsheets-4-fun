# PyMongo

It's a Python distribution ideally for working with MongoDB.

Let's see the basics here!

## Installing the Pymongo Library
```bash
python3 -m pip install pymongo
```

## Working on Pymongo

### 1. Importing pymongo in Python code
```python
import pymongo
```

### 2. Setting the Mongo Database
```python
# Getting Mongo client
mongo_client = pymongo.MongoClient()

# Accessing an specific database
db_games = mongo_client['games-db']
```

### 3. Getting an specific Mongo Collection
```python
game_collection = db_games['gamesInventory']
```

### 4. Basic Operations

#### **Deleting** the entire array of documents in the collection
```python
print('Deleting all documents')
result_info = game_collection.delete_many()
# Displaying the total of documents deleted
print('Total documents deleted: ', result_info.deleted_count)
```
#### **Inserting** an array of documents in the collection
```python
# Prepare the array of documents
games_array = [
{ game: "Fifa 23", company:"EA", price:89.90, stock: [ { store: "A", qty: 5 }, { store: "C", qty: 45 } ], status:"A" },
{ game: "Battlefield V",company:"EA", price:99.90, stock: [ { store: "C", qty: 75 } ], status:"A" },
{ game: "Stray", company:"BlueTwelve Studio", price:120.90, stock: [ { store: "A", qty: 60 }, { store: "B", qty: 15 } ], status:"I" },
{ game: "Resident Evil 4", company:"Konami", price:130.90, stock: [ { store: "A", qty: 40 }, { store: "B", qty: 5 } ], status:"A" },
{ game: "Dead Island 2", company:"Deep Silver", price:150.90 ,stock: [ { store: "B", qty: 10 }, { store: "C", qty: 35 } ], status:"A" }
]

# Inserting the array in the collection
print("Inserting Documents")
result_info = game_collection.insert_many(games_array)

# Displaying the total of documents inserted
print("Total documents inserted: ", len(result_info.inserted_ids))

# Displaying the ids created
for document_id in result_info.inserted_ids:
    print("ID: ",document_id)
```

#### Using `find`

```python
# Find documents with status A and price greater than 100.0
find_dict = {
    "status":"A",
    "price":{"$gt":100.00}
}

# You can omit the filter keyword
result_info = game_collection.find(filter = find_dict)

# Display the results
for document in result_info:
    print(document)
```

The result should look like this:
```bash
{'_id': ObjectId('643f200fb1372d24956079b2'), 'game': 'Resident Evil 4', 'company': 'Konami', 'price': 130.9, 'stock': [{'store': 'A', 'qty': 40}, {'store': 'B', 'qty': 5}], 'status': 'A'}
{'_id': ObjectId('643f200fb1372d24956079b3'), 'game': 'Dead Island 2', 'company': 'Deep Silver', 'price': 150.9, 'stock': [{'store': 'B', 'qty': 10}, {'store': 'C', 'qty': 35}], 'status': 'A'}
```

#### Using `find` and `project`
```python
# Find documents with status A and price greater than 100.0
find_dict = {
    'status':'A',
    'price':{'$gt':100.00}
}

# Display only the name of the game
project_dict = {
    '_id': 0,
    'game': 1
}

# You can omit the filter and projection keywords
result_info = game_collection.find(filter = find_dict, projection = project_dict)

# Display the results
for document in result_info:
    print(document)
```
The result should look like this:
```bash
{'game': 'Resident Evil 4'}
{'game': 'Dead Island 2'}
```

#### Using aggregation pipelines
```python
# Find the game(s) that have the lowest total qty at all the stores. 
# The result is given below for your reference (the field name and 
# order must be the same).

my_pipeline = [
    {'$project':{
        'game':1,
        'total_qty':{'$sum':'$stock.qty'}
    }},
    {'$group':{
        '_id':'$total_qty',
        'games':{
            '$push':'$$ROOT.game'
        }
    }},
    {'$sort':{
        '_id':1
    }},
    {'$limit':1}
]

# You can omit the pipeline keyword
result_info = game_collection.aggregate(pipeline = my_pipeline)

# Display results
for document in result_info:
    print(document)
```
The result should look like this:
```bash
{'_id': 45, 'games': ['Resident Evil 4', 'Dead Island 2']}
```

---

### Follow me on:
<img src="https://cdn-icons-png.flaticon.com/512/2175/2175377.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> GitHub: https://github.com/luismirandad27

&nbsp;
<img src="https://cdn-icons-png.flaticon.com/512/5968/5968933.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> Medium: https://medium.com/@lmirandad27

&nbsp;
<img src="https://cdn-icons-png.flaticon.com/512/145/145807.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> LinkedIn: https://www.linkedin.com/in/lmirandad27/