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

---

### Follow me on:
<img src="https://cdn-icons-png.flaticon.com/512/2175/2175377.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> GitHub: https://github.com/luismirandad27

&nbsp;
<img src="https://cdn-icons-png.flaticon.com/512/5968/5968933.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> Medium: https://medium.com/@lmirandad27

&nbsp;
<img src="https://cdn-icons-png.flaticon.com/512/145/145807.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> LinkedIn: https://www.linkedin.com/in/lmirandad27/