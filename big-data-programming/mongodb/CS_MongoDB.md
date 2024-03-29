# Introduction to MongoDB

### Some concepts:

- MongoDB database: set of databases
- database: set of multiple collections
- collection: different types of objects
- object: document
- Dynamic Schema (**fluent polymorphism**)

### Document Representation

```json
{
    name: "Sue",
    age: 26,
    status: "A",
    groups: ["news","sports"]
}
//{} -> document
//[] -> arrays
```

---

## Using MongoDB Server
<br/>

Start MongoDB by using brew services
```bash
brew services start mongodb-community
```

To stop MongDB by using brew services
```bash
brew services stop mongodb-community
```

On terminal
```bash
> mongosh
```

Switch to a database (you dont have to create a new one)

```bash
use inventory
```
*MongoDB is case-sensitive*

Clear the prompt

```bash
cls
```

Show available databases
```bash
show dbs
-----------------
admin   40.00 KiB
config  12.00 KiB
local   80.00 KiB
```
## Collections
Creating a collection called `funCol` and insert one object
```bash
db.funCol.insertOne({attribute_name:"Hello World"})
```

Retrieve the information stored in the collecting with `find`
```
db.testCol.find({})
```

## Inserting Data
<br/>

Insert one document with `insertOne`
```bash
db.gameInventory.insertOne(
{ game: "Fifa 23", stock: 100, tags:["multiplayer","soccer"], details: { year: 2022, company: "EA", rating: 4.5 }, status: "On Sale" });
```
Insert multiple documents with `insertMany`
```bash
db.gameInventory.insertMany( [
{ game: "Fifa 23", stock: 100, tags:["multiplayer","soccer"], details: { year: 2022, company: "EA", rating: 4.5 }, status: "On Sale", storeId:[1,2,3] },
{ game: "Battlefield 2042", stock: 25, tags:["multiplayer","shooter"], details: { year: 2021, company: "EA", rating: 4.2 }, status: "On Sale", storeId:[5,8,9] },
{ game: "Call of Duty MW 2", stock: 85, tags:["multiplayer","shooter"], details: { year: 2022, company: "Activision", rating: 5 }, status: "On Sale", storeId:[1,7,9] },
{ game: "Crash Bandicoot", stock: 0, tags:["adventure","arcade"], details: { year: 2019, company: "Naughty Dog", rating: 3.9 }, status: "Out of Stock", storeId:[4,5,6] },
{ game: "Stray", stock: 0, tags:["adventure","futuristic"], details: { year: 2022, company: "BlueTwelve Studio", rating: 4.5 }, status: "Out of Stock", storeId:[8,9,10] }
] );
```
## Querying Documents
<br/>

Retrieving all documents
```
db.gameInventory.find({})
```
Single filter condition
```bash
db.inventory.find({status:"On Sale"})
```
*The Comparison Operators*
| Operator | Description                                                         |
| -------- | ------------------------------------------------------------------- |
| $eq      | Matches values that are equal to a specified value.                 |
| $gt      | Matches values that are greater than a specified value              |
| $gte     | Matches values that are greater than or equal to a specified value. |
| $in      | Matches any of the values specified in an array.                    |
| $lt      | Matches values that are less than a specified value.                |
| $lte     | Matches values that are less than or equal to a specified value.    |
| $ne      | Matches all values that are not equal to a specified value.         |
| $nin     | Matches none of the values specified in an array                    |

*Logical Operators*
| Operator | Description                                                                                             |
| -------- | ------------------------------------------------------------------------------------------------------- |
| $and     | Joins query clauses with a logical AND returns all documents that match the conditions of both clauses. |
| $not     | Inverts the effect of a query expression and returns documents that do not match the query expression.  |
| $nor     | Joins query clauses with a logical NOR returns all documents that fail to match both clauses.           |
| $or      | Joins query clauses with a logical OR returns all documents that match the conditions of either clause. |

Making multiple conditions
```bash
db.gameInventory.find({$and:[{status:"On Sale"},{stock:{$lt:30}}]})
::could be like this too
db.gameInventory.find({status:"On Sale", stock:{$lt:30}})
```
Making multiple conditions **on the same field**
```bash
db.gameInventory.find({$and:[{stock:{$gt:30}},{stock:{$lt:50}}]})
::could be like this too
db.gameInventory.find({stock:{$gt:30,$lt:50}})
```
Including multiple logical operators
```bash
db.gameInventory.find({
  status:"On Sale",
  $or:{[{stock:{$gt:30}},{details.year:{$gt:2018}}]}
})
```
Regular Expression Patterns
| Pattern | Description                                                           |
| ------- | --------------------------------------------------------------------- |
| .       | Matches any character except a newline character.                     |
| \*      | Matches zero or more occurrences of the preceding character or group. |
| +       | Matches one or more occurrences of the preceding character or group.  |
| ?       | Matches zero or one occurrence of the preceding character or group.   |
| [ ]     | Matches any character within the brackets.                            |
| [^ ]    | Matches any character not within the brackets.                        |
| \\w     | Matches any word character (alphanumeric character plus underscore).  |
| \\W     | Matches any non-word character.                                       |
| \\d     | Matches any digit character.                                          |
| \\D     | Matches any non-digit character.                                      |
| \\s     | Matches any whitespace character (space, tab, newline, etc.).         |
| \\S     | Matches any non-whitespace character.                                 |
| ^       | Matches the start of the string.                                      |
| $       | Matches the end of the string.                                        |
| \\b     | Matches a word boundary.                                              |
| \\B     | Matches a non-word boundary.                                          |

Example:
```bash
db.gameInventory.find({item:/ttle/}) //contains ttle
```
```bash
db.gameInventory.find({item:/d$/}) //ends with d
```
```bash
db.gameInventory.find({item:{$in:[/^m/,/^p/]}}) //starts with m or p
```
### Sorting Documents
<br/>

```bash
db.gameInventory.find({}).sort({stock:1, status: -1})
```

### Embedded/Nested Documents
<br/>

Filter with equility match (requiring an exact match of the specified value document, **including the field older**)
```bash
db.gameInventory.find({detail:{year:2022, company:"EA",rating:4}})
```
Nested Fields -> equality match
```bash
db.gameInventory.find({"details.year":2022})
```
Nested Fields -> **AND**
```bash
db.gameInventory.find({"detail.year":{$lt:2019},"detail.company":"EA", status:"On Sale"})
```
Nested Fields -> **OR**
```bash
db.gameInventory.find({$or:[{"detail.year":{$lt:2019}},{"detail.company":"EA"},{status:"On Sale"}]})
```
### Querying Array
<br/>

Filtering where *value* is the exact array to match, **include the order of elements**
```bash
db.gameInventory.find({tags:["multiplayer","shooter"]})
```
Filtering where the array contains the elements, **without regard to order the elemens in the array**
```bash
db.gameInventory.find({tags:{$all:["multiplayer","shooter"]}})
```
Multiple conditions with arrays #1:
One element can satisfy the first condition, another element (maybe the same one) can satisfy the second condition
```bash
db.gameInventory.find({storeId:{$gt:5,$lt8}})
```
Multiple conditions with arrays #2:
The element must match between both conditions
```bash
db.gameInventory.find({storeId:{$elemMatch:{$gt:5, $lt:8}}})
```
Query for an element by the array index position (**array starts with index 0**)
```bash
db.gameInventory.find({"storeId.1":{$gt:1}})
```
Query by the size of the array
```bash
db.gameInventory.find({tags:{$size:2}})
```

### Nested Document in an Array
<br/>

```bash
db.gameInventory.insertMany( [
{ game: "Fifa 23", tags:["multiplayer","soccer"], details: { year: 2022, company: "EA", rating: 4.5 }, status: "On Sale", stockInfo:[{store: "A", stock: 5}]},
{ game: "Battlefield 2042", tags:["multiplayer","shooter"], details: { year: 2021, company: "EA", rating: 4.2 }, status: "On Sale",stockInfo:[{store: "A", stock: 5},{store: "B", stock: 10}] },
{ item: "Call of Duty MW 2", tags:["multiplayer","shooter"], details: { year: 2022, company: "Activision", rating: 5 }, status: "On Sale", stockInfo:[{store: "A", stock: 8},{store: "B", stock: 2},{store:"C", stock:3}]},
{ item: "Crash Bandicoot", tags:["adventure","arcade"], details: { year: 2019, company: "Naughty Dog", rating: 3.9 }, status: "Out of Stock",stockInfo:[] },
{ item: "Stray", tags:["adventure","futuristic"], details: { year: 2022, company: "BlueTwelve Studio", rating: 4.5 }, status: "Out of Stock", stockInfo:[] }
] );
```
Query based on nested document in an Array -> **order matters!**
```bash
db.gameInventory.find({stockInfo:{store:"A",stock:5}})
```
Adding operators
```bash
db.gameInventory.find({"stockInfo.stock":{$lte: 5}})
```
Multiple conditions for a nested document
```bash
db.gameInventory.find({stockInfo:{$elemMatch:{stock:10,store:"B"}}})
```
```bash
db.gameInventory.find({stockInfo:{$elemMatch:{stock:{$gt:5,$lte:10}}}})
```
### NULL or Missing Fields
<br/>

By using BSON Null Type
```bash
db.gameInventory.find({status:{$type:10}})
```
To look for documents that do not contain an specific field
```bash
db.gameInventory.find({status:{$exists:false}})
```
### Project Fields to return from query
<br/>

To include item and details fields
```bash
db.gameInventory.find({status:"On Sale"},{item:1,details:1})
```
To excluede details field
```bash
db.gameInventory.find({status:"On Sale"},{details:0})
```
To include embedded documents
```bash
db.gameInventory.find({status:"On Sale"},{item:1, status:1, "details.year":1})
```

Remember to not add exclusions if you are including inclusion projection (error)

### Distinct
```bash
db.gameInventory.distinct("details.company")
```
---
## Aggregation Pipeline
Let's say that we have the following documents

```bash
db.gamesInventory.insertMany( [
{ game: "Fifa 23", company:"EA", price:89.90, stock: [ { store: "A", qty: 5 }, { store: "C", qty: 45 } ], status:"A" },
{ game: "Battlefield V",company:"EA", price:99.90, stock: [ { store: "C", qty: 75 } ], status:"A" },
{ game: "Stray", company:"BlueTwelve Studio", price:120.90, stock: [ { store: "A", qty: 60 }, { store: "B", qty: 15 } ], status:"I" },
{ game: "Resident Evil 4", company:"Konami", price:130.90, stock: [ { store: "A", qty: 40 }, { store: "B", qty: 5 } ], status:"A" },
{ game: "Dead Island 2", company:"Deep Silver", price:150.90 ,stock: [ { store: "B", qty: 10 }, { store: "C", qty: 35 } ], status:"A" }
] )
```

**Aggregations with 2 stages**
```bash
db.gamesInventory.aggregate([
                      { $match: {status: "A"} },
                      { $group: { _id: "$game", total: { $sum: "$price"}} }
])
```

Each element of the array inside the `aggregate([])` are called **Stages**:
- Stage 1: the `$match` stage filters the documents by the field
- Stage 2: the `$group` stage will group the documents based `game` and sum the `price` field.

Let's translate this into SQL:
```sql
SELECT game, sum(price)
FROM gamesInventory
WHERE status = 'A'
GROUP BY game
```

**Aggregation with Multiple Fields**
```bash
db.gamesInventory.aggregate([
    {$group: {
        _id: {customer_id:"$game", status:"$status"},
        total: {$sum: "$price"}
      }
    }
])
```
*The `_id` refers to the GROUP BY section*
<br/>
SQL translation
```sql
SELECT game, status, sum(amount)
FROM gamesInventory
GROUP BY game, status
```

**Aggregation with 3 stages example (Sorting)**
```bash
db.gamesInventory.aggregate([
  {$match:  {status:"A"}},
  {$group:  {_id:"$game",total:{$sum:"$price"}}},
  {$sort:   {_id:1}}
])
```

**Aggregation to count the number of elements (before mongodb 5)**
```bash
db.gamesInventory.aggregate([
    {$match:{status:"A"}},
    {$group:{_id:null, order_count:{$sum:1}}}
])
```
**Aggregation to count the number of elements (with mongodb 5)**
```bash
db.gamesInventory.aggregate([
    {$match:{status:"A"}},
    {$group:{_id:null, order_count:{$count:{}}}}
])
```

Keep in mind that `count` does not require any parameter. By the way, we have another operations like $min, $max, $avg


### `$unwind`
Operator for array handling

Calculating the total quantity by game
```bash
db.gamesInventory.aggregate([
  {$unwind: "$stock"},
  {$group: {_id:"$game",total_stock:{$sum:"$stock.qty"}}}
])
```

You don't want to use `unwind`? Let's use `project` instead
```bash
db.inventory.aggregate([
    {$project: { _id:0, game:1, total_stock:{$sum:"$stock.qty"} }}
]);
```

--- 
## Some Aggregation Examples
Find the games from EA and Konami that are available at more than two stores. Display the game name and company fields, as well as the total qty of all available stores.

```bash
db.gamesInventory.aggregate([
  {$match: {
    company:{$in:["EA","Konami"]},
  }},
  {$project: {
    _id:0,
    game:1,
    company:1,
    num_stores:{$size:"$stock"},
    total_qty:{$sum:"$stock.qty"}
  }},
  {$match: {
    num_stores:{$gt:2}
  }},
  {
  $project: {
    num_stores:0
  }}
]);

```
Find each store that has at least 10 Fifa 23 games. Display the store name and the total qty of all Fifa 23 items at the store and sort the result in descending order of the total qty. The result is given below for your reference (the field name and order must be the same)

```bash
db.gamesInventory.aggregate([
    {$match: {
      game:"Fifa 23"
    }},
    {$unwind: "$stock"},
    {$group: {
      _id: "$stock.store",
      total_qty: {
        $sum: "$stock.qty"
      }
    }},
    {$match: {
      total_qty:{$gte:45}
    }},
    {$project: {
      store:"$_id",
      total_qty:"$total_qty",
      _id:0
    }},
    {$sort: {
      total_qty: -1
    }}
]);

```

Find the game(s) that have the lowest total qty at all the stores. The result is given below for your reference (the field name and order must be the same).

```bash
db.gamesInventory.aggregate([
    {$project: {
      game:1,
      total_qty:{$sum:"$stock.qty"}
    }},
    {$group: {
      _id: "$total_qty",
      games: {
        $push: "$$ROOT.game"
      }
    }},
    {$project: {
      _id:0,
      games:1,
      total_qty:"$_id"
    }},
    {$sort: {total_qty: 1}},
    {$limit: 1}
]);
```
*Here we are applying the $push aggregation operator, this will
change the perspective of the document when we group by the total_qty*

```bash
[ { games: [ 'Resident Evil 4', 'Dead Island 2' ], total_qty: 45 } ]
```

Note:
- You may use project when you want to make any aggregation for each document.
- But you may use unwind and group when you want to based on any field across all the documents

## Bonus Track: `$addToSet`

Based on the MongoDB 5.0's documentation this operation helps to add a value to an array unless the value is already present.

For example:
Give the list of all stock qty for each store for every game

```bash
db.gamesInventory.aggregate([
    {$unwind: "$stock"},
    {$group:{
        _id:"$stock.store",
        stocks:{
            $addToSet:"$stock.qty"
        }
    }}
])
```
The result will look like this:
```bash
[
  { _id: 'B', stocks: [ 15, 5, 10 ] },
  { _id: 'C', stocks: [ 35, 45, 75 ] },
  { _id: 'A', stocks: [ 5, 60, 40 ] }
]
```

## More operations available in the Mongo DB's [documentation](https://www.mongodb.com/docs/manual/reference/operator/):

---

### Follow me on:
<img src="https://cdn-icons-png.flaticon.com/512/2175/2175377.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> GitHub: https://github.com/luismirandad27

&nbsp;
<img src="https://cdn-icons-png.flaticon.com/512/5968/5968933.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> Medium: https://medium.com/@lmirandad27

&nbsp;
<img src="https://cdn-icons-png.flaticon.com/512/145/145807.png" alt="Markdown Monster icon" style="height:20px;width:20px;border-radius:5px"/> LinkedIn: https://www.linkedin.com/in/lmirandad27/
