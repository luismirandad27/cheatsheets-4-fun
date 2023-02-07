# Introduction to MongoDB

- MongoDB database = set of databases
- database = set of multiple collections
- collection = different types of objects
- object == document

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

- Dynamic Schema (**fluent polymorphism**)

## Using MongoDB Server

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
test> use inventory
switched to db inventory
```
*MongoDB is case-sensitive*

Clear the prompt

```bash
test> cls
```

Show available databases
```bash
test> show dbs
admin   40.00 KiB
config  12.00 KiB
local   80.00 KiB
```

Creating a collection called `testCol` and insert one object
```bash
test> db.testCol.insertOne({x:1})
{
  acknowledged: true,
  insertedId: ObjectId("63e2be52822e5ceedcc6561e")
}
```

Retrieve the information stored in the collecting
```bash
test> db.testCol.find({})
[
  { _id: ObjectId("63e2be52822e5ceedcc6561e"), x: 1 },
  { _id: ObjectId("63e2bf05822e5ceedcc6561f"), x: 1, y: 2 }
]
test>
```

Insert multiple documents
```bash
testDB> db.inventory.insertMany( [
...  { item: "canvas", qty: 100, size: { h: 28, w: 35.5, uom: "cm" }, status: "A" },
...  { item: "journal", qty: 25, size: { h: 14, w: 21, uom: "cm" }, status: "A" },
...  { item: "mat", qty: 85, size: { h: 27.9, w: 35.5, uom: "cm" }, status: "A" },
...  { item: "mousepad", qty: 25, size: { h: 19, w: 22.85, uom: "cm" }, status: "P" },
...  { item: "notebook", qty: 50, size: { h: 8.5, w: 11, uom: "in" }, status: "P" },
...  { item: "paper", qty: 50, size: { h: 8.5, w: 11, uom: "in" }, status: "A" },
...  { item: "paper", qty: 100, size: { h: 8.5, w: 11, uom: "in" }, status: "D" },
...  { item: "planner", qty: 75, size: { h: 22.85, w: 30, uom: "cm" }, status: "D" },
...  { item: "postcard", qty: 45, size: { h: 10, w: 15.25, uom: "cm" }, status: "A" },
...  { item: "postcard", qty: 70, size: { h: 10, w: 15.25, uom: "cm" }, status: "D" },
...  { item: "sketchbook", qty: 80, size: { h: 14, w: 21, uom: "cm" }, status: "A" },
...  { item: "sketch pad", qty: 95, size: { h: 22.85, w: 30.5, uom: "cm" }, status: "A" }
... ] );
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId("63e2c1d2822e5ceedcc65620"),
    '1': ObjectId("63e2c1d2822e5ceedcc65621"),
    '2': ObjectId("63e2c1d2822e5ceedcc65622"),
    '3': ObjectId("63e2c1d2822e5ceedcc65623"),
    '4': ObjectId("63e2c1d2822e5ceedcc65624"),
    '5': ObjectId("63e2c1d2822e5ceedcc65625"),
    '6': ObjectId("63e2c1d2822e5ceedcc65626"),
    '7': ObjectId("63e2c1d2822e5ceedcc65627"),
    '8': ObjectId("63e2c1d2822e5ceedcc65628"),
    '9': ObjectId("63e2c1d2822e5ceedcc65629"),
    '10': ObjectId("63e2c1d2822e5ceedcc6562a"),
    '11': ObjectId("63e2c1d2822e5ceedcc6562b")
  }
}
```
Counting Documents
```bash
testDB> db.inventory.countDocuments({})
12
```

Delete One Document
```bash
testDB> db.inventory.deleteOne({ status: "P" })
{ acknowledged: true, deletedCount: 1 }
```
Make filters
```bash
testDB> db.inventory.find({status:"D"})
[
  {
    _id: ObjectId("63e2c3cd822e5ceedcc6563e"),
    item: 'paper',
    qty: 100,
    size: { h: 8.5, w: 11, uom: 'in' },
    status: 'D'
  },
  {
    _id: ObjectId("63e2c3cd822e5ceedcc6563f"),
    item: 'planner',
    qty: 75,
    size: { h: 22.85, w: 30, uom: 'cm' },
    status: 'D'
  },
  {
    _id: ObjectId("63e2c3cd822e5ceedcc65641"),
    item: 'postcard',
    qty: 70,
    size: { h: 10, w: 15.25, uom: 'cm' },
    status: 'D'
  }
]
```
Using operators
```bash
testDB> db.inventory.find( { $and: [ {status:"A"},{ qty: {$lt:30}} ] } )
[
  {
    _id: ObjectId("63e2c3cd822e5ceedcc65639"),
    item: 'journal',
    qty: 25,
    size: { h: 14, w: 21, uom: 'cm' },
    status: 'A'
  }
]
testDB>
```
Implicit `and`
```bash
testDB> db.inventory.find( { status : "A",qty:{$lt:30} } )
[
  {
    _id: ObjectId("63e2c3cd822e5ceedcc65639"),
    item: 'journal',
    qty: 25,
    size: { h: 14, w: 21, uom: 'cm' },
    status: 'A'
  }
]
```
*There is no implicit OR*

Pattern Matching
```bash
testDB> db.inventory.find( { item: /ket/ } ) // Contains Ket
[
  {
    _id: ObjectId("63e2c3cd822e5ceedcc65642"),
    item: 'sketchbook',
    qty: 80,
    size: { h: 14, w: 21, uom: 'cm' },
    status: 'A'
  },
  {
    _id: ObjectId("63e2c3cd822e5ceedcc65643"),
    item: 'sketch pad',
    qty: 95,
    size: { h: 22.85, w: 30.5, uom: 'cm' },
    status: 'A'
  }
]
```

Update First Document
```bash
testDB> db.inventory.updateOne(
... { item: "paper" },
... { $set: { "size.uom": "cm", status: "P" } },
... );
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
```


Show all collections
```bash
test> show collections
testCol
```
Drop a collection
```bash
test> db.testCol.drop()
true
```
Display the current db
```bash
test> db
test
```