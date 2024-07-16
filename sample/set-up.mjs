import MongoDatabaseConnection from '../lib/mongo/MongoDatabaseConnection.mjs'

let database = new MongoDatabaseConnection('mongodb://127.0.0.1:27017', 'test-database')
await database.connect()

let coll = database.getCollection('test-collection')
await coll.createDocument({
  hello: 'world'
})

console.log(await coll.findDocuments({}, { _id: false }).toArray())

await database.dropDatabase()
await database.close()