import DatabaseCollection from '../DatabaseCollection.mjs'
import { ObjectId } from 'mongodb'

export default class MongoDatabaseCollection extends DatabaseCollection {

  #collection

  constructor(mongoCollection, collectionName) {
    super(collectionName)
    this.#collection = mongoCollection
  }

  createIndex(keys, options) {
    return this.#collection.createIndex(keys, options)
  }

  removeIndex(index) {
    return this.#collection.dropIndex(index)
  }

  createDocument(document) {
    return this.#collection.insertOne(document)
  }

  async createDocuments(documents) {
    try {
      return this.#createDocumentsAux(documents)
    } catch (e) {
      // Retry operation once, if it fails then let the error be thrown
      let { result } = e
      let insertIDs = Object.values(result.insertedIds)
      await this.deleteDocuments(insertIDs.map(id => ({ _id: id })))
      return this.#createDocumentsAux(documents)
    }
  }

  #createDocumentsAux(documents) {
    let bulk = this.#collection.initializeUnorderedBulkOp()
    documents.forEach(doc => bulk.insert(doc))
    return bulk.execute()
  }

  findDocuments(query, projection) {
    let cursor = this.#collection.find(query)
    if (projection) cursor.project(projection)
    return cursor
  }

  findDocument(query, projection) {
    return this.findDocuments(query, projection).next()
  }

  updateDocuments(query, update) {
    return this.#collection.updateMany(query, update)
  }

  updateDocument(query, update) {
    return this.#collection.updateOne(query, update)
  }

  replaceDocument(query, update, options) {
    return this.#collection.replaceOne(query, update, options)
  }

  deleteDocument(query) {
    return this.#collection.deleteOne(query)
  }

  deleteDocuments(query) {
    let bulk = this.#collection.initializeUnorderedBulkOp()
    bulk.find(query).delete()
    return bulk.execute()
  }

  distinct(field, query) {
    return this.#collection.distinct(field, query, { cursor: {} })
  }

  countDocuments(query) {
    return this.#collection.countDocuments(query)
  }

  aggregate(pipeline) {
    return this.#collection.aggregate(pipeline)
  }

  bulkWrite(operations) {
    return this.#collection.bulkWrite(operations)
  }

  dropCollection() {
    return this.#collection.drop()
  }

  async explain(query) {
    return (await query.explain('executionStats')).executionStats
  }

  createObjectID(objectID) {
    if (objectID instanceof ObjectId) return objectID
    return ObjectId.createFromHexString(objectID)
  }
}
