import DatabaseCollection from '../DatabaseCollection.mjs'
import { ObjectId } from 'mongodb'

export default class MongoDatabaseCollection extends DatabaseCollection {

  #collection

  constructor(mongoCollection, collectionName) {
    super(collectionName)
    this.#collection = mongoCollection
  }

  async createIndex(keys, options) {
    return await this.#collection.createIndex(keys, options)
  }

  async removeIndex(index) {
    return await this.#collection.dropIndex(index)
  }

  async createDocument(document) {
    return await this.#collection.insertOne(document)
  }

  async createDocuments(documents) {
    try {
      return await this.#createDocumentsAux(documents)
    } catch (e) {
      await new Promise(r => setTimeout(r, 5000))

      // Retry operation once, if it fails then let the error be thrown
      let { result } = e
      let insertIDs = Object.values(result.insertedIds)
      await this.deleteDocuments({ _id: { $in: insertIDs } })
      return await this.#createDocumentsAux(documents)
    }
  }

  async #createDocumentsAux(documents) {
    let bulk = this.#collection.initializeUnorderedBulkOp()
    documents.forEach(doc => bulk.insert(doc))
    return await bulk.execute()
  }

  findDocuments(query, projection, readConcern = {}) {
    let cursor = this.#collection.find(query, { readConcern })
    if (projection) cursor.project(projection)
    return cursor
  }

  async findDocument(query, projection, readConcern) {
    return await this.findDocuments(query, projection, readConcern).next()
  }

  async updateDocuments(query, update) {
    return await this.#collection.updateMany(query, update)
  }

  async updateDocument(query, update) {
    return await this.#collection.updateOne(query, update)
  }

  async replaceDocument(query, update, options) {
    return await this.#collection.replaceOne(query, update, options)
  }

  async deleteDocument(query) {
    return await this.#collection.deleteOne(query)
  }

  async deleteDocuments(query) {
    let bulk = this.#collection.initializeUnorderedBulkOp()
    bulk.find(query).delete()
    return await bulk.execute()
  }

  async distinct(field, query) {
    return await this.#collection.distinct(field, query, { cursor: {} })
  }

  async countDocuments(query) {
    return await this.#collection.countDocuments(query)
  }

  aggregate(pipeline) {
    return this.#collection.aggregate(pipeline)
  }

  async bulkWrite(operations) {
    return await this.#collection.bulkWrite(operations)
  }

  async dropCollection() {
    return await this.#collection.drop()
  }

  async explain(query) {
    return (await query.explain('executionStats')).executionStats
  }

  createObjectID(objectID) {
    if (objectID instanceof ObjectId) return objectID
    return ObjectId.createFromHexString(objectID)
  }
}
