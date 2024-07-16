import DatabaseCollection from '../DatabaseCollection.mjs'

export default class MongoDatabaseCollection extends DatabaseCollection {
  constructor(mongoCollection, collectionName) {
    this.collection = mongoCollection
    this.name = collectionName
  }

  async createIndex(keys, options) {
    return await this.collection.createIndex(keys, options)
  }

  async createDocument(document) {
    return await this.collection.insertOne(document)
  }

  async createDocuments(documents) {
    return await this.collection.insertMany(documents)
  }

  async findDocuments(query, projection) {
    return await this.collection.find(query, projection)
  }

  async findDocument(query, projection, callback) {
    return await this.collection.findOne(query, projection, callback)
  }

  async updateDocuments(query, update) {
    return await this.collection.updateMany(query, update)
  }

  async updateDocument(query, update) {
    return await this.collection.updateOne(query, update)
  }

  async replaceDocument(query, update, options) {
    return await this.collection.replaceOne(query, update, options)
  }

  async deleteDocument(query) {
    return await this.collection.deleteOne(query)
  }

  async deleteDocuments(query) {
    let bulk = this.collection.initializeUnorderedBulkOp()
    bulk.find(query).delete()
    return await bulk.execute()
  }

  async distinct(field, query) {
    return await this.collection.distinct(field, query, { cursor: {} })
  }

  async countDocuments(query) {
    return await this.collection.countDocuments(query)
  }

  async aggregate(pipeline) {
    return await this.collection.aggregate(pipeline)
  }

  async bulkWrite(operations) {
    return await this.collection.bulkWrite(operations)
  }

  async dropCollection() {
    return await this.collection.drop()
  }

  async explain(query) {
    return (await query.explain('executionStats')).executionStats
  }
}
