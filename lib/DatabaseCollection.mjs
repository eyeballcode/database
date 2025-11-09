export default class DatabaseCollection {
  #name

  constructor(collectionName) {
    this.#name = collectionName
  }

  /**
   * 
   * @returns {string} The collection name
   */
  getCollectionName() {
    return this.#name
  }

  createIndex(keys, options) {

  }

  removeIndex(index) {

  }

  createDocument(document) {

  }

  createDocuments(documents) {

  }

  findDocument(query) {

  }

  findDocuments(query) {

  }

  updateDocument(query, update) {

  }

  updateDocuments(query, update) {
    
  }

  replaceDocument(query, update, options) {

  }

  deleteDocument(query) {

  }

  deleteDocuments(query) {

  }

  distinct(field, query) {

  }

  countDocuments(query) {

  }

  dropCollection() {

  }

  bulkWrite(operations) {

  }

  createObjectID(objectID) {
  }

  /**
   * Retrieve all documents matching a query in batches. Useful when retrieving a large number of documents at once.
   * 
   * @param {object} query The underlying query
   * @param {number} batchSize The number of documents in each batch processed
   * @param {function} callback A function to process the documents in each batch
   */
  async batchQuery(query, batchSize, callback) {
    let curMaxID = null
    let iter = 0

    while (true) {
      const {
        documents,
        lastSeenID
      } = await this.#getBatch(batchSize, query, curMaxID)
      curMaxID = lastSeenID
      if (documents.length === 0) break

      await callback(documents, iter++)
    }
  }

  async #getBatch(batchSize, baseQuery, lastSeenID) {
    const query = lastSeenID ? {
      ...baseQuery,
      _id: {
        $gt: lastSeenID
      }
    } : baseQuery

    const documents = await this.findDocuments(query).sort({ _id: 1 }).limit(batchSize).toArray()

    return {
      documents, lastSeenID: documents.length > 0 ? documents[documents.length - 1]._id : null
    }
  }
}