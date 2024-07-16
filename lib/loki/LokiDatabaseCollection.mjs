import DatabaseCollection from '../DatabaseCollection.mjs'

export default class LokiDatabaseCollection extends DatabaseCollection {
  constructor (collection) {
    super()
    this.collection = collection
  }

  createIndex (keys, options) {
    return this.collection.ensureIndex(keys)
  }

  createDocument (document) {
    return this.collection.insert(document)
  }

  createDocuments (documents) {
    return this.collection.insert(documents)
  }

  transformQuery(query) {
    for (let key of Object.keys(query)) {
      if (query[key].exec) {
        query[key] = {
          $regex: query[key]
        }
      }
    }
    return query
  }

  findDocuments (query) {
    return {
      toArray: () => this.collection.find(this.transformQuery(query))
    }
  }

  findDocument (query) {
    return this.collection.findOne(this.transformQuery(query))
  }

  dropCollection() {
    return this.collection.clear()
  }
}
