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
}