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

  async createIndex(keys, options) {

  }

  async createDocument(document) {

  }

  async createDocuments(documents) {

  }

  async findDocument(query) {

  }

  async findDocuments(query) {

  }

  async updateDocument(query, update) {

  }

  async updateDocuments(query, update) {
    
  }

  async replaceDocument(query, update, options) {

  }

  async deleteDocument(query) {

  }

  async deleteDocuments(query) {

  }

  async distinct(field, query) {

  }

  async countDocuments(query) {

  }

  async dropCollection() {

  }
}