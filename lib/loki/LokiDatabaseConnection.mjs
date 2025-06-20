import DatabaseConnection from '../DatabaseConnection.mjs'
import LokiDatabaseCollection from './LokiDatabaseCollection.mjs'
import Loki from 'lokijs'

export default class LokiDatabaseConnection extends DatabaseConnection {
  loki

  constructor(databaseName, filename) {
    super()
    if (filename) {
      this.loki = new Loki(filename)
    } else {
      this.loki = new Loki(databaseName, {
        env: 'BROWSER'
      })
    }
  }

  /**
   * Opens a connection to the database
   * 
   * @param {Object} options Options when connecting to the database
   */
  connect(options) {
    return new Promise(resolve => {
      this.loki.loadDatabase({}, resolve)
    })
  }

  /**
   * Creates a new collection.
   * 
   * @param {string} collectionName The collection name
   * @returns {LokiDatabaseCollection} The collection that was created
   */
  async createCollection(collectionName) {
    return this.getCollection(collectionName)
  }

  /**
   * Gets an existing collection from the database.
   * 
   * @param {string} collectionName The collection name
   * @returns {LokiDatabaseCollection} The collection that was created
   */
  getCollection(collectionName) {
    let collection = this.loki.getCollection(collectionName)
    if (!collection) collection = this.loki.addCollection(collectionName)
    return new LokiDatabaseCollection(collection)
  }

  /**
   * Saves the database to the disk
   */
  saveDatabase() {
    return new Promise(resolve => {
      this.loki.saveDatabase(resolve)
    })
  }

  close() {
    this.loki.close()
  }
}
