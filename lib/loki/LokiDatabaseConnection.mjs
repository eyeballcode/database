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

  connect(options) {
    return new Promise(resolve => {
      this.loki.loadDatabase({}, resolve)
    })
  }

  async createCollection(collectionName) {
    this.loki.addCollection(collectionName)
    return this.getCollection(collectionName)
  }

  getCollection(collectionName) {
    return new LokiDatabaseCollection(this.loki.getCollection(collectionName))
  }

  saveDatabase() {
    return new Promise(resolve => {
      this.loki.saveDatabase(resolve)
    })
  }

  close() {
    this.loki.close()
  }
}
