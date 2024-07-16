import { MongoClient } from 'mongodb'

import DatabaseCollection from './MongoDatabaseCollection.mjs'
import DatabaseConnection from '../DatabaseConnection.mjs'

export default class MongoDatabaseConnection extends DatabaseConnection {

  #databaseURL
  #databaseName
  #database

  /**
   * Constructs a new connection to the MongoDB database.
   * 
   * @param {string} databaseURL The MongoDB database connection string
   * @param {string} databaseName The database name
   */
  constructor(databaseURL, databaseName) {
    super()
    this.#databaseURL = databaseURL
    this.#databaseName = databaseName
  }

  async connect(options) {
    if (callback == null) {
      callback = options
    }

    await new Promise((resolve, reject) => {
      MongoClient.connect(this.#databaseURL, {
          ...options,
          useNewUrlParser: true,
          useUnifiedTopology: true
      }, (err, client) => {
        if (err) return reject(err)
        this.#database = client.db(this.#databaseName)

        resolve()
      })
    })
  }

  async createCollection(collectionName, options) {
    await this.#database.createCollection(collectionName, {
      ...options,
      storageEngine: {
        wiredTiger: {
          configString: 'block_compressor=zstd,prefix_compression=true'
        }
      }
    })

    return this.getCollection(collectionName)
  }

  getCollection(collectionName) {
    return new DatabaseCollection(this.#database.collection(collectionName), collectionName)
  }

  async adminCommand(command) {
    return await this.#database.executeDbAdminCommand(command)
  }

  async runCommand(command) {
    return await this.#database.command(command)
  }
}
