import { MongoClient } from 'mongodb'

import DatabaseCollection from './MongoDatabaseCollection.mjs'
import DatabaseConnection from '../DatabaseConnection.mjs'

export default class MongoDatabaseConnection extends DatabaseConnection {

  #databaseURL
  #databaseName
  #client
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
    let client = new MongoClient(this.#databaseURL, options)
    await client.connect()

    this.#client = client
    this.#database = client.db(this.#databaseName)
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

  dropDatabase() {
    return this.#database.dropDatabase()
  }

  adminCommand(command) {
    return this.#database.executeDbAdminCommand(command)
  }

  runCommand(command) {
    return this.#database.command(command)
  }

  async close() {
    return this.#client.close()
  }
}
