import { MongoClient } from 'mongodb'

import MongoDatabaseCollection from './MongoDatabaseCollection.mjs'
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

  /**
   * Opens a connection to the database
   * 
   * @param {Object} options Options when connecting to the database
   */
  async connect(options) {
    let client = new MongoClient(this.#databaseURL, options)
    await client.connect()

    this.#client = client
    this.#database = client.db(this.#databaseName)
  }

  /**
   * Creates a new collection.
   * 
   * @param {string} collectionName The collection name
   * @param {Object} options Options for creating the collection
   * @returns {MongoDatabaseConnection} The collection that was created
   */
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

  /**
   * Gets an existing collection from the database.
   * 
   * @param {string} collectionName The collection name
   * @returns {MongoDatabaseCollection} The collection that was created
   */
  getCollection(collectionName) {
    return new MongoDatabaseCollection(this.#database.collection(collectionName), collectionName)
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
