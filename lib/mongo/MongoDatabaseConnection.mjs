import { MongoClient } from 'mongodb'

import MongoDatabaseCollection from './MongoDatabaseCollection.mjs'
import DatabaseConnection from '../DatabaseConnection.mjs'
import MongoDatabaseDebugCollection from './MongoDatabaseDebugCollection.mjs'

export default class MongoDatabaseConnection extends DatabaseConnection {

  #databaseURL
  #databaseName
  #client
  #database

  #triggerThreshold

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
  async connect(options = {}) {
    let client = new MongoClient(this.#databaseURL, options)
    this.#client = client
    await client.connect()

    this.#database = client.db(this.#databaseName)

    client.on('serverHeartbeatFailed', event => this.emit('serverHeartbeatFailed', event))
  }

  /**
   * Creates a new collection.
   * 
   * @param {string} collectionName The collection name
   * @param {Object} options Options for creating the collection
   * @returns {MongoDatabaseCollection} The collection that was created
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
   * Enables debugging on the database connection.
   * 
   * @param {function} triggerThreshold The function called when a query has a high a ratio of documents examined:returned
   */
  enableDebugging(triggerThreshold) {
    this.#triggerThreshold = triggerThreshold
  }

  /**
   * Disables debugging on the database connection. Collections previously retrieved are not affected
   */
  disableDebugging() { this.#triggerThreshold = null }

  /**
   * Gets an existing collection from the database.
   * 
   * @param {string} collectionName The collection name
   * @returns {MongoDatabaseCollection} The collection that was created
   */
  getCollection(collectionName) {
    if (this.#triggerThreshold) {
      return new MongoDatabaseDebugCollection(this.#database.collection(collectionName), collectionName, this.#triggerThreshold)
    }
    return new MongoDatabaseCollection(this.#database.collection(collectionName), collectionName)
  }

  dropDatabase() {
    return this.#database.dropDatabase()
  }

  adminCommand(command) {
    return this.#database.admin().command(command)
  }

  runCommand(command) {
    return this.#database.command(command)
  }

  async ping() {
    await this.runCommand({ ping: 1 })
  }

  async close() {
    return this.#client.close()
  }

  async getCollectionNames() {
    return (await this.#database.listCollections().toArray()).filter(coll => coll.type === 'collection').map(coll => coll.name)
  }

  async dumpToLoki(targetDB, collectionNames) {
    for (let collectionInfo of collectionNames) {
      let collection = await this.getCollection(collectionInfo.name)
      let outputCollection = await targetDB.getCollection(collectionInfo.name)

      let cursor = await collection.findDocuments(collectionInfo.query, collectionInfo.projection || {})
      for await (let doc of cursor) {
        await outputCollection.createDocument(JSON.parse(JSON.stringify(doc)))
      }
    }
  }
}
