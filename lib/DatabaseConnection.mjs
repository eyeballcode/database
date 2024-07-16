export default class DatabaseConnection {

  /**
   * Opens a connection to the database
   * 
   * @param {Object} options Options when connecting to the database
   */
  async connect(options) {

  }

  /**
   * Creates a new collection or table. Should do nothing if the collection already exists.
   * 
   * @param {string} collectionName The collection name
   * @param {Object} options Options for creating the collection
   * @returns {DatabaseCollection} The collection that was created
   */
  async createCollection(collectionName, options) {

  }

  /**
   * Gets an existing collection from the database.
   * 
   * @param {string} collectionName The collection name
   * @returns {DatabaseCollection} The collection that was created
   */
  getCollection(collectionName) {

  }

  async adminCommand(command) {

  }

  async runCommand(command) {

  }

}