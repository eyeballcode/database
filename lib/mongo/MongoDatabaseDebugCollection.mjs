import MongoDatabaseCollection from './MongoDatabaseCollection.mjs'

export default class MongoDatabaseDebugCollection extends MongoDatabaseCollection {

  #thresholdTrigger

  constructor(mongoCollection, collectionName, thresholdTrigger) {
    super(mongoCollection, collectionName)
    this.#thresholdTrigger = thresholdTrigger
  }

  async findDocuments(query, projection) {
    let cursor = super.findDocuments(query, projection)
    let explained = await this.explain(cursor)
    if (this.isHighThreshold(explained)) {
      this.#thresholdTrigger(explained)
    }

    return await super.findDocuments(query, projection)
  }

  async findDocument(query, projection, callback) {
    return await (await this.findDocuments(query, projection)).limit(1).next()
  }

  isHighThreshold(explained) {
    return explained.totalDocsExamined / explained.nReturned > 10
  }

  async explain(query) {
    return (await query.explain('executionStats')).executionStats
  }
}
