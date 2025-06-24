import MongoDatabaseCollection from './MongoDatabaseCollection.mjs'

export default class MongoDatabaseDebugCollection extends MongoDatabaseCollection {

  #thresholdTrigger

  constructor(mongoCollection, collectionName, thresholdTrigger) {
    super(mongoCollection, collectionName)
    this.#thresholdTrigger = thresholdTrigger
  }

  findDocuments(query, projection) {
    let targetObject = {}
    Error.captureStackTrace(targetObject)

    setTimeout(async () => {
      let cursor = super.findDocuments(query, projection)
      let explained = await this.explain(cursor)
      if (this.isHighThreshold(explained)) {
        this.#thresholdTrigger(explained, query, targetObject.stack)
      }
    })

    return super.findDocuments(query, projection)
  }

  findDocument(query, projection, callback) {
    return this.findDocuments(query, projection).limit(1).next()
  }

  isHighThreshold(explained) {
    return explained.totalDocsExamined / explained.nReturned > 10
  }

  async explain(query) {
    return (await query.explain('executionStats')).executionStats
  }
}
