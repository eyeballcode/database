import { expect } from 'chai'
import LokiDatabaseConnection from '../lib/loki/LokiDatabaseConnection.mjs'

describe('The in memory database', () => {
  it('Should create a new collection that can store items', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocument({ a: 1, b: 1 })
    await coll.createDocument({ a: 2, b: 2 })

    expect((await coll.findDocument({ a: 1 })).b).to.equal(1)
    expect((await coll.findDocument({ a: 2 })).b).to.equal(2)
  })
})