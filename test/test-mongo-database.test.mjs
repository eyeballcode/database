import { expect } from 'chai'
import MongoDatabaseConnection from '../lib/mongo/MongoDatabaseConnection.mjs'

const serverHost = '127.0.0.1', serverPort = '27017'

let database = new MongoDatabaseConnection(`mongodb://${serverHost}:${serverPort}`, 'transportme-test-db')
let connected = false
await new Promise(async resolve => {
  try {
    setTimeout(async () => {
      if (!connected) {
        await database.close()
        resolve()
      }
    }, 50)
    await database.connect()
    connected = true
    resolve()
  } catch (e) {
    await database.close()
  }
})

if (connected) {
  describe('The MongoDB database', () => {
    before(async () => {
      await database.dropDatabase()
    })
    
    it('Insertion/Retrival check', async () => {
      let coll = await database.getCollection('test1')
      await coll.createDocuments([{ id: 1, status: 'ok' }, { id: 2, status: 'not ok' }, { id: 3, status: 'ok' }])
      expect((await coll.findDocument({ id: 1 })).status).to.equal('ok')
      expect((await coll.findDocument({ id: 2 })).status).to.equal('not ok')
    })

    after(async () => {
      await database.dropDatabase()
      await database.close()
    })
  })
}