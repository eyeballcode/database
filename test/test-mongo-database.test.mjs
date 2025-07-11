import { expect } from 'chai'
import MongoDatabaseConnection from '../lib/mongo/MongoDatabaseConnection.mjs'
import LokiDatabaseConnection from '../lib/loki/LokiDatabaseConnection.mjs'

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
    }, 100)
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

    it('Ping check', async () => {
      await database.ping()
    })

    it('Insertion/Retrival check', async () => {
      let coll = await database.getCollection('test1')
      await coll.createDocuments([{ id: 1, status: 'ok' }, { id: 2, status: 'not ok' }, { id: 3, status: 'ok' }])
      expect((await coll.findDocument({ id: 1 })).status).to.equal('ok')
      expect((await coll.findDocument({ id: 2 })).status).to.equal('not ok')
    })

    it('Projects away unwanted fields', async () => {
      let coll = await database.getCollection('test1')

      expect((await coll.findDocument({ id: 1 }, { status: 0 })).status).to.not.exist
      expect((await coll.findDocuments({ id: 1 }, { status: 0 }).toArray())[0].status).to.not.exist
    })
    
    it('Insertion/Retrival check', async () => {
      let coll = await database.getCollection('test1')
      await coll.createDocuments([{ id: 1, status: 'ok' }, { id: 2, status: 'not ok' }, { id: 3, status: 'ok' }])
      expect((await coll.findDocument({ id: 1 })).status).to.equal('ok')
      expect((await coll.findDocument({ id: 2 })).status).to.equal('not ok')
    })
    
    it('Should trigger the callback if a query has a high document examined to returned ratio while in debug mode', async () => {
      let called = false
      database.enableDebugging(() => {
        called = true
      })
      let coll = await database.getCollection('test2')
      for (let i = 0; i < 50; i++) await coll.createDocument({ id: i })
      await coll.findDocument({ id: 42 })
      setTimeout(() => expect(called).to.be.true, 10)
    })
    
    it('Can search an object by its ObjectID without explicitly creating an ID', async () => {
      let coll = await database.getCollection('test3')
      let objID = (await coll.createDocument({ test: 'hi' })).insertedId
      let queryObj = await coll.findDocument({ _id: coll.createObjectID(objID) })
      expect(queryObj.test).to.equal('hi')

      let queryObj2 = await coll.findDocument({ _id: coll.createObjectID(objID.toString()) })
      expect(queryObj2.test).to.equal('hi')
    })

    describe('The getCollectionNames function', () => {
      it('Returns all the collection names in the database', async () => {
        let collectionNames = await database.getCollectionNames()
        expect(collectionNames).to.include('test1')
        expect(collectionNames).to.include('test2')
      })
    })

    it('Can dump a set of collections to a Loki database', async () => {
      let coll = await database.getCollection('test4')
      let testObjID = (await coll.createDocument({ foo: 'bar' })).insertedId
      await coll.createDocument({ test: 'ok', name: testObjID, discard: 'me' })

      let target = new LokiDatabaseConnection('out-db')
      await target.connect()

      await database.dumpToLoki(target, [{
        name: 'test1',
        query: {},
      }, {
        name: 'test2',
        query: { id: { $lte: 9 } }
      }, {
        name: 'test4',
        query: {},
        projection: {
          discard: 0
        }
      }])

      let coll1 = await database.getCollection('test1')
      let coll1Loki = await target.getCollection('test1')
      expect((await coll1Loki.findDocument({ id: 1 })).status).to.equal('ok')
      expect((await coll1Loki.findDocument({ id: 1 }))._id).to.equal((await coll1.findDocument({ id: 1 }))._id.toString())
      expect((await coll1Loki.findDocument({ id: 2 })).status).to.equal('not ok')
      expect((await coll1Loki.findDocument({ id: 3 })).status).to.equal('ok')

      let coll2 = await target.getCollection('test2')
      expect(await coll2.countDocuments()).to.equal(10)

      let coll4 = await target.getCollection('test4')
      expect((await coll4.findDocument({ test: 'ok' })).name).to.equal(testObjID.toString())
      expect((await coll4.findDocument({ test: 'ok' })).discard).to.not.exist
    })

    after(async () => {
      await database.dropDatabase()
      await database.close()
    })
  })
}