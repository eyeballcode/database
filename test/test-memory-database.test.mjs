import { expect } from 'chai'
import LokiDatabaseConnection from '../lib/loki/LokiDatabaseConnection.mjs'
import LokiDatabaseCollection from '../lib/loki/LokiDatabaseCollection.mjs'

const aggData = [{
  mode: 'bus',
  stopName: "Dole Avenue/Cheddar Road",
  id: 0
}, {
  mode: 'bus',
  stopName: "Huntingdale Station",
  id: 1
}, {
  mode: 'bus',
  stopName: "Huntingdale Station",
  id: 2
}, {
  mode: 'bus',
  stopName: "Huntingdale Station",
  id: 3
}, {
  mode: 'bus',
  stopName: "Oakleigh Station",
  id: 4
}, {
  mode: 'metro',
  stopName: "Nunawading Station",
  id: 5
}]

describe('The in memory database', () => {
  it('Should create a new collection that can store items', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocument({ a: 1, b: 1 })
    await coll.createDocument({ a: 2, b: 2 })

    expect((await coll.findDocument({ a: 1 })).b).to.equal(1)
    expect((await coll.findDocument({ a: 2 })).b).to.equal(2)
  })

  it('Should transform geospatial queries using $where and turf', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocument({
      "stopName": "Dole Avenue/Cheddar Road",
      "location": {
        "type": "MultiPoint",
        "coordinates": [[ 145.018951051008, -37.7007748061827 ]]
      }
    })
    await coll.createDocument({
      "stopName": "Rex Street/Taylors Road",
      "location": {
        "type": "MultiPoint",
        "coordinates": [[ 144.776152425766, -37.7269752097338 ]]
      }
    })

    let stop = await coll.findDocument({
      location: {
        $nearSphere: {
          $geometry: {
            "type": "Point",
            "coordinates": [ 145.018954051418, -37.7007758062827 ]
          },
          $maxDistance: 500
        }
      }
    })

    expect(stop).to.not.be.null
    expect(stop.stopName).to.equal('Dole Avenue/Cheddar Road')
  })

  it('Should ensure maxDistance is in metres', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocument({
      "stopName": "Dole Avenue/Cheddar Road",
      "location": {
        "type": "MultiPoint",
        "coordinates": [[ 145.436193608845, -38.4376225731191 ]]
      }
    })

    let stop = await coll.findDocument({
      location: {
        $nearSphere: {
          $geometry: {
            "type": "Point",
            "coordinates": [ 145.018954051418, -37.7007758062827 ]
          },
          $maxDistance: 500
        }
      }
    })

    expect(stop).to.be.null
  })

  it('Should find distinct fields based on a query', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments([{
      mode: 'bus',
      stopName: "Dole Avenue/Cheddar Road"
    }, {
      mode: 'bus',
      stopName: "Huntingdale Station"
    }, {
      mode: 'bus',
      stopName: "Oakleigh Station"
    }, {
      mode: 'metro',
      stopName: "Nunawading Station"
    }])

    expect(await coll.distinct('stopName', { mode: 'bus' })).to.have.members([
      'Dole Avenue/Cheddar Road',
      'Huntingdale Station',
      'Oakleigh Station'
    ])

    expect(await coll.distinct('_id').length).to.equal(4)
  })

  it('Allow mongodb style updating', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments([{
      mode: 'bus',
      stopName: "Dole Avenue/Cheddar Road"
    }, {
      mode: 'bus',
      stopName: "Huntingdale Station"
    }, {
      mode: 'bus',
      stopName: "Oakleigh Station"
    }, {
      mode: 'metro',
      stopName: "Nunawading Station"
    }])

    await coll.updateDocument({
      stopName: "Dole Avenue/Cheddar Road"
    }, {
      $set: { mode: 'tram' }
    })

    expect(coll.findDocument({ stopName: "Dole Avenue/Cheddar Road" }).mode).to.equal('tram')
  })

  it('Allow updating multiple documents at once', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments([{
      mode: 'bus',
      stopName: "Dole Avenue/Cheddar Road",
      updateMe: 1
    }, {
      mode: 'bus',
      stopName: "Huntingdale Station",
      updateMe: 1
    }, {
      mode: 'bus',
      stopName: "Oakleigh Station"
    }, {
      mode: 'metro',
      stopName: "Nunawading Station"
    }])

    await coll.updateDocuments({
      updateMe: 1
    }, {
      $set: { mode: 'tram' }
    })

    expect(coll.findDocument({ stopName: "Dole Avenue/Cheddar Road" }).mode).to.equal('tram')
    expect(coll.findDocument({ stopName: "Huntingdale Station" }).mode).to.equal('tram')
    expect(coll.findDocument({ stopName: "Oakleigh Station" }).mode).to.equal('bus')
    expect(coll.findDocument({ stopName: "Nunawading Station" }).mode).to.equal('metro')
  })

  it('Allow mongodb style aggregation with a very simple 2 stage pipeline only', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments(JSON.parse(JSON.stringify(aggData)))

    let data = await coll.aggregate([
      {
        $match: {}
      }, {
        $group: {
          _id: {
            mode: '$mode',
            name: '$stopName'
          }
        }
      }
    ]).toArray()

    expect(data).to.deep.equal([
      { _id: { mode: 'bus', name: 'Dole Avenue/Cheddar Road' } },
      { _id: { mode: 'bus', name: 'Huntingdale Station' } },
      { _id: { mode: 'bus', name: 'Oakleigh Station' } },
      { _id: { mode: 'metro', name: 'Nunawading Station' } }
    ])
  })

  it('Should allow aggregation to sort by count', async () => {
    let data = []
    for (let i = 0; i < 20; i++) data.push({ name: 'Thomastown', index: Math.random() })
    for (let i = 0; i < 25; i++) data.push({ name: 'Lalor', index: Math.random() })

    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments(data.sort((a, b) => a.index - b.index))

    let counts = await coll.aggregate([
      {
        $match: {}
      }, {
        $sortByCount: '$name'
      }
    ]).toArray()

    expect(counts).to.deep.equal([
      { _id: 'Lalor', count: 25 },
      { _id: 'Thomastown', count: 20 }
    ])
  })

  it('Should allow mongodb style matching of array items with $elemMatch', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments([{
      name: 'Huntingdale',
      bays: [{
        mode: 'bus',
        stopGTFSID: '51587',
        sub: { tram: true }
      }, {
        mode: 'metro',
        stopGTFSID: '123'
      }]
    }, {
      name: 'Huntingdale 2',
      bays: [{
        mode: 'bus',
        stopGTFSID: '51587',
        sub: { tram: false }
      }, {
        mode: 'metro',
        stopGTFSID: '123'
      }]
    }, {
      name: 'Monash',
      bays: [{
        mode: 'bus',
        stopGTFSID: '19810'
      }]
    }])

    let test1 = await coll.findDocuments({
      bays: {
        $elemMatch: {
          mode: 'bus',
          stopGTFSID: '51587',
          'sub.tram': true
        }
      }
    }).toArray()

    expect(test1.length).to.equal(1)
    expect(test1[0].name).to.equal('Huntingdale')

    let test2 = await coll.findDocuments({
      bays: {
        $elemMatch: {
          mode: 'metro',
          stopGTFSID: '123'
        }
      }
    }).toArray()

    expect(test2.length).to.equal(2)
    expect(test2[0].name).to.equal('Huntingdale')
    expect(test2[0]).to.equal(test1[0])
    expect(test2[1].name).to.equal('Huntingdale 2')
  })

  it('Should ensure $elemMatch respects $in/$gte/other checks', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments([{
      name: 'Huntingdale 1',
      bays: [{
        mode: 'bus',
        stopGTFSID: '51587',
        sub: { count: 4 }
      }, {
        mode: 'metro',
        stopGTFSID: '123'
      }]
    }, {
      name: 'Huntingdale 2',
      bays: [{
        mode: 'bus',
        stopGTFSID: '51587',
        sub: { count: 10 }
      }, {
        mode: 'metro',
        stopGTFSID: '123'
      }]
    }, {
      name: 'Huntingdale 3',
      bays: [{
        mode: 'bus',
        stopGTFSID: '999',
        sub: { count: 4 }
      }, {
        mode: 'metro',
        stopGTFSID: '123'
      }]
    }])

    let data = await coll.findDocuments({
      bays: {
        $elemMatch: {
          stopGTFSID: { $in: [ '51587' ] },
          'sub.count': {
            $gte: 3,
            $lte: 5
          }
        }
      }
    }).toArray()

    expect(data.length).to.equal(1)
    expect(data[0].name).to.equal('Huntingdale 1')
  })

  it('Should allow document deletion', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocument({
      mode: 'bus',
      stopName: "Dole Avenue/Cheddar Road",
      id: 0
    })

    await coll.deleteDocument({ id: 0 })

    expect(await coll.findDocument({ id: 0 })).to.be.null
  })

  it('Should allow document replacement', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocument({
      mode: 'bus',
      stopName: "Dole Avenue/Cheddar Road",
      id: 0
    })

    await coll.replaceDocument({ id: 0 }, {
      newMode: 'tram',
      newID: 1
    })

    expect(await coll.findDocument({ id: 0 })).to.be.null
    let replacement = await coll.findDocument({ newID: 1 })
    expect(replacement.newMode).to.equal('tram')
    expect(replacement.newID).to.equal(1)
    expect(replacement.stopName).to.be.undefined
  })

  it('Should allow bulk inserts', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.bulkWrite([{ insertOne: { name: 'Hi' } }])
    expect(await coll.findDocument({ name: 'Hi' })).to.exist
  })

  it('Should allow bulk updates', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments([{
      mode: 'bus',
      stopName: "Dole Avenue/Cheddar Road",
      id: 0
    }, {
      mode: 'bus',
      stopName: "Huntingdale Station",
      id: 1
    }, {
      mode: 'bus',
      stopName: "Huntingdale Station",
      id: 2
    }, {
      mode: 'bus',
      stopName: "Huntingdale Station",
      id: 3
    }, {
      mode: 'bus',
      stopName: "Oakleigh Station",
      id: 4
    }, {
      mode: 'metro',
      stopName: "Nunawading Station",
      id: 5
    }])

    await coll.bulkWrite([{
      updateOne: {
        filter: { id: 2 },
        update: { $set: { name: 'Test' } }
      }
    }, {
      updateOne: {
        filter: { id: 5 },
        update: { $set: { name: 'Test 2' } }
      }
    }])
    expect((await coll.findDocument({ id: 2 })).name).to.equal('Test')
    expect((await coll.findDocument({ id: 5 })).name).to.equal('Test 2')
  })

  it('Should allow bulk replacements with/without upsert', async () => {
    let db = new LokiDatabaseConnection('test-db')
    let coll = await db.createCollection('test-coll')

    await coll.bulkWrite([{ replaceOne: {
      filter: { name: 'Bye' },
      replacement: { name: 'Hi' }
    }}])

    expect(await coll.findDocument({ name: 'Hi' })).to.not.exist

    await coll.bulkWrite([{ replaceOne: {
      filter: { name: 'Bye' },
      replacement: { name: 'Hi' },
      upsert: true
    }}])

    expect(await coll.findDocument({ name: 'Hi' })).to.exist
  })  

  it('Should allow bulk replacements', async () => {
    let db = new LokiDatabaseConnection('test-db')
    let coll = await db.createCollection('test-coll')

    for (let i = 0; i < 5; i++) {
      await coll.createDocument({
        mode: 'bus',
        stopName: "Dole Avenue/Cheddar Road",
        id: i
      })  
    }

    await coll.bulkWrite([{ replaceOne: {
      filter: { id: 0 },
      replacement: { replacement: '1' },
      upsert: true
    }}, { replaceOne: {
      filter: { id: 1 },
      replacement: { name: 'Hi' },
      upsert: true
    }}])

    expect(await coll.findDocument({ replacement: '1' })).to.exist
    expect(await coll.findDocument({ name: 'Hi' })).to.exist

    expect(await coll.findDocument({ id: 0 })).to.not.exist
    expect(await coll.findDocument({ id: 1 })).to.not.exist
    expect(await coll.findDocument({ id: 2 })).to.exist
  })

  it('Should count by using find and returning the length', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments(JSON.parse(JSON.stringify(aggData)))

    expect(await coll.countDocuments({})).to.equal(aggData.length)
    expect(await coll.countDocuments({ stopName: /Huntingdale/ }))
      .to.equal(aggData.filter(s => s.stopName.includes('Huntingdale')).length)
  })

  it('Should match arrays by their contents', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    let days = ['Mon', 'Tue', 'Wed', 'Thu']
    await coll.createDocument({
      days
    })

    expect(await coll.findDocument({ days: 'Mon' })).to.exist
  })

  describe('The _fieldMatches function', () => {
    it('Should return a trivial comparison if no $ operators are given', () => {
      expect(LokiDatabaseCollection._fieldMatches('51587', '51587')).to.be.true
    })

    it('Should check for $ne', () => {
      expect(LokiDatabaseCollection._fieldMatches('51587', { $ne: '51587' })).to.be.false
      expect(LokiDatabaseCollection._fieldMatches('51586', { $ne: '51587' })).to.be.true
    })

    it('Should check for $in', () => {
      expect(LokiDatabaseCollection._fieldMatches('51587', { $in: ['51587'] })).to.be.true
      expect(LokiDatabaseCollection._fieldMatches('51586', { $in: ['51587'] })).to.be.false
      expect(LokiDatabaseCollection._fieldMatches('51586', { $in: ['51586', '51587'] })).to.be.true
    })

    it('Should check for $gt/$gte', () => {
      expect(LokiDatabaseCollection._fieldMatches(15, { $gt: 13 })).to.be.true
      expect(LokiDatabaseCollection._fieldMatches(10, { $gt: 13 })).to.be.false
      expect(LokiDatabaseCollection._fieldMatches(13, { $gt: 13 })).to.be.false
      expect(LokiDatabaseCollection._fieldMatches(13, { $gte: 13 })).to.be.true
    })

    it('Should check for $lt/$lte', () => {
      expect(LokiDatabaseCollection._fieldMatches(15, { $lt: 13 })).to.be.false
      expect(LokiDatabaseCollection._fieldMatches(10, { $lt: 13 })).to.be.true
      expect(LokiDatabaseCollection._fieldMatches(13, { $lt: 13 })).to.be.false
      expect(LokiDatabaseCollection._fieldMatches(13, { $lte: 13 })).to.be.true
    })

    it('Should check for $not', () => {
      expect(LokiDatabaseCollection._fieldMatches(15, { $not: { $lt: 13 } })).to.be.true
      expect(LokiDatabaseCollection._fieldMatches(10, { $not: { $lt: 13 } })).to.be.false

      expect(LokiDatabaseCollection._fieldMatches('abc', { $not: { $in: ['abc', 'def'] } })).to.be.false
      expect(LokiDatabaseCollection._fieldMatches('abcd', { $not: { $in: ['abc', 'def'] } })).to.be.true
    })

    it('Should allow chaining multiple operators', () => {
      expect(LokiDatabaseCollection._fieldMatches(15, { $gte: 13, $lte: 17 })).to.be.true
      expect(LokiDatabaseCollection._fieldMatches(20, { $gte: 13, $lte: 17 })).to.be.false
    })

    it('Should allow matching array contents', () => {
      expect(LokiDatabaseCollection._fieldMatches([20, 21], 20)).to.be.true
      expect(LokiDatabaseCollection._fieldMatches([19, 21], 20)).to.be.false
    })
  })
})