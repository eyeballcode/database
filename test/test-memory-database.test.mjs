import { expect } from 'chai'
import LokiDatabaseConnection from '../lib/loki/LokiDatabaseConnection.mjs'

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

  it('Allow mongodb style matching of array items with $elemMatch', async () => {
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

    let data = await coll.findDocuments({
      bays: {
        $elemMatch: {
          mode: 'bus',
          stopGTFSID: '51587',
          'sub.tram': true
        }
      }
    }).toArray()

    expect(data.length).to.equal(1)
    expect(data[0].name).to.equal('Huntingdale')
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

  it('Should count by using find and returning the length', async () => {
    let db = new LokiDatabaseConnection('test-db')

    let coll = await db.createCollection('test-coll')
    await coll.createDocuments(JSON.parse(JSON.stringify(aggData)))

    expect(await coll.countDocuments({})).to.equal(aggData.length)
    expect(await coll.countDocuments({ stopName: /Huntingdale/ }))
      .to.equal(aggData.filter(s => s.stopName.includes('Huntingdale')).length)
  })
})