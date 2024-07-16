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
})