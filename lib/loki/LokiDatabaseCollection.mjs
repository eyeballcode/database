import DatabaseCollection from '../DatabaseCollection.mjs'
import distance from '@turf/distance'
import crypto from 'crypto'

export default class LokiDatabaseCollection extends DatabaseCollection {
  constructor (collection) {
    super()
    this.collection = collection
  }

  #createID(document) {
    if (!document._id) document._id = crypto.randomBytes(16).toString('hex')
    return document
  }

  createIndex(keys, options) {
    return this.collection.ensureIndex(keys)
  }

  createDocument(document) {
    this.#createID(document)
    return this.collection.insert(document)
  }

  createDocuments(documents) {
    return this.collection.insert(documents.map(doc => this.#createID(doc)))
  }

  transformQuery(query) {
    for (let key of Object.keys(query)) {
      if (query[key].$nearSphere) {
        let queryGeom = query[key].$nearSphere.$geometry
        let maxDist = query[key].$nearSphere.$maxDistance
        query[key] = {
          $where: val => {
            if (val.type === 'Point') return distance(queryGeom, val) < maxDist
            if (val.type === 'MultiPoint') {
              return val.coordinates.map(coordinates => {
                return distance(queryGeom, {
                  type: 'Point',
                  coordinates
                })
              }).some(dist => dist < maxDist)
            }
            return false
          }
        }
      }

      if (query[key].exec) {
        query[key] = {
          $regex: query[key]
        }
      }
    }
    return query
  }

  findDocuments(query) {
    return {
      toArray: () => this.collection.find(this.transformQuery(query))
    }
  }

  findDocument(query) {
    return this.collection.findOne(this.transformQuery(query))
  }

  dropCollection() {
    return this.collection.clear()
  }

  distinct(field, query) {
    let matching = this.findDocuments(query || {}).toArray()
    return matching.map(doc => doc[field]).filter((e, i, a) => a.indexOf(e) === i)
  }
}
