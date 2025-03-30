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

  removeIndex(index) {}

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
        let maxDist = query[key].$nearSphere.$maxDistance / 1000
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
      } else if (query[key].$elemMatch) {
        let match = query[key].$elemMatch
        let matchingFields = Object.keys(match).map(field => ({
          key: field.split('.'),
          value: match[field]
        }))

        query[key] = {
          $where: val => {
            for (let item of val) {
              let allMatch = true
              for (let field of matchingFields) {
                let obj = item
                for (let layer of field.key) {
                  obj = obj[layer]
                  if (typeof obj === 'undefined') { allMatch = false; break }
                }
                if (!this.constructor._fieldMatches(obj, field.value)) { allMatch = false; break }
              }
              if (allMatch) return true
            }
            return false
          }
        }
      } else if (query[key].exec) {
        query[key] = {
          $regex: query[key]
        }
      } else {
        let match = query[key]
        query[key] = {
          $where: val => this.constructor._fieldMatches(val, match)
        }
      }
    }

    return query
  }

  static _fieldMatches(target, query) {
    if (typeof query !== 'object' || !Object.keys(query).some(key => key[0] === '$')) {
      if (target instanceof Array) return target.includes(query)
      else return target === query
    }

    for (let op of Object.keys(query)) {
      if (op === '$in' && !query[op].includes(target)) return false
      else if (op === '$ne' && query[op] === target) return false
      else if (op === '$lt' && !(target < query[op])) return false
      else if (op === '$lte' && !(target <= query[op])) return false
      else if (op === '$gt' && !(target > query[op])) return false
      else if (op === '$gte' && !(target >= query[op])) return false
    }

    return true
  }

  findDocuments(query) {
    return {
      toArray: () => this.collection.find(this.transformQuery(query))
    }
  }

  findDocument(query) {
    return this.collection.findOne(this.transformQuery(query))
  }

  updateDocument(query, update) {
    let original = this.findDocument(query)

    for (let field of Object.keys(update.$set)) {
      original[field] = update.$set[field]
    }

    return this.collection.update(original)
  }

  replaceDocument(query, replacement) {
    let original = this.findDocument(query)
    let _id = original._id

    this.deleteDocument({ _id })
    this.createDocument({ _id, ...replacement })
  }

  deleteDocument(query) {
    let original = this.findDocument(query)
    this.collection.remove(original)
  }

  dropCollection() {
    return this.collection.clear()
  }

  distinct(field, query) {
    let matching = this.findDocuments(query || {}).toArray()
    return matching.map(doc => doc[field]).filter((e, i, a) => a.indexOf(e) === i)
  }

  aggregate(pipeline) {
    let results = []
    for (let stage of pipeline) {
      if (stage.$match) results = this.findDocuments(stage.$match).toArray()
      else if (stage.$group) {
        let getID = item => {
          let id = {}
          for (let field of Object.keys(stage.$group._id)) id[field] = item[stage.$group._id[field].slice(1)]
          return id
        }

        let idsSeen = new Set()
        results = results.map(item => {
          let id = getID(item)
          let output = { data: { _id: getID(item) }, serialID: JSON.stringify(id) }
          // for (let field of Object.keys(stage.$group)) {
          //   if (field === '_id') output[field] = getID(item)
          //   else output[field] = item[field]
          // }
          
          return output
        }).filter(item => {
          if (idsSeen.has(item.serialID)) return false

          idsSeen.add(item.serialID)
          return true
        }).map(item => item.data)
      } else if (stage.$sortByCount) {
        let counts = {}
        for (let item of results) {
          let fieldData = item[stage.$sortByCount.slice(1)]
          if (!counts[fieldData]) counts[fieldData] = 0

          counts[fieldData]++
        }

        results = Object.keys(counts).map(key => ({ _id: key, count: counts[key] })).sort((a, b) => b.count - a.count)
      }
    }

    return { toArray: () => results }
  }

  bulkWrite(operations) {
    for (let operation of operations) {
      if (operation.insertOne) this.createDocument(operation.insertOne)
      if (operation.updateOne) this.updateDocument(operation.updateOne.filter, operation.updateOne.update)
      if (operation.replaceOne) this.replaceDocument(operation.updateOne.filter, operation.updateOne.replacement)
    }
  }

  countDocuments(query) {
    return this.findDocuments(query).toArray().length
  }

}
