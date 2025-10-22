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
    let data
    if (document.$loki) {
      let newDoc = { ...document }
      delete newDoc.$loki

      this.#createID(newDoc)
      data = this.collection.insert(newDoc)
    } else {
      this.#createID(document)
      data = this.collection.insert(document)
    }

    return {
      acknowledged: true,
      insertedId: data._id
    }
  }

  createDocuments(documents) {
    return this.collection.insert(documents.map(doc => this.#createID(doc)))
  }

  transformQuery(rawQuery) {
    let query = { ...rawQuery }

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
        let allFields = Object.keys(match).map(field => ({
          key: field.split('.'),
          value: match[field]
        }))
        let matchingFields = allFields.filter(field => !(field.key.length === 1 && field.key[0][0] === '$'))
        let specialFields = allFields.filter(field => field.key.length === 1 && field.key[0][0] === '$')

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

              for (let field of specialFields) {
                let operator = field.key[0]
                if (operator === '$or') {
                  let anyMatch = field.value.some(possibleMatch => {
                    for (let operatorField of Object.keys(possibleMatch)) {
                      if (!this.constructor._fieldMatches(item[operatorField], possibleMatch[operatorField])) return false
                    }
                    return true // Found one that matched all fields, break and stop checking
                  })
                  if (!anyMatch) { allMatch = false; break }
                }
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
      } else if (key === '$or' || key === '$and') {
        query[key] = query[key].map(subQuery => this.transformQuery(subQuery))
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
    if (target instanceof Array && query instanceof Array) {
      if (target.length !== query.length) return false
      for (let i = 0; i < target.length; i++) {
        if (target[i] !== query[i]) return false
      }
      return true
    }
    if (typeof query !== 'object' || !Object.keys(query).some(key => key[0] === '$')) {
      if (target instanceof Array) return target.includes(query)
      else return target === query
    }

    if (query['$not']) {
      return !this._fieldMatches(target, query['$not'])
    }

    for (let op of Object.keys(query)) {
      if (op === '$in' && !query[op].includes(target)) return false
      else if (op === '$ne' && query[op] === target) return false
      else if (op === '$lt' && !(target < query[op])) return false
      else if (op === '$lte' && !(target <= query[op])) return false
      else if (op === '$gt' && !(target > query[op])) return false
      else if (op === '$gte' && !(target >= query[op])) return false
      else if (op === '$exists') return (typeof target === 'undefined') !== query[op]
    }

    return true
  }

  findDocuments(query, projection) {
    return {
      toArray: () => this.collection.find(this.transformQuery(query), projection).map(d => this.project(d, projection))
    }
  }

  findDocument(query, projection) {
    return this.project(this.collection.findOne(this.transformQuery(query)), projection)
  }

  project(doc, projection = {}) {
    if (!doc) return null

    const projKeys = Object.keys(projection)
    const keys = Object.keys(doc)
    if (keys.length === 0) return doc

    const allConsistent = projKeys.reduce((acc, key) => acc && projection[key] === projection[projKeys[0]], true)
    if (!allConsistent) throw new Error('Inconsistent projection ' + JSON.stringify(projection))
    const projectionType = projection[keys[0]]
    if (projectionType === 1) {
      return keys.reduce((acc, key) => projection[key] === 1 ? ({
        ...acc, [key]: doc[key]
      }) : acc, {})
    } else {
      return keys.reduce((acc, key) => projection[key] === 0 ? acc : ({
        ...acc, [key]: doc[key]
      }), {})
    }
  }

  updateDocument(query, update) {
    let original = this.findDocument(query)

    for (let field of Object.keys(update.$set)) {
      original[field] = update.$set[field]
    }

    return this.collection.update(original)
  }

  updateDocuments(query, update) {
    let originals = this.findDocuments(query).toArray()

    for (let original of originals) {
      for (let field of Object.keys(update.$set)) {
        original[field] = update.$set[field]
      }

      this.collection.update(original)
    }
  }

  replaceDocument(query, replacement, upsert) {
    let original = this.findDocument(query)
    if (!original && upsert) return this.createDocument(replacement)
    else if (original) {
      let _id = original._id

      this.deleteDocument({ _id })
      this.createDocument({ _id, ...replacement })
    }
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
      if (operation.replaceOne) this.replaceDocument(operation.replaceOne.filter, operation.replaceOne.replacement, operation.replaceOne.upsert)
    }
  }

  countDocuments(query = {}) {
    return this.findDocuments(query).toArray().length
  }

  createObjectID(objectID) {
    return objectID
  }

}
