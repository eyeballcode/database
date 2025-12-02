export default class LokiCursor {

  #data

  constructor(data) { 
    this.#data = data
  }

  toArray() { return this.#data }

  sort(columns) {
    const columnNames = Object.keys(columns)
    const sortColumn = (name, a, b) => {
      if (typeof a[name] === 'number' && typeof b[name] === 'number') return a[name] - b[name]
      if (typeof a[name] !== 'undefined' && typeof b[name] !== 'undefined') return a[name].toString().localeCompare(b[name])
      if (typeof a[name] === 'undefined' && typeof b[name] === 'undefined') return 0
      return typeof a[name] === 'undefined' ? 1 : -1
    }

    return new LokiCursor(this.#data.toSorted((a, b) => {
      for (const columnName of columnNames) {
        const direction = columns[columnName]
        const sortResult = sortColumn(columnName, a, b)
        if (sortResult !== 0) return sortResult * direction
      }
    }))
  }

  // Has to modify the cursor in place
  next() {
    return this.#data.shift()
  }

  limit(count) {
    return new LokiCursor(this.#data.slice(0, count))
  }

  skip(count) {
    return new LokiCursor(this.#data.slice(count))
  }

}