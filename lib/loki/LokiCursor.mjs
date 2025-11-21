export default class LokiCursor {

  #data

  constructor(data) { 
    this.#data = data
  }

  toArray() { return this.#data }

}