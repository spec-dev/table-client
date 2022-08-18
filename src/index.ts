import SpecTableClient from './client'

const tableClient = new SpecTableClient()
const runQuery = (query, opts) => tableClient.runQuery(query, opts)
const streamQuery = (query, transforms, opts) => tableClient.streamQuery(query, transforms, opts)

export { SpecTableClient, tableClient, runQuery, streamQuery }
export { ethereum } from './lib/tables'
