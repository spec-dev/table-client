import SpecTableClient from './client'

const tableClient = new SpecTableClient()
const runQuery = (query) => tableClient.runQuery(query)
const streamQuery = (query, transforms) => tableClient.streamQuery(query, transforms)

export { SpecTableClient, tableClient, runQuery, streamQuery }
export { ethereum } from './lib/tables'
