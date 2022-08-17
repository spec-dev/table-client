import SpecTableClient from './client'

const tableClient = new SpecTableClient()
const runQuery = tableClient.runQuery
const streamQuery = tableClient.streamQuery

export { SpecTableClient, tableClient, runQuery, streamQuery }
export { ethereum } from './lib/tables'
