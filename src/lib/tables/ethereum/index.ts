import { queryBuilder } from '../queryBuilder'

const SCHEMA_NAME = 'ethereum'
const tableNames = {
    BLOCKS: 'blocks',
    TRANSACTIONS: 'transactions',
    LOGS: 'logs',
    TRACES: 'traces',
    CONTRACTS: 'contracts',
}

const blocks = () => queryBuilder.withSchema(SCHEMA_NAME).from(tableNames.BLOCKS)
const transactions = () => queryBuilder.withSchema(SCHEMA_NAME).from(tableNames.TRANSACTIONS)
const logs = () => queryBuilder.withSchema(SCHEMA_NAME).from(tableNames.LOGS)
const traces = () => queryBuilder.withSchema(SCHEMA_NAME).from(tableNames.TRACES)
const contracts = () => queryBuilder.withSchema(SCHEMA_NAME).from(tableNames.CONTRACTS)

export default {
    blocks,
    transactions,
    logs,
    traces,
    contracts,
}
