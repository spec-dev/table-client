import { newQueryBuilder } from '../queryBuilder'

const SCHEMA_NAME = 'ethereum'

const tableNames = {
    BLOCKS: 'blocks',
    TRANSACTIONS: 'transactions',
    LOGS: 'logs',
    TRACES: 'traces',
    CONTRACTS: 'contracts',
    LATEST_INTERACTIONS: 'latest_interactions',
}

const blocks = () => newQueryBuilder().withSchema(SCHEMA_NAME).from(tableNames.BLOCKS)
const transactions = () => newQueryBuilder().withSchema(SCHEMA_NAME).from(tableNames.TRANSACTIONS)
const logs = () => newQueryBuilder().withSchema(SCHEMA_NAME).from(tableNames.LOGS)
const traces = () => newQueryBuilder().withSchema(SCHEMA_NAME).from(tableNames.TRACES)
const contracts = () => newQueryBuilder().withSchema(SCHEMA_NAME).from(tableNames.CONTRACTS)
const latestInteractions = () =>
    newQueryBuilder().withSchema(SCHEMA_NAME).from(tableNames.LATEST_INTERACTIONS)

export default {
    blocks,
    transactions,
    logs,
    traces,
    contracts,
    latestInteractions,
}
