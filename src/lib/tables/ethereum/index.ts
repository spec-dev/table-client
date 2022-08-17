import { schema } from '../builder'
import { Knex } from 'knex'

const SCHEMA_NAME = 'ethereum'
const tableNames = {
    BLOCKS: 'blocks',
    TRANSACTIONS: 'transactions',
    LOGS: 'logs',
    TRACES: 'traces',
    CONTRACTS: 'contracts',
}

const blocks = (tx?: any): Knex.QueryBuilder => schema(SCHEMA_NAME, tx).from(tableNames.BLOCKS)
const transactions = (tx?: any): Knex.QueryBuilder => schema(SCHEMA_NAME, tx).from(tableNames.TRANSACTIONS)
const logs = (tx?: any): Knex.QueryBuilder => schema(SCHEMA_NAME, tx).from(tableNames.LOGS)
const traces = (tx?: any): Knex.QueryBuilder => schema(SCHEMA_NAME, tx).from(tableNames.TRACES)
const contracts = (tx?: any): Knex.QueryBuilder => schema(SCHEMA_NAME, tx).from(tableNames.CONTRACTS)

export default {
    blocks,
    transactions,
    logs,
    traces,
    contracts,
}