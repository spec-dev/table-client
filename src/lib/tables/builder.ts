import knex, { Knex } from 'knex'

export const db = () => knex({ client: 'pg' })

export const schema = (name: string, tx?: any): Knex.QueryBuilder => {
    tx = tx || db()
    return tx.withSchema(name)
}