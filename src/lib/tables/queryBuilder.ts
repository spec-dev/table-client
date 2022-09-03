import Client from 'knex/lib/client'
import PostgresQueryBuilder from 'knex/lib/dialects/postgres/query/pg-querybuilder'
import PostgresQueryCompiler from 'knex/lib/dialects/postgres/query/pg-querycompiler'
import { makeEscape } from 'knex/lib/util/string'

let client

function createClient() {
    client = new Client({ client: 'pg' })

    client.wrapIdentifierImpl = (value) => {
        if (value === '*') return value

        let arrayAccessor = ''
        const arrayAccessorMatch = value.match(/(.*?)(\[[0-9]+\])/)

        if (arrayAccessorMatch) {
            value = arrayAccessorMatch[1]
            arrayAccessor = arrayAccessorMatch[2]
        }

        return `"${value.replace(/"/g, '""')}"${arrayAccessor}`
    }

    client.positionBindings = (sql) => {
        let questionCount = 0
        return sql.replace(/(\\*)(\?)/g, (_, escapes) => {
            if (escapes.length % 2) {
                return '?'
            }
            questionCount++
            return `$${questionCount}`
        })
    }

    client.toPathForJson = (jsonPath) => {
        const PG_PATH_REGEX = /^{.*}$/
        if (jsonPath.match(PG_PATH_REGEX)) {
            return jsonPath
        }
        return (
            '{' +
            jsonPath
                .replace(/^(\$\.)/, '') // remove the first dollar
                .replace('.', ',')
                .replace(/\[([0-9]+)]/, ',$1') + // transform [number] to ,number
            '}'
        )
    }

    function arrayString(arr, esc) {
        let result = '{'
        for (let i = 0; i < arr.length; i++) {
            if (i > 0) result += ','
            const val = arr[i]
            if (val === null || typeof val === 'undefined') {
                result += 'NULL'
            } else if (Array.isArray(val)) {
                result += arrayString(val, esc)
            } else if (typeof val === 'number') {
                result += val
            } else {
                result += JSON.stringify(typeof val === 'string' ? val : esc(val))
            }
        }
        return result + '}'
    }

    client.dialect = 'postgresql'
    client.driverName = 'pg'
    client.canCancelQuery = true
    client._escapeBinding = makeEscape({
        escapeArray(val, esc) {
            return esc(arrayString(val, esc))
        },
        escapeString(str) {
            let hasBackslash = false
            let escaped = "'"
            for (let i = 0; i < str.length; i++) {
                const c = str[i]
                if (c === "'") {
                    escaped += c + c
                } else if (c === '\\') {
                    escaped += c + c
                    hasBackslash = true
                } else {
                    escaped += c
                }
            }
            escaped += "'"
            if (hasBackslash === true) {
                escaped = 'E' + escaped
            }
            return escaped
        },
        escapeObject(val, prepareValue, timezone, seen: any[] = []) {
            if (val && typeof val.toPostgres === 'function') {
                seen = seen || []
                if (seen.indexOf(val) !== -1) {
                    throw new Error(
                        `circular reference detected while preparing "${val}" for query`
                    )
                }
                seen.push(val)
                return prepareValue(val.toPostgres(prepareValue), seen)
            }
            return JSON.stringify(val)
        },
    })
    client.queryCompiler = (builder) => new PostgresQueryCompiler(client, builder)
    client.queryBuilder = () => new PostgresQueryBuilder(client)
}

/**
 * Create monkey-patched knex.js Postgres query builder.
 */
export function newQueryBuilder() {
    client || createClient()
    return client.queryBuilder()
}
