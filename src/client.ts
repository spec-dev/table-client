import config from './lib/config'
import { SpecTableClientOptions, StringKeyMap } from './lib/types'
import { Knex } from 'knex'
import { fetch } from 'cross-fetch'

const DEFAULT_OPTIONS = {
    origin: config.SHARED_TABLES_ORIGIN,
}

/**
 * Spec Table Client.
 *
 * A Javascript client for querying Spec's shared tables.
 */
export default class SpecTableClient {
    protected origin: string

    get queryUrl(): string {
        const url = new URL(this.origin)
        url.pathname = '/query'
        return url.toString()
    }

    get requestHeaders(): StringKeyMap {
        return {
            'Content-Type': 'application/json',
            // TODO: Some type of auth header here...
        }
    }

    /**
     * Create a new client instance.
     */
    constructor(options?: SpecTableClientOptions) {
        const settings = { ...DEFAULT_OPTIONS, ...options }
        this.origin = settings.origin
    }

    async runQuery(query: Knex.QueryBuilder): Promise<any> {
        // Perform basic JSON POST request.
        let resp: Response
        try {
            resp = await this._performBasicQuery(this._packageQueryAsPayload(query))
        } catch (err) {
            throw `Error running query: ${err}`
        }

        // Require 200 status to succeed.
        if (resp.status !== 200) {
            throw `Query failed with error status ${resp.status}`
        }

        // Parse JSON response.
        let result: any
        try {
            result = await resp.json()
        } catch (err) {
            throw `Query response error: Failed to parse JSON response data: ${err}`
        }

        return result
    }

    streamQuery(query: Knex.QueryBuilder) {
        // Create request
        // Perform request
        // Build response
        // Set up controller and shit
    }

    async _performBasicQuery(payload: StringKeyMap): Promise<Response> {
        let body
        try {
            body = JSON.stringify(payload)
        } catch (err) {
            throw `JSON error while packaging payload, ${payload}: ${err}`
        }
        return fetch(this.queryUrl, {
            method: 'POST',
            headers: this.requestHeaders,
            body,
        })
    }

    _packageQueryAsPayload(query: Knex.QueryBuilder): StringKeyMap {
        try {
            return query.toSQL().toNative()
        } catch (err) {
            throw `Error building query: ${err}`
        }
    }
}
