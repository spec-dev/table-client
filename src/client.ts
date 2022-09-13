import config from './lib/config'
import {
    RecordTransform,
    SpecTableClientOptions,
    StringKeyMap,
    SpecTableQueryOptions,
} from './lib/types'
import { Knex } from 'knex'
import { JSONParser } from './json/index'
import { camelizeKeys } from 'humps'
import fetch, { Response } from 'node-fetch'

const DEFAULT_OPTIONS = {
    origin: config.SHARED_TABLES_ORIGIN,
}

const DEFAULT_QUERY_OPTIONS = {
    transforms: [],
    camelResponse: true,
}

const streamRespHeaders = {
    'Content-Type': 'application/json',
    'Transfer-Encoding': 'chunked',
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

    get streamUrl(): string {
        const url = new URL(this.origin)
        url.pathname = '/stream'
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

    /**
     * Perform a query and get the result.
     */
    async runQuery<T>(query: Knex.QueryBuilder, options?: SpecTableQueryOptions): Promise<T> {
        const opts = { ...DEFAULT_QUERY_OPTIONS, ...options }

        // Perform basic POST request.
        let resp: Response
        try {
            resp = await this._makeQueryRequest(this.queryUrl, this._packageQueryAsPayload(query))
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

        // Add key-camelization as a transform if specified.
        const transforms = opts?.transforms || []
        if (opts?.camelResponse) {
            transforms.push((obj) => camelizeKeys(obj))
        }

        if (!transforms.length) {
            return result as T
        }

        if (!Array.isArray) {
            return this._transformRecord(result, transforms)
        }

        return result.map((r) => this._transformRecord(r, transforms)).filter((r) => !!r) as T
    }

    /**
     * Perform a query and stream the result.
     */
    async streamQuery(query: Knex.QueryBuilder, writeable: any, options?: SpecTableQueryOptions) {
        const opts = { ...DEFAULT_QUERY_OPTIONS, ...options }

        // Make initial request.
        const abortController = new AbortController()
        let resp: Response
        try {
            resp = await this._makeQueryRequest(
                this.streamUrl,
                this._packageQueryAsPayload(query),
                abortController
            )
        } catch (err) {
            throw `Stream query request error: ${err}`
        }
        if (!resp || !resp.body) throw 'Stream query error - No response body'

        // Create a JSON parser that parses every individual
        // record on-the-fly and applies the given transforms.
        const jsonParser = new JSONParser({
            stringBufferSize: undefined,
            paths: ['$.*'],
            keepStack: false,
        })

        // Add key-camelization as a transform if specified.
        const transforms = opts?.transforms || []
        if (opts?.camelResponse) {
            transforms.push((obj) => camelizeKeys(obj))
        }

        let streamClosed = false
        let hasEnqueuedOpeningBracket = false
        let hasEnqueuedAnObject = false

        // Handle user-provided transforms and modify each record accordingly.
        jsonParser.onValue = (record) => {
            if (!record || streamClosed) return
            record = record as StringKeyMap

            if (!hasEnqueuedOpeningBracket) {
                writeable.write(new TextEncoder().encode('['))
                hasEnqueuedOpeningBracket = true
            }

            // Enqueue error and close stream if error encountered.
            if (record.error) {
                enqueueJSON(record)
                hasEnqueuedAnObject = true
                writeable.write(new TextEncoder().encode(']'))
                writeable.end()
                streamClosed = true
                return
            }

            // Apply any record transforms.
            const transformedRecord = this._transformRecord(record, transforms)
            if (!transformedRecord) return

            // Convert record back to buffer and enqueue it.
            enqueueJSON(transformedRecord)
            hasEnqueuedAnObject = true
        }

        const enqueueJSON = (data) => {
            try {
                let str = JSON.stringify(data)
                if (hasEnqueuedAnObject) {
                    str = ',' + str
                }
                const buffer = new TextEncoder().encode(str)
                writeable.write(buffer)
            } catch (err) {
                console.error('Error enqueueing JSON data', data)
            }
        }

        writeable.writeHead(200, streamRespHeaders)

        try {
            for await (let chunk of resp.body) {
                chunk && !streamClosed && jsonParser.write(chunk)
            }
            setTimeout(() => {
                writeable.write(new TextEncoder().encode(
                    hasEnqueuedOpeningBracket ? ']' : '[]'
                ))
                writeable.end()
                streamClosed = true
            }, 10)
        } catch (err) {
            throw `Readable stream iteration error: ${err}`
        }
    }

    /**
     * Run a record through a list of user-defined transforms.
     */
    _transformRecord(record: StringKeyMap, transforms: RecordTransform[] = []): any {
        let transformedRecord = record
        for (const transform of transforms) {
            transformedRecord = transform(transformedRecord)
            if (transformedRecord === null) break // support for filter transforms
        }
        return transformedRecord
    }

    /**
     * Initial query POST request.
     */
    async _makeQueryRequest(
        url: string,
        payload: StringKeyMap,
        abortController?: AbortController
    ): Promise<Response> {
        // Stringify body.
        let body
        try {
            body = JSON.stringify(payload)
        } catch (err) {
            throw `JSON error while packaging payload, ${payload}: ${err}`
        }

        // Create options with optional abort controller signal.
        const options: StringKeyMap = {
            method: 'POST',
            headers: this.requestHeaders,
            body,
        }
        if (abortController) {
            options.signal = abortController.signal
        }

        return fetch(url, options)
    }

    /**
     * Convert a knex query to it's raw sql and bindings.
     */
    _packageQueryAsPayload(query: Knex.QueryBuilder): StringKeyMap {
        try {
            return query.toSQL().toNative()
        } catch (err) {
            throw `Error building query: ${err}`
        }
    }
}
