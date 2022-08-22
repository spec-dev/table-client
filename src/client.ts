import config from './lib/config'
import {
    RecordTransform,
    SpecTableClientOptions,
    StringKeyMap,
    SpecTableQueryOptions,
} from './lib/types'
import { Knex } from 'knex'
import { ReadableStream } from 'node:stream/web'
import { JSONParser } from './json/index'
import { camelizeKeys } from 'humps'

const DEFAULT_OPTIONS = {
    origin: config.SHARED_TABLES_ORIGIN,
}

const DEFAULT_QUERY_OPTIONS = {
    transforms: [],
    camelResponse: false,
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
    async runQuery(query: Knex.QueryBuilder, options?: SpecTableQueryOptions): Promise<any> {
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
        const transforms = options?.transforms || []
        if (options?.camelResponse) {
            transforms.push((obj) => camelizeKeys(obj))
        }

        if (!transforms.length) {
            return result
        }

        if (!Array.isArray) {
            return this._transformRecord(result, transforms)
        }

        return result.map((r) => this._transformRecord(r, transforms)).filter((r) => !!r)
    }

    /**
     * Perform a query and stream the result.
     */
    async streamQuery(
        query: Knex.QueryBuilder,
        options?: SpecTableQueryOptions
    ): Promise<Response> {
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

        // Get response body as a readable stream.
        const reader = resp.body.getReader()
        if (!reader) throw 'Failed to attach reader to stream query.'

        // Create a JSON parser that parses every individual
        // record on-the-fly and applies the given transforms.
        const jsonParser = new JSONParser({
            stringBufferSize: undefined,
            paths: ['$.*'],
            keepStack: false,
        })

        // Add key-camelization as a transform if specified.
        const transforms = options?.transforms || []
        if (options?.camelResponse) {
            transforms.push((obj) => camelizeKeys(obj))
        }

        let streamController
        let streamClosed = false

        // Handle user-provided transforms and modify each record accordingly.
        jsonParser.onValue = (record) => {
            if (!record || streamClosed) return
            record = record as StringKeyMap

            // Enqueue error and close stream if error encountered.
            if (record.error) {
                enqueueJSON(record)
                streamController?.close()
                streamClosed = true
                return
            }

            // Apply any record transforms.
            const transformedRecord = this._transformRecord(record, transforms)
            if (!transformedRecord) return

            // Convert record back to buffer and enqueue it.
            enqueueJSON(transformedRecord)
        }

        const enqueueJSON = (data) => {
            try {
                const buffer = new TextEncoder().encode(JSON.stringify(data))
                streamController?.enqueue(buffer)
            } catch (err) {
                console.error('Error enqueueing JSON data', data)
            }
        }

        // Stream query results to a new stream response.
        const stream = new ReadableStream({
            start(controller) {
                streamController = controller
                async function pump() {
                    try {
                        if (streamClosed) return
                        const { done, value } = await reader.read()
                        value && jsonParser.write(value)
                        if (done) {
                            setTimeout(() => {
                                controller.close()
                                streamClosed = true
                            }, 10)
                            return
                        }
                        return pump()
                    } catch (err) {
                        abortController.abort()
                        throw err
                    }
                }
                return pump()
            },
            cancel() {
                abortController.abort()
            },
        })

        return new Response(stream, { headers: streamRespHeaders })
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
