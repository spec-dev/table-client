import config from './lib/config'
import { RecordTransform, SpecTableClientOptions, StringKeyMap } from './lib/types'
import { Knex } from 'knex'
import { ReadableStream } from 'node:stream/web'
import { JSONParser } from './json/index'

const DEFAULT_OPTIONS = {
    origin: config.SHARED_TABLES_ORIGIN,
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

    async runQuery(query: Knex.QueryBuilder): Promise<any> {
        // Perform basic JSON POST request.
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

        return result
    }

    async streamQuery(
        query: Knex.QueryBuilder,
        transforms: RecordTransform[] = []
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

        // Create a JSON parser that parses every individual record on-the-fly from
        // the query stream and applies the given transforms (if any are provided).
        const jsonparser = new JSONParser({
            stringBufferSize: undefined,
            paths: ['$.*'],
            keepStack: false,
        })

        // Handle user-provided transforms and modify each record accordingly.
        let streamController
        jsonparser.onValue = async (record) => {
            const endRecord = await this._transformRecord(record, transforms)
            const buffer = new TextEncoder().encode(JSON.stringify(endRecord))
            streamController?.enqueue(buffer)
        }

        // Send result segment over the wire.
        const enqueue = (value) =>
            (transforms || []).length > 0
                ? jsonparser.write(value)
                : streamController.enqueue(value)

        // Stream query results to a new stream response.
        const stream = new ReadableStream({
            start(controller) {
                streamController = controller
                async function pump() {
                    try {
                        const { done, value } = await reader.read()
                        if (done) {
                            controller.close()
                            return
                        }
                        enqueue(value)
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

    async _transformRecord(record: any, transforms: RecordTransform[] = []): Promise<any> {
        let transformedRecord = record
        for (const transform of transforms) {
            transformedRecord = await transform(transformedRecord)
        }
        return transformedRecord
    }

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

    _packageQueryAsPayload(query: Knex.QueryBuilder): StringKeyMap {
        try {
            return query.toSQL().toNative()
        } catch (err) {
            throw `Error building query: ${err}`
        }
    }
}
