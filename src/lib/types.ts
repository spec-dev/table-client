export type StringKeyMap = { [key: string]: any }

export type SpecTableClientOptions = {
    origin?: string
}

export type SpecTableQueryOptions = {
    transforms?: RecordTransform[]
    camelResponse?: boolean
}

export type RecordTransform = (input: any) => Promise<any>
