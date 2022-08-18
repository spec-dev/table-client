export type StringKeyMap = { [key: string]: any }

export type SpecTableClientOptions = {
    origin?: string
}

export type RecordTransform = (input: any) => Promise<any>
