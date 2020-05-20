export declare type FetchAsStringTypes = 'String' | 'Boolean' | 'Number' | 'Date' | 'JSON';
export interface ExecuteOptions {
    sqlText: string;
    binds?: any[];
    streamResult?: boolean;
    fetchAsString?: FetchAsStringTypes[];
}
