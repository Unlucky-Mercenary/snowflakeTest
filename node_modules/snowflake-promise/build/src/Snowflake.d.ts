import { Statement } from './Statement';
import { ConfigureOptions } from './types/ConfigureOptions';
import { ConnectionOptions } from './types/ConnectionOptions';
import { ExecuteOptions } from './types/ExecuteOptions';
import { LoggingOptions } from './types/LoggingOptions';
export declare class Snowflake {
    private readonly sdk_connection;
    private readonly logSql;
    /**
     * Creates a new Snowflake instance.
     *
     * @param connectionOptions The Snowflake connection options
     * @param loggingOptions Controls query logging and SDK-level logging
     * @param configureOptions Additional configuration options
     */
    constructor(connectionOptions: ConnectionOptions, loggingOptions?: LoggingOptions, configureOptions?: ConfigureOptions | boolean);
    /** the connection id */
    get id(): string;
    /** Establishes a connection if we aren't in a fatal state. */
    connect(): Promise<void>;
    /**
     * Immediately terminates the connection without waiting for currently
     * executing statements to complete.
     */
    destroy(): Promise<void>;
    /** Create a Statement. */
    createStatement(options: ExecuteOptions): Statement;
    /** A convenience function to execute a SQL statement and return the resulting rows. */
    execute(sqlText: string, binds?: any[]): Promise<any[]>;
}
