import { SQLServerCollection } from './SQLServerCollection';
import { Sql } from './MSSQLDB';
import Future from 'fibers/future';

export class SQLServerDatabase
{
    constructor(options)
    {
        this.debug = false;
        this.options = options.settings;
        this.schemas = [];
    }

    attachSchema(collection, schema)
    {
        this.schemas[collection] = schema;
    }

    getSchema(collection)
    {
        return this.schemas[collection];
    }

    insert(query, callback)
    {
        return this.execute(query, callback);
    }

    remove(query, callback)
    {
        return this.execute(query, callback);
    }

    update(query, callback)
    {
        return this.execute(query, callback);
    }

    execute(query, callback)
    {
        let future = new Future();

        if(!query)
            throw "You must provide an statement query";

        Sql.driver.connect(this.options, error => {
            if(error)
                future.thrown(error);

            new Sql.driver.Request().query(query, function(error, result) {
                if(!error && callback)
                    callback(error, result);

                if(!error)
                    future['return'](result);
                else
                {
                    // Log query first to try it out on the management studio to see what exactly is giving an error
                    console.log(error);

                    future['throw'](error);
                }
            });
        });

        try {
            let result = future.wait();

            this.debug && console.log('SQLServerDatabase.execute:returning');

            this.debug && console.log('----------------------------------------');
            
            return result;
        }
        catch (e) {
            throw e;
        }
        finally
        {
            Sql.driver.close();
        }
    }

    collection(name){
        return new SQLServerCollection(name, this);
    }

    getConnection(){
        let future = new Future();

        if(!this.connection)
            this.connection = Sql.driver.connect(this.options, error => {
                throw error;
            });

        this.debug && console.log('SQLServerDatabase.getConnection');

        // This must be a ConnectionPool instance
        return this.connection;
    }

    getRequest()
    {
        let connection = this.getConnection();

        return new Sql.driver.Request(connection);
    }

    executeQuery(query, callback)
    {
        let future = new Future();
        let debug = this.debug;

        debug && console.log('SQLServerDatabase.executeQuery:')
        //this.debug && console.log(query);
        debug && console.log('SQLServerDatabase.executeQuery: Connecting');

        let sqlConnection = new Sql.driver.ConnectionPool(this.options, function(error){
            if(error)
                future['thrown'](error);

            debug && console.log('SQLServerDatabase.executeQuery: Querying');

            new Sql.driver.Request(sqlConnection).query(query, function(error, result) {
                if(!error)
                {
                    debug && console.log('SQLServerDatabase.executeQuery: Returning');

                    future['return'](result.recordset);

                    sqlConnection.close();
                }
                else
                {
                    debug && console.log('SQLServerDatabase.executeQuery:error ' + error.toString());

                    future['throw'](error);
                }
            });
        });

        debug && console.log('SQLServerDatabase.executeQuery: Waiting');

        return future.wait();
    }
}
