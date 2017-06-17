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

        this.getConnection().then(function(pool){
            let request = new Sql.driver.Request();

            request.query(query, function(error, result){
                if(!error && callback)
                    callback(error, result);

                if(!error)
                    future['return'](result);
                else
                {
                    // Log query first to try it out on the management studio to see what exactly is giving an error
                    console.log(query);

                    future['throw'](error);
                }
            });
        });

        return future.wait();
    }
}

SQLServerDatabase.prototype.collection = function(name){
    return new SQLServerCollection(name, this);
};

SQLServerDatabase.prototype.getConnection = function(){
    if(!this.connection)
        this.connection = Sql.driver.connect(this.options);

    this.debug && console.log('SQLServerDatabase.getConnection');
    return this.connection;
};

SQLServerDatabase.prototype.getRequest = function()
{
    let connection = this.getConnection();

    return new Sql.driver.Request(connection);
};

SQLServerDatabase.prototype.executeQuery = function(query, callback)
{
    let future = new Future();

    this.debug && console.log('SQLServerDatabase.executeQuery:before ' + query);

    this.getConnection().then(function(pool){
        let request = new Sql.driver.Request();

        request.query(query, function(error, result) {
            if(!error)
            {
                this.debug && console.log('SQLServerDatabase.executeQuery:during');

                future['return'](result.recordset);
            }
            else
                console.log(error);
        });
    });

    this.debug && console.log('SQLServerDatabase.executeQuery:waiting');
    try {
        let result = future.wait();

        this.debug && console.log('SQLServerDatabase.executeQuery:returning');

        this.debug && console.log('----------------------------------------');

        return result;
    }
    catch (e) {
        console.log(e);
    }
    finally {

    }
};
