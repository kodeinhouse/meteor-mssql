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

SQLServerDatabase.prototype.executeQuery = function(query)
{
    let future = new Future();
    this.debug && console.log('SQLServerDatabase.executeQuery:before ' + query);

    this.getConnection().then(function(pool){
        new Sql.driver.Request().query(query).then(function(result){
            this.debug && console.log('SQLServerDatabase.executeQuery:during');
            future['return'](result.recordset);
        });
    });

    this.debug && console.log('SQLServerDatabase.executeQuery:waiting');
    let result = future.wait();
    this.debug && console.log('SQLServerDatabase.executeQuery:returning');
    this.debug && console.log('----------------------------------------');
    return result;
};
