import { SQLServerCollection } from './SQLServerCollection';
import { Sql } from './MSSQLDB';
import Future from 'fibers/future';

export class SQLServerDatabase
{
    constructor(options)
    {
        this.options = options.settings;
        this.schema = options.schema;
    }
}

SQLServerDatabase.prototype.collection = function(name){
    return new SQLServerCollection(name, this);
};

SQLServerDatabase.prototype.getConnection = function(){
    if(!this.connection)
        this.connection = Sql.driver.connect(this.options);

    console.log('SQLServerDatabase.getConnection');
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
    console.log('SQLServerDatabase.executeQuery:before');

    this.getConnection().then(function(pool){
        new Sql.driver.Request().query(query).then(function(result){
            console.log('SQLServerDatabase.executeQuery:during');
            future['return'](result.recordset);
        });
    });

    console.log('SQLServerDatabase.executeQuery:waiting');
    let result = future.wait();
    console.log('SQLServerDatabase.executeQuery:returning');
    console.log('----------------------------------------');
    return result;
};
