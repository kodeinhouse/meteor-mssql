import { DatabaseCursor } from './DatabaseCursor';

export class SQLServerCollection
{
    constructor( name, database){
        this.name = name;
        this.database = database;
    }
}

SQLServerCollection.prototype.find = function(selector, fields, options)
{
    let query = null;

    if(typeof selector == 'string')
        query = selector;
    else
        if(typeof selector == 'object' && Object.keys(selector).length == 0)
            query = `SELECT * FROM [${this.name}]`;
        else
        {
            console.log(selector);
            throw new Error("Selector with conditions is not implemented");
        }


    console.log('SQLServerCollection.find');
    console.log(selector);
    console.log(fields);
    console.log(options);

    return new DatabaseCursor(query, this.database);
};
