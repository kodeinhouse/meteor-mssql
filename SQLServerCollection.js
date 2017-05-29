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
        if(typeof selector == 'object')
        {
            if(Object.keys(selector).length == 0)
               query = `SELECT * FROM [${this.name}]`;
            else
            {
                // transform = { name => CompanyName }
                let properties = typeof this.database.schema.properties == 'object' ? this.database.schema.properties : {};
                let conditions = [];

                // selector = { name: 'XXXX' }
                for(let key in selector)
                {
                    if(properties.hasOwnProperty(key))
                        conditions.push({key: properties[key], value: selector[key]});
                    else
                        conditions.push({key: key, value: selector[key]});
                }

                let condition = conditions.map(function(item){
                    return `${item.key} = ${item.value}`;
                }).join(' ');

                query = `SELECT *
                         FROM [${this.name}]
                         WHERE ${condition}`;

                console.log(query);
            }
        }
        else
            throw new Error("Unknown selector type.");

    console.log('SQLServerCollection.find');
    console.log(selector);
    console.log(fields);
    console.log(options);

    return new DatabaseCursor(query, this.database);
};
