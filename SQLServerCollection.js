import { DatabaseCursor } from './DatabaseCursor';

export class SQLServerCollection
{
    constructor( name, database){
        this.debug = false;
        this.name = name;
        this.database = database;
        this.schema = database.getSchema(name);
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
            {
                query = 'SELECT ';
                query += options && options.limit ? `TOP ${options.limit} ` : '';
                query += `* `;
                query += `FROM [${this.name}] `;
            }
            else
            {
                // transform = { name => CompanyName }
                let properties = typeof this.schema.properties == 'object' ? this.schema.properties : {};
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

                query = 'SELECT ';
                query += options && options.limit ? `TOP ${options.limit} ` : '';
                query += `* `;
                query += `FROM [${this.name}] `;
                query += `WHERE ${condition}`;

                console.log(query);
            }
        }
        else
            throw new Error("Unknown selector type.");

    console.log('SQLServerCollection.find');
    this.debug && console.log(selector);
    this.debug && console.log(fields);
    this.debug && console.log(options);

    return new DatabaseCursor(this.database, this.name, query);
};
