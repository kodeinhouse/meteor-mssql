import { DatabaseCursor } from './DatabaseCursor';

export class SQLServerCollection
{
    constructor( name, database){
        this.debug = false;
        this.name = name;
        this.database = database;
        this.schema = database.getSchema(name);
    }

    getSchemaProperties()
    {
        return typeof this.schema.properties == 'object' ? this.schema.properties : {};
    }

    getFields(fields)
    {
        let properties = this.getSchemaProperties();
        let columns = [];

        if(fields != null)
        {
            if(properties.aliases != null)
            {
                // Extract optional properties
                let aliases = properties.aliases || {};

                // Remove that from the current object
                delete properties.aliases;

                // Merget properties
                properties = Object.assign(properties, aliases);
            }

            columns = Object.keys(fields).filter(c => { return fields[c] == 1}).map(c => { return properties[c]});

            if(columns.length == 0)
                throw new Error("Invalid fields selector.");
        }
        else
        {
            // By default only core properties needs to be translated into the selector
            delete properties.aliases;

            let processKeys = function(properties){
                let columns = [];

                for(let key in properties){
                    // This is capable of transforming a plain object into an object like {name: 'CompanyName', contact: { name: 'ContactName', phone: 'ContactPhone'}}
                    if(typeof properties[key] != 'object')
                        columns.push(properties[key]);
                    else
                    {
                        let items = processKeys(properties[key]).filter(c => columns.indexOf(c) == -1);

                        columns = columns.concat(items);
                    }
                }

                return columns;
            };

            columns = processKeys(properties);
        }

        return columns.map(c => { return `[${c}]`}).join(', ');
    }

    getCondition(selector)
    {
        // transform = { name: CompanyName }
        let properties = this.getSchemaProperties();
        let conditions = [];

        // selector = { name: 'XXXX' }
        for(let key in selector)
        {
            if(properties.hasOwnProperty(key))
                conditions.push({key: properties[key], value: selector[key]});
            else
                conditions.push({key: key, value: selector[key]});
        }

        conditions = conditions.map(function(item){
            return `${item.key} = ${item.value}`;
        })

        return conditions.join(' ');
    }

    getQuery(selector, fields, options)
    {
        let query = null;

        if(options.query)
            query = options.query;
        else
            if(typeof selector == 'string')
                query = selector;
            else
                if(typeof selector == 'object')
                {
                    if(Object.keys(selector).length == 0)
                    {
                        query = 'SELECT ';
                        query += options && options.limit ? `TOP ${options.limit} ` : '';
                        query += this.getFields(fields) + ' ';
                        query += `FROM [${this.name}] `;
                    }
                    else
                    {
                        let condition = this.getCondition(selector);

                        query = 'SELECT ';
                        query += options && options.limit ? `TOP ${options.limit} ` : '';
                        query += this.getFields(fields) + ' ';
                        query += `FROM [${this.name}] `;
                        query += `WHERE ${condition}`;
                    }
                }
                else
                    throw new Error("Unknown selector type.");

        this.debug && console.log(query);

        return query;
    }

    getSQLValue(value)
    {
        if(typeof value == 'string')
            return `'${value}'`;
        else
            if(typeof value == 'number')
                return value;
            else
                if(value instanceof Date)
                {
                    value = value.toISOString().slice(0, 19).replace('T', ' ');

                    return `'${value}'`;
                }
                else
                    if(typeof value == 'boolean')
                        return value ? 1 : 0;
                    else
                        return 'NULL';
    }

    getSQLProperties(fields)
    {
        let properties = this.getSchemaProperties();
        let items = [];

        for(let key in fields)
        {
            items.push({key: properties[key], value: fields[key]});
        }

        return items.map(c => {return `[${c.key}] = ${this.getSQLValue(c.value)}`; }).join(', ');
    }

    find(selector, fields, options)
    {
        let query = this.getQuery(selector, fields, options);

        this.debug && console.log('SQLServerCollection.find');
        this.debug && console.log(selector);
        this.debug && console.log(fields);
        this.debug && console.log(options);

        return new DatabaseCursor(this.database, this.name, query);
    }

    insert(fields, options, callback)
    {
        let mapping = this.getSchemaProperties();
        let properties = [];

        for(let key in fields)
        {
            properties.push({key: mapping[key], value: fields[key]});
        }

        // Thinking currently only on a table with an IDENTITY primary key
        properties.splice(properties.map(c => { return c.key}).indexOf(mapping['_id']), 1);

        let fieldsPart = properties.map(c => { return `[${c.key}]`; }).join(', ');
        let valuesPart = properties.map(c => { return this.getSQLValue(c.value)}).join(', ');

        let query = `INSERT INTO [${this.name}] (${fieldsPart}) VALUES (${valuesPart}); SELECT SCOPE_IDENTITY() AS id`;

        return this.database.insert(query, function(error, result){
            if(!error && result)
                result = Array.isArray(result.recordset) && result.recordset.length > 0 ? result.recordset[0].id : null;

            // Callback coming from MSSQLConnection expectes the generated id
            // This will never be called on error
            callback(error, result);
        });
    }

    update(selector, fields, options, callback)
    {
        let properties = this.getSQLProperties(fields.$set);
        let conditions = this.getSQLProperties(selector);

        let query = `UPDATE [${this.name}] SET ${properties} WHERE ${conditions}`;

        return this.database.update(query, function(error, result){

            if(result)
            {
                let rowsAffected = result.rowsAffected;

                // Callback coming from MSSQLConnection expectes an object in the following form {result: {n: #}}
                result = {result: {n: (Array.isArray(rowsAffected) && rowsAffected.length > 0 ? rowsAffected[0] : null)}};
            }

            callback(error, result);
        });
    }

    remove(selector, options, callback)
    {
        let conditions = this.getSQLProperties(selector);

        let query = `DELETE FROM [${this.name}] WHERE ${conditions}`;

        return this.database.update(query, function(error, result){

            if(result)
            {
                let rowsAffected = result.rowsAffected;

                // Callback coming from MSSQLConnection expectes an object in the following form {result: {n: #}}
                result = {result: {n: (Array.isArray(rowsAffected) && rowsAffected.length > 0 ? rowsAffected[0] : null)}};
            }

            callback(error, result);
        });
    }
}
