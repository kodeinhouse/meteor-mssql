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
                    {
                        if(columns.indexOf(properties[key]) == -1)
                            columns.push(properties[key]);
                    }
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

    getEqualOperator(value)
    {
        return (value == null ? 'IS' : '=');
    }

    getNotEqualOperator(value){
        return (value == null ? 'IS NOT' : '<>');
    }

    getGreaterThanOrEqualOperator(value){
        return '>=';
    }

    getGreaterThanOperator(value){
        return '>';
    }

    getLessThanOrEqualOperator(value){
        return '<=';
    }

    getLessThanOperator(value){
        return '<';
    }

    getINOperator(value){
        return 'IN';
    }

    getSQLOperator(operator, value){
        if(operator == '$eq')
            return this.getEqualOperator(value);
        else
            if(operator == '$ne')
                return this.getNotEqualOperator(value);
            else
                if(operator == '$lte')
                    return this.getLessThanOrEqualOperator(value);
                else
                    if(operator == '$gte')
                        return this.getGreaterThanOrEqualOperator(value);
                    else
                        if(operator == '$lt')
                            return this.getLessThanOperator(value);
                        else
                            if(operator == '$gt')
                                return this.getGreaterThanOperator(value);
                            else
                                if(operator == '$in')
                                    return this.getINOperator(value);
                                else
                                    throw `Operator ${operator} not implemented.`;
    }

    getOperator(property)
    {
        if(property.hasOwnProperty('$eq'))
            return '$eq';
        else
            if(property.hasOwnProperty('$ne'))
                return '$ne';
            else
                if(property.hasOwnProperty('$gte'))
                    return '$gte';
                else
                    if(property.hasOwnProperty('$gt'))
                        return '$gt';
                    else
                        if(property.hasOwnProperty('$lte'))
                            return '$lte';
                        else
                            if(property.hasOwnProperty('$lt'))
                                return '$lt';
                            else
                                if(property.hasOwnProperty('$in'))
                                    return '$in';
                                else
                                    throw `Operator ${operator} not implemented.`;
    }

    getCondition(item)
    {
        let property = item.value;
        let operator = this.getOperator(property);
        let value = property[operator];

        if(value == null)
            return `${item.key} ${this.getSQLOperator(operator, value)} NULL`;
        else
            return `${item.key} ${this.getSQLOperator(operator, value)} ${this.getSQLValue(value)}`;
    }

    getConditions(selector)
    {
        // transform = { name: CompanyName }
        let properties = this.getSchemaProperties();
        let conditions = [];
        let self = this;

        // selector = { name: 'XXXX' }
        for(let key in selector)
        {
            if(properties.hasOwnProperty(key))
                conditions.push({key: properties[key], value: selector[key]});
            else
                conditions.push({key: key, value: selector[key]});
        }

        conditions = conditions.map(function(item){
            if(item.value != null)
            {
                if(item.value.constructor !== Object)
                    return `${item.key} = ${self.getSQLValue(item.value)}`;
                else
                    return self.getCondition(item); // The value was specified as an object like {$ne: 'xxx'}
            }
            else
                return `${item.key} IS NULL`;
        });

        return conditions.join(' AND ');
    }

    getSort(fields)
    {
        let sorts = [];
        let properties = this.getSchemaProperties();

        for(let key in fields)
        {
            sorts.push(properties[key] + ' ' + (fields[key] == 1 ? 'ASC' : 'DESC'));
        }

        return sorts.join(', ');
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
                    let sort = this.getSort(options.sort);

                    if(Object.keys(selector).length == 0)
                    {
                        query = 'SELECT ';
                        query += options && options.limit ? `TOP ${options.limit} ` : '';
                        query += this.getFields(fields) + ' ';
                        query += `FROM [${this.name}] `;
                        query += (sort ? `ORDER BY ${sort}` : '');
                    }
                    else
                    {
                        let conditions = this.getConditions(selector);

                        query = 'SELECT ';
                        query += options && options.limit ? `TOP ${options.limit} ` : '';
                        query += this.getFields(fields) + ' ';
                        query += `FROM [${this.name}] `;
                        query += `WHERE ${conditions} `;
                        query += (sort ? `ORDER BY ${sort}` : '');
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
                        if(value == null)
                            return 'NULL';
                        else
                            if(Array.isArray(value))
                                return `(${value.join(', ')})`;
                            else
                                throw Error("SQL value transformation is not implemented.");
    }

    getSQLProperties(fields, properties)
    {
        let items = [];

        //{ name: 'XXXX', contact: {firstName: 'dfaf'}}

        let convertProperties = function(fields, properties)
        {
            for(let key in fields)
            {
                if(typeof properties[key] == 'undefined')
                    continue;
                else
                    if(typeof properties[key] != 'object')
                        items.push({key: properties[key], value: fields[key]});
                    else
                        if(Object.keys(properties[key]).length > 0)
                            convertProperties(fields[key], properties[key]);
            }
        };

        convertProperties(fields, properties);

        return items.map(c => {return `[${c.key}] = ${this.getSQLValue(c.value)}`; }).join(', ');
    }

    getTableName()
    {
        return this.schema.table ? this.schema.table : this.name;
    }

    find(selector, fields, options)
    {
        try {
            let query = this.getQuery(selector, fields, options);

            this.debug && console.log('SQLServerCollection.find');
            this.debug && console.log(selector);
            this.debug && console.log(fields);
            this.debug && console.log(options);

            return new DatabaseCursor(this.database, this.name, query);
        } catch (e) {
            console.log("Error while building query: " + e.toString());
            throw e;
        }
    }

    insert(fields, options, callback)
    {
        let schema = this.schema;
        let mapping = schema.fields;
        let properties = [];

        for(let key in fields)
        {
            if(mapping.hasOwnProperty(key))
                properties.push({key: mapping[key], value: fields[key]});
        }

        // Thinking currently only on a table with an IDENTITY primary key
        if(schema.primaryKey.identity && fields.hasOwnProperty('_id'))
            properties.splice(properties.map(c => { return c.key}).indexOf(mapping['_id']), 1);

        let fieldsPart = properties.map(c => { return `[${c.key}]`; }).join(', ');
        let valuesPart = properties.map(c => { return this.getSQLValue(c.value)}).join(', ');

        let query = `INSERT INTO [${this.getTableName()}] (${fieldsPart}) VALUES (${valuesPart});`;

        if(schema.primaryKey.identity)
            query += 'SELECT SCOPE_IDENTITY() AS id';

        // This result has full response object from the SQL.Request result
        let id = null;

        this.database.insert(query, function(error, result){
            if(!error && result)
            {
                if(schema.primaryKey.identity)
                {
                    result = Array.isArray(result.recordset) && result.recordset.length > 0 ? result.recordset[0].id : null;
                }
                else
                    result = fields._id;

                id = result;
            }

            // Callback coming from MSSQLConnection expectes the generated id
            // This will never be called on error
            callback(error, result);
        });

        return id;
    }

    update(selector, fields, options, callback)
    {
        let properties = this.getSQLProperties(fields.$set, this.schema.fields);

        let conditions = this.getConditions(selector);

        let query = `UPDATE [${this.getTableName()}] SET ${properties} WHERE ${conditions}`;

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
        let conditions = this.getSQLProperties(selector, this.getSchemaProperties());

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
