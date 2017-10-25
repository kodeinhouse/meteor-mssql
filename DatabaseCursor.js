export class DatabaseCursor
{
    constructor(database, collection, query)
    {
        this.collection = collection;
        this.database = database;
        this.query = query;

        this.read();
    }
}

DatabaseCursor.prototype.read = function()
{
    this.index = 0;
    let result = [];

    try {
        result = this.database.executeQuery(this.query);
    }
    catch (e) {
        throw e;
    }
    finally {
        this.processResult(result);
    }
};

DatabaseCursor.prototype.getTransform = function(){

    let schema = this.database.getSchema(this.collection);

    if(schema && schema.transform)
        return schema.transform;
    else
    {
        return function(record, index){
            // To simulate a mongo collection we need to set the _id property
            record._id = auxIndex;

            return record;
        };
    }
};

DatabaseCursor.prototype.processResult = function(result)
{
    let transform = this.getTransform();
    
    this.records = result.map(transform);
};

DatabaseCursor.prototype.nextObject = function(callback)
{
    return callback(null, this.records[this.index++]);
};

DatabaseCursor.prototype.count = function(callback)
{
    return callback(null, this.records.length);
};

DatabaseCursor.prototype.rewind = function()
{
    this.read();
};
