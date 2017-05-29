export class DatabaseCursor
{
    constructor(query, database)
    {
        this.query = query;
        this.database = database;

        this.read();
    }
}

DatabaseCursor.prototype.read = function()
{
    this.index = 0;
    let result = this.database.executeQuery(this.query);

    this.processResult(result);
};

DatabaseCursor.prototype.getTransform = function(){

    if(this.database.schema && this.database.schema.transform)
        return this.database.schema.transform;
    else
    {
        return function(record, index){
            // To simulate a mongo collection we need to set the _id property
            record._id = auxIndex;
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
    //console.log('nextObject');
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
