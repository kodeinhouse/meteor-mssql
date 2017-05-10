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

DatabaseCursor.prototype.processResult = function(result)
{
    let auxIndex = 0;
    this.records = result.map(function(record){
        record._id = auxIndex;
        auxIndex++;
        return record;
    });
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
