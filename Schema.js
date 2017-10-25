export class Schema
{
    constructor(config)
    {
        config = Object.assign({primaryKey: {
            identity: false // set identity false by default
        }}, config);

        Object.assign(this, config);

        this.transform = this.createTransform(config.transform);
    }

    /**
     * @type This is a private method
     * @param {Object} || {Function} It could be any of these
     */
    createTransform(transform)
    {
        if(transform)
        {
            let type = typeof transform;

            if(type == 'function')
                return transform;
            else
                if(type == 'object')
                {
                    let self = this;

                    let properties = Object.assign({}, transform);

                    if(properties.aliases)
                    {
                        // Extract optional properties
                        let aliases = properties.aliases || {};

                        // Remove that from the current object
                        delete properties.aliases;

                        // Assign to another property to use it for the INSERT and UPDATE statements
                        this.fields = Object.assign({}, properties);

                        // Merge properties
                        properties = Object.assign(properties, aliases);
                    }
                    else
                        this.fields = Object.assign({}, properties);

                    this.properties = properties;

                    return function(record, index){

                        let map = function(properties){
                            let clone = {};

                            for(let key in properties) {

                                // This is capable of transforming a plain object into an object like {name: 'CompanyName', contact: { name: 'ContactName', phone: 'ContactPhone'}}
                                if(typeof properties[key] != 'object')
                                {
                                    let field = properties[key];

                                    // Assign the property only if the record has it
                                    if(record.hasOwnProperty(field))
                                    {
                                        if(key != '_id') // Default mongodb primary key
                                            clone[key] = record[properties[key]];
                                        else
                                            clone[key] = record[properties[key]] != null ? record[properties[key]].toString() : null;
                                    }
                                    else
                                        false && console.log("Schema.transform: Ignoring field " + field);
                                }
                                else
                                {
                                    let result = map(properties[key]);

                                    // We don't want to create empty properties
                                    if(Object.keys(result).length > 0)
                                        clone[key] = result;
                                }
                            }

                            return clone;
                        };

                        return map(properties);
                    };
                }
                else
                    throw Error("Sorry, don't know how to process this transform.");
        }
        else
            return ((record) => {return record});
    }
}
