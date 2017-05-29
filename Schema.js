export class Schema
{
    constructor(config)
    {
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
                    let properties = transform;

                    return function(record, index){
                        let clone = {};

                        for(let key in properties){
                            clone[key] = record[properties[key]];
                        }

                        return clone;
                    };
                }
                else
                    throw Error("Sorry, don't know how to process this transform.");
        }
        else
            return ((record) => {return record});
    }
}
