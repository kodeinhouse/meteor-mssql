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
                    let self = this;

                    self.properties = transform;

                    return function(record, index){

                        let processKeys = function(properties){
                            let clone = {};

                            for(let key in properties){
                                if(typeof properties[key] != 'object')
                                    clone[key] = record[properties[key]];
                                else
                                    clone[key] = processKeys(properties[key]);
                            }

                            return clone;
                        };

                        return processKeys(self.properties);
                    };
                }
                else
                    throw Error("Sorry, don't know how to process this transform.");
        }
        else
            return ((record) => {return record});
    }
}
