import { MSSQLConnection } from './MSSQLConnection';

Drivers = {};

RemoteCollectionDriver = function (options) {
  var self = this;
  console.log('Creating a new connection');

  self.mssql = new MSSQLConnection(options);
};

_.extend(RemoteCollectionDriver.prototype, {
  open: function (name) {
        var self = this;
        var ret = {};
        _.each(
          ['find', 'findOne', 'insert', 'update', 'upsert',
           'remove', '_ensureIndex', '_dropIndex', '_createCappedCollection',
           'dropCollection', 'rawCollection'],
          function (m) {
            ret[m] = _.bind(self.mssql[m], self.mssql, name);
          });
        return ret;
    },
    attachSchema: function(name, schema)
    {
        this.mssql.setSchema(name, schema);
    }
});


// Create the singleton RemoteCollectionDriver only on demand, so we
// only require MSSQL configuration if it's actually used (eg, not if
// you're only trying to receive data from a remote DDP server.)
Drivers.DefaultRemoteCollectionDriver = _.once(function (options) {
  return new RemoteCollectionDriver(Object.assign(options, {settings: Meteor.settings.mssql}));
});
