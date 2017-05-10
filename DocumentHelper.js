import { MSSQLDB } from './MSSQLDB';

export const replaceTypes = function (document, atomTransformer) {
  if (typeof document !== 'object' || document === null)
    return document;

  var replacedTopLevelAtom = atomTransformer(document);
  if (replacedTopLevelAtom !== undefined)
    return replacedTopLevelAtom;

  var ret = document;
  _.each(document, function (val, key) {
    var valReplaced = replaceTypes(val, atomTransformer);
    if (val !== valReplaced) {
      // Lazy clone. Shallow copy.
      if (ret === document)
        ret = _.clone(document);
      ret[key] = valReplaced;
    }
  });
  return ret;
};

export const replaceMSSQLAtomWithMeteor = function (document) {
  /*if (document instanceof MSSQLDB.Binary) {
    var buffer = document.value(true);
    return new Uint8Array(buffer);
}*/
  /*if (document instanceof MSSQLDB.ObjectID) {
    return new MSSQL.ObjectID(document.toHexString());
}*/
  if (document["EJSON$type"] && document["EJSON$value"] && _.size(document) === 2) {
    return EJSON.fromJSONValue(replaceNames(unmakeMSSQLLegal, document));
  }
  if (document instanceof MSSQLDB.Timestamp) {
    // For now, the Meteor representation of a MSSQL timestamp type (not a date!
    // this is a weird internal thing used in the oplog!) is the same as the
    // MSSQL representation. We need to do this explicitly or else we would do a
    // structural clone and lose the prototype.
    return document;
  }
  return undefined;
};

// This is used to add or remove EJSON from the beginning of everything nested
// inside an EJSON custom type. It should only be called on pure JSON!
export const replaceNames = function (filter, thing) {
  if (typeof thing === "object") {
    if (_.isArray(thing)) {
      return _.map(thing, _.bind(replaceNames, null, filter));
    }
    var ret = {};
    _.each(thing, function (value, key) {
      ret[filter(key)] = replaceNames(filter, value);
    });
    return ret;
  }
  return thing;
};

export const makeMSSQLLegal = function (name) { return "EJSON" + name; };
export const unmakeMSSQLLegal = function (name) { return name.substr(5); };
