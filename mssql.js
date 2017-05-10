
// Variables exported by this module can be imported by other packages and
// applications. See mssql-tests.js for an example of importing.
/**
 * @summary Namespace for MSSQLDB-related items
 * @namespace
 */
export const MSSQL = {

};

/**
 * @summary Create a MSSQL-style `ObjectID`.  If you don't specify a `hexString`, the `ObjectID` will generated randomly (not using MSSQLDB's ID construction rules).
 * @locus Anywhere
 * @class
 * @param {String} [hexString] Optional.  The 24-character hexadecimal contents of the ObjectID to create
 */
MSSQL.ObjectID = MongoID.ObjectID;
