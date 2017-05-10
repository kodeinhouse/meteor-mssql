export const MSSQLDB = Npm.require('mssql');
export const Sql = {driver: MSSQLDB};

// Ensure that EJSON.clone keeps a Timestamp as a Timestamp (instead of just
// doing a structural clone).
// XXX how ok is this? what if there are multiple copies of MSSQLDB loaded?
// HACK: Don't think we need this for SQL Server
MSSQLDB.Timestamp = function(){

};

MSSQLDB.Timestamp.prototype.clone = function () {
  // Timestamps should be immutable.
  return this;
};
