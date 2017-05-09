// Import Tinytest from the tinytest Meteor package.
import { Tinytest } from "meteor/tinytest";

// Import and rename a variable exported by mssql.js.
import { name as packageName } from "meteor/kodein:mssql";

// Write your tests here!
// Here is an example.
Tinytest.add('mssql - example', function (test) {
  test.equal(packageName, "mssql");
});
