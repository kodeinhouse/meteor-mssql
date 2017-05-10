# meteor-mssql

Meteor package to use SQL Server as backend database and minimongo over DDP  

We intend to provide a simple way to query SQL Server databases using the same pub/sub system as the first step to have reactive queries just like with Mongo.

This is a development version without any sql injection checking
## DO NOT USE IT IN PRODUCTION


### Installation

Download the repository anywhere and point the METEOR_PACKAGE_DIRS to the parent folder
Inside your application run 
```bash 
meteor add mssql
```

### Usage
Create your MSSQL Collection as follow as you normally do with Mongo's

```javascript 
import { MSSQL } from 'meteor/mssql';

export const Customers = new MSSQL.Collection("Customers");
```
In your publish functions you can use the .find method to get all records
```javascript
Meteor.publish("customers.all", function(condition){
     return Customers.find({});
});
```
or
```javascript
Meteor.publish("customers.all", function(condition){
    return Customers.find(`SELECT C.FirstName, C.LastName, P.Name, P.Price 
                              FROM Customers C
                                INNER JOIN Plan P
                                   ON C.PlanId = P.PlanId`);
});
```
SQL Queries works only on the server  

Queries will be cached by Meteor server and they will run every 10 seconds but data will be sent over the wire ONLY if a change is made to the database affecting the result of the query provided.

### Connection
In your settings file you need to add the connection options as follow:

```json
{
       "mssql": {
           "server"    : "your-server",
           "database"  : "your-database",
           "user"      : "your-username",
           "password"  : "your-password",
           "port"      : "your-port",
           "options"   : {
             "useUTC"     : false,
             "appName"    : "mssql-driver"   "// Set whatever name you like to identify your app
          }
      }
  }
 ```
