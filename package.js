Package.describe({
  name: 'mssql',
  version: '0.0.1',
  // Brief, one-line summary of the package.
  summary: 'Meteor package to use SQL Server as backend database and minimongo over DDP',
  // URL to the Git repository containing the source code for this package.
  git: 'https://github.com/kodeinhouse/meteor-mssql',
  // By default, Meteor will default to using README.md for documentation.
  // To avoid submitting documentation, set this field to null.
  documentation: 'README.md'
});

Npm.depends({
  "mssql": "4.0.4"
});

Package.onUse(function(api) {
  api.versionsFrom('1.4.4.2');
  api.use('ecmascript');
  api.use('allow-deny');

  api.use([
    'random',
    'ejson',
    'underscore',
    'minimongo',
    'ddp',
    'tracker',
    'diff-sequence',
    'mongo-id',
    'check',
    'ecmascript'
  ]);

  // Binary Heap data structure is used to optimize oplog observe driver
  // performance.
  api.use('binary-heap', 'server');

  // Allow us to detect 'insecure'.
  api.use('insecure', {weak: true});

  // Allow us to detect 'autopublish', and publish collections if it's loaded.
  api.use('autopublish', 'server', {weak: true});

  // Allow us to detect 'disable-oplog', which turns off oplog tailing for your
  // app even if it's configured in the environment. (This package will be
  // probably be removed before 1.0.)
  api.use('disable-oplog', 'server', {weak: true});

  // defaultRemoteCollectionDriver gets its deployConfig from something that is
  // (for questionable reasons) initialized by the webapp package.
  api.use('webapp', 'server', {weak: true});

  // If the facts package is loaded, publish some statistics.
  api.use('facts', 'server', {weak: true});

  api.use('callback-hook', 'server');

  api.export('Drivers', 'server');
  api.export('MSSQL');

  api.mainModule('mssql.js');

  api.addFiles('Collection.js', ['client', 'server']);
  api.addFiles('RemoteCollectionDriver.js', ['server']);
});

Package.onTest(function(api) {
  api.use('ecmascript');
  api.use('tinytest');
  api.use('mssql');
  api.use('check');
  api.use(['tinytest', 'underscore', 'test-helpers', 'ejson', 'random',
           'ddp', 'base64']);
  api.mainModule('mssql-tests.js');
});
