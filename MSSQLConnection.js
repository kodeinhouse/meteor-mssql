import { MSSQLDB } from './MSSQLDB';
import { Cursor } from './Cursor';
import { CursorDescription } from './CursorDescription';
import { replaceTypes, makeMSSQLLegal } from './DocumentHelper';
import { SynchronousCursor } from './SynchronousCursor';
import { SQLServerDatabase } from './SQLServerDatabase';
import { SQLServerCollection } from './SQLServerCollection';
import { ObserveMultiplexer } from './ObserveMultiplexer';
import { PollingObserveDriver } from './PollingObserveDriver';

MSSQLConnection = function (options) {
  var self = this;

  this.options = options || {}; // TODO: Validate the options property
  self._observeMultiplexers = {};
  self._onFailoverHook = new Hook;

  // HACK: I don't think we need any of these for sql server
  /*
  var mongoOptions = Object.assign({
    // Reconnect on error.
    autoReconnect: true,
    // Try to reconnect forever, instead of stopping after 30 tries (the
    // default), with each attempt separated by 1000ms.
    reconnectTries: Infinity
  }, MSSQL._connectionOptions);

  // Disable the native parser by default, unless specifically enabled
  // in the mongo URL.
  // - The native driver can cause errors which normally would be
  //   thrown, caught, and handled into segfaults that take down the
  //   whole app.
  // - Binary modules don't yet work when you bundle and move the bundle
  //   to a different platform (aka deploy)
  // We should revisit this after binary npm module support lands.
  if (!(/[\?&]native_?[pP]arser=/.test(url))) {
    mongoOptions.native_parser = false;
  }

  // Internally the oplog connections specify their own poolSize
  // which we don't want to overwrite with any user defined value
  if (_.has(options, 'poolSize')) {
    // If we just set this for "server", replSet will override it. If we just
    // set it for replSet, it will be ignored if we're not using a replSet.
    mongoOptions.poolSize = options.poolSize;
  }

  self.db = null;
  // We keep track of the ReplSet's primary, so that we can trigger hooks when
  // it changes.  The Node driver's joined callback seems to fire way too
  // often, which is why we need to track it ourselves.
  self._primary = null;
  self._oplogHandle = null;
  self._docFetcher = null;


  var connectFuture = new Future;
  MSSQLDB.connect(
    url,
    mongoOptions,
    Meteor.bindEnvironment(
      function (err, db) {
        if (err) {
          throw err;
        }

        // First, figure out what the current primary is, if any.
        if (db.serverConfig.isMasterDoc) {
          self._primary = db.serverConfig.isMasterDoc.primary;
        }

        db.serverConfig.on(
          'joined', Meteor.bindEnvironment(function (kind, doc) {
            if (kind === 'primary') {
              if (doc.primary !== self._primary) {
                self._primary = doc.primary;
                self._onFailoverHook.each(function (callback) {
                  callback();
                  return true;
                });
              }
            } else if (doc.me === self._primary) {
              // The thing we thought was primary is now something other than
              // primary.  Forget that we thought it was primary.  (This means
              // that if a server stops being primary and then starts being
              // primary again without another server becoming primary in the
              // middle, we'll correctly count it as a failover.)
              self._primary = null;
            }
          }));

        // Allow the constructor to return.
        connectFuture['return'](db);
      },
      connectFuture.resolver()  // onException
    )
  );

  // Wait for the connection to be successful; throws on failure.
  self.db = connectFuture.wait();

  if (options.oplogUrl && ! Package['disable-oplog']) {
    self._oplogHandle = new OplogHandle(options.oplogUrl, self.db.databaseName);
    self._docFetcher = new DocFetcher(self);
    }*/

    self.db = new SQLServerDatabase(options);
};

MSSQLConnection.prototype.close = function() {
  var self = this;

  if (! self.db)
    throw Error("close called before Connection created?");

  // XXX probably untested
  var oplogHandle = self._oplogHandle;
  self._oplogHandle = null;
  if (oplogHandle)
    oplogHandle.stop();

  // Use Future.wrap so that errors get thrown. This happens to
  // work even outside a fiber since the 'close' method is not
  // actually asynchronous.
  Future.wrap(_.bind(self.db.close, self.db))(true).wait();
};

// Returns the MSSQL Collection object; may yield.
MSSQLConnection.prototype.rawCollection = function (collectionName) {
  var self = this;

  if (! self.db)
    throw Error("rawCollection called before Connection created?");

  // Our implementation doesn't use a collection object like mongo
  return self.db.collection(collectionName);
};

MSSQLConnection.prototype._createCappedCollection = function (
    collectionName, byteSize, maxDocuments) {
  var self = this;

  if (! self.db)
    throw Error("_createCappedCollection called before Connection created?");

  var future = new Future();
  self.db.createCollection(
    collectionName,
    { capped: true, size: byteSize, max: maxDocuments },
    future.resolver());
  future.wait();
};

// This should be called synchronously with a write, to create a
// transaction on the current write fence, if any. After we can read
// the write, and after observers have been notified (or at least,
// after the observer notifiers have added themselves to the write
// fence), you should call 'committed()' on the object returned.
MSSQLConnection.prototype._maybeBeginWrite = function () {
  var fence = DDPServer._CurrentWriteFence.get();
  if (fence) {
    return fence.beginWrite();
  } else {
    return {committed: function () {}};
  }
};

// Internal interface: adds a callback which is called when the MSSQL primary
// changes. Returns a stop handle.
MSSQLConnection.prototype._onFailover = function (callback) {
  return this._onFailoverHook.register(callback);
};

MSSQLConnection.prototype._insert = function (collection_name, document,
                                              callback) {
  var self = this;

  var sendError = function (e) {
    if (callback)
      return callback(e);
    throw e;
  };

  if (collection_name === "___meteor_failure_test_collection") {
    var e = new Error("Failure test");
    e.expected = true;
    sendError(e);
    return;
  }

  if (!(LocalCollection._isPlainObject(document) &&
        !EJSON._isCustomType(document))) {
    sendError(new Error(
      "Only plain objects may be inserted into MSSQLDB"));
    return;
  }

  var write = self._maybeBeginWrite();
  var refresh = function () {
    Meteor.refresh({collection: collection_name, id: document._id });
  };
  callback = bindEnvironmentForWrite(writeCallback(write, refresh, callback));
  try {
    var collection = self.rawCollection(collection_name);
    collection.insert(replaceTypes(document, replaceMeteorAtomWithMSSQL),
                      {safe: true}, callback);
  } catch (err) {
    write.committed();
    throw err;
  }
};

// Cause queries that may be affected by the selector to poll in this write
// fence.
MSSQLConnection.prototype._refresh = function (collectionName, selector) {
  var refreshKey = {collection: collectionName};
  // If we know which documents we're removing, don't poll queries that are
  // specific to other documents. (Note that multiple notifications here should
  // not cause multiple polls, since all our listener is doing is enqueueing a
  // poll.)
  var specificIds = LocalCollection._idsMatchedBySelector(selector);
  if (specificIds) {
    _.each(specificIds, function (id) {
      Meteor.refresh(_.extend({id: id}, refreshKey));
    });
  } else {
    Meteor.refresh(refreshKey);
  }
};

MSSQLConnection.prototype._remove = function (collection_name, selector,
                                              callback) {
  var self = this;

  if (collection_name === "___meteor_failure_test_collection") {
    var e = new Error("Failure test");
    e.expected = true;
    if (callback) {
      return callback(e);
    } else {
      throw e;
    }
  }

  var write = self._maybeBeginWrite();
  var refresh = function () {
    self._refresh(collection_name, selector);
  };
  callback = bindEnvironmentForWrite(writeCallback(write, refresh, callback));

  try {
    var collection = self.rawCollection(collection_name);
    var wrappedCallback = function(err, driverResult) {
      callback(err, transformResult(driverResult).numberAffected);
    };
    collection.remove(replaceTypes(selector, replaceMeteorAtomWithMSSQL),
                       {safe: true}, wrappedCallback);
  } catch (err) {
    write.committed();
    throw err;
  }
};

MSSQLConnection.prototype._dropCollection = function (collectionName, cb) {
  var self = this;

  var write = self._maybeBeginWrite();
  var refresh = function () {
    Meteor.refresh({collection: collectionName, id: null,
                    dropCollection: true});
  };
  cb = bindEnvironmentForWrite(writeCallback(write, refresh, cb));

  try {
    var collection = self.rawCollection(collectionName);
    collection.drop(cb);
  } catch (e) {
    write.committed();
    throw e;
  }
};

// For testing only.  Slightly better than `c.rawDatabase().dropDatabase()`
// because it lets the test's fence wait for it to be complete.
MSSQLConnection.prototype._dropDatabase = function (cb) {
  var self = this;

  var write = self._maybeBeginWrite();
  var refresh = function () {
    Meteor.refresh({ dropDatabase: true });
  };
  cb = bindEnvironmentForWrite(writeCallback(write, refresh, cb));

  try {
    self.db.dropDatabase(cb);
  } catch (e) {
    write.committed();
    throw e;
  }
};

MSSQLConnection.prototype._update = function (collection_name, selector, mod,
                                              options, callback) {
  var self = this;

  if (! callback && options instanceof Function) {
    callback = options;
    options = null;
  }

  if (collection_name === "___meteor_failure_test_collection") {
    var e = new Error("Failure test");
    e.expected = true;
    if (callback) {
      return callback(e);
    } else {
      throw e;
    }
  }

  // explicit safety check. null and undefined can crash the mongo
  // driver. Although the node driver and minimongo do 'support'
  // non-object modifier in that they don't crash, they are not
  // meaningful operations and do not do anything. Defensively throw an
  // error here.
  if (!mod || typeof mod !== 'object')
    throw new Error("Invalid modifier. Modifier must be an object.");

  if (!(LocalCollection._isPlainObject(mod) &&
        !EJSON._isCustomType(mod))) {
    throw new Error(
      "Only plain objects may be used as replacement" +
        " documents in MSSQLDB");
  }

  if (!options) options = {};

  var write = self._maybeBeginWrite();
  var refresh = function () {
    self._refresh(collection_name, selector);
  };
  callback = writeCallback(write, refresh, callback);
  try {
    var collection = self.rawCollection(collection_name);
    var mongoOpts = {safe: true};
    // explictly enumerate options that minimongo supports
    if (options.upsert) mongoOpts.upsert = true;
    if (options.multi) mongoOpts.multi = true;
    // Lets you get a more more full result from MSSQLDB. Use with caution:
    // might not work with C.upsert (as opposed to C.update({upsert:true}) or
    // with simulated upsert.
    if (options.fullResult) mongoOpts.fullResult = true;

    var mongoSelector = replaceTypes(selector, replaceMeteorAtomWithMSSQL);
    var mongoMod = replaceTypes(mod, replaceMeteorAtomWithMSSQL);

    var isModify = isModificationMod(mongoMod);
    var knownId = selector._id || mod._id;

    if (options._forbidReplace && ! isModify) {
      var err = new Error("Invalid modifier. Replacements are forbidden.");
      if (callback) {
        return callback(err);
      } else {
        throw err;
      }
    }

    if (options.upsert && (! knownId) && options.insertedId) {
      // XXX If we know we're using MSSQL 2.6 (and this isn't a replacement)
      //     we should be able to just use $setOnInsert instead of this
      //     simulated upsert thing. (We can't use $setOnInsert with
      //     replacements because there's nowhere to write it, and $setOnInsert
      //     can't set _id on MSSQL 2.4.)
      //
      //     Also, in the future we could do a real upsert for the mongo id
      //     generation case, if the the node mongo driver gives us back the id
      //     of the upserted doc (which our current version does not).
      //
      //     For more context, see
      //     https://github.com/meteor/meteor/issues/2278#issuecomment-64252706
      simulateUpsertWithInsertedId(
        collection, mongoSelector, mongoMod,
        isModify, options,
        // This callback does not need to be bindEnvironment'ed because
        // simulateUpsertWithInsertedId() wraps it and then passes it through
        // bindEnvironmentForWrite.
        function (error, result) {
          // If we got here via a upsert() call, then options._returnObject will
          // be set and we should return the whole object. Otherwise, we should
          // just return the number of affected docs to match the mongo API.
          if (result && ! options._returnObject) {
            callback(error, result.numberAffected);
          } else {
            callback(error, result);
          }
        }
      );
    } else {
      collection.update(
        mongoSelector, mongoMod, mongoOpts,
        bindEnvironmentForWrite(function (err, result) {
          if (! err) {
            var meteorResult = transformResult(result);
            if (meteorResult && options._returnObject) {
              // If this was an upsert() call, and we ended up
              // inserting a new doc and we know its id, then
              // return that id as well.

              if (options.upsert && meteorResult.insertedId && knownId) {
                meteorResult.insertedId = knownId;
              }
              callback(err, meteorResult);
            } else {
              callback(err, meteorResult.numberAffected);
            }
          } else {
            callback(err);
          }
        }));
    }
  } catch (e) {
    write.committed();
    throw e;
  }
};

// XXX MSSQLConnection.upsert() does not return the id of the inserted document
// unless you set it explicitly in the selector or modifier (as a replacement
// doc).
MSSQLConnection.prototype.upsert = function (collectionName, selector, mod,
                                             options, callback) {
  var self = this;
  if (typeof options === "function" && ! callback) {
    callback = options;
    options = {};
  }

  return self.update(collectionName, selector, mod,
                     _.extend({}, options, {
                       upsert: true,
                       _returnObject: true
                     }), callback);
};

MSSQLConnection.prototype.find = function (collectionName, selector, options) {
  var self = this;

  if (arguments.length === 1)
    selector = {};

  return new Cursor(
    self, new CursorDescription(collectionName, selector, options));
};

MSSQLConnection.prototype.query = function (collectionName, selector, options) {
  var self = this;

  if (arguments.length === 1)
    selector = {};

  return new Cursor(
    self, new CursorDescription(collectionName, selector, options));
};

MSSQLConnection.prototype.findOne = function (collection_name, selector,
                                              options) {
  var self = this;
  if (arguments.length === 1)
    selector = {};

  options = options || {};
  options.limit = 1;
  return self.find(collection_name, selector, options).fetch()[0];
};

// We'll actually design an index API later. For now, we just pass through to
// MSSQL's, but make it synchronous.
MSSQLConnection.prototype._ensureIndex = function (collectionName, index,
                                                   options) {
  var self = this;

  // We expect this function to be called at startup, not from within a method,
  // so we don't interact with the write fence.
  var collection = self.rawCollection(collectionName);
  var future = new Future;
  var indexName = collection.ensureIndex(index, options, future.resolver());
  future.wait();
};
MSSQLConnection.prototype._dropIndex = function (collectionName, index) {
  var self = this;

  // This function is only used by test code, not within a method, so we don't
  // interact with the write fence.
  var collection = self.rawCollection(collectionName);
  var future = new Future;
  var indexName = collection.dropIndex(index, future.resolver());
  future.wait();
};

MSSQLConnection.prototype._createSynchronousCursor = function(
    cursorDescription, options) {
  var self = this;
  options = _.pick(options || {}, 'selfForIteration', 'useTransform');

  var collection = self.rawCollection(cursorDescription.collectionName);
  var cursorOptions = cursorDescription.options;
  var mongoOptions = {
    sort: cursorOptions.sort,
    limit: cursorOptions.limit,
    skip: cursorOptions.skip
  };

  // Do we want a tailable cursor (which only works on capped collections)?
  if (cursorOptions.tailable) {
    // We want a tailable cursor...
    mongoOptions.tailable = true;
    // ... and for the server to wait a bit if any getMore has no data (rather
    // than making us put the relevant sleeps in the client)...
    mongoOptions.awaitdata = true;
    // ... and to keep querying the server indefinitely rather than just 5 times
    // if there's no more data.
    mongoOptions.numberOfRetries = -1;
    // And if this is on the oplog collection and the cursor specifies a 'ts',
    // then set the undocumented oplog replay flag, which does a special scan to
    // find the first document (instead of creating an index on ts). This is a
    // very hard-coded MSSQL flag which only works on the oplog collection and
    // only works with the ts field.
    if (cursorDescription.collectionName === OPLOG_COLLECTION &&
        cursorDescription.selector.ts) {
      mongoOptions.oplogReplay = true;
    }
  }

  var dbCursor = collection.find(
    replaceTypes(cursorDescription.selector, replaceMeteorAtomWithMSSQL),
    cursorOptions.fields, mongoOptions);

  return new SynchronousCursor(dbCursor, cursorDescription, options);
};

MSSQLConnection.prototype.tail = function (cursorDescription, docCallback) {
  var self = this;
  if (!cursorDescription.options.tailable)
    throw new Error("Can only tail a tailable cursor");

  var cursor = self._createSynchronousCursor(cursorDescription);

  var stopped = false;
  var lastTS;
  var loop = function () {
    var doc = null;
    while (true) {
      if (stopped)
        return;
      try {
        doc = cursor._nextObject();
      } catch (err) {
        // There's no good way to figure out if this was actually an error
        // from MSSQL. Ah well. But either way, we need to retry the cursor
        // (unless the failure was because the observe got stopped).
        doc = null;
      }
      // Since cursor._nextObject can yield, we need to check again to see if
      // we've been stopped before calling the callback.
      if (stopped)
        return;
      if (doc) {
        // If a tailable cursor contains a "ts" field, use it to recreate the
        // cursor on error. ("ts" is a standard that MSSQL uses internally for
        // the oplog, and there's a special flag that lets you do binary search
        // on it instead of needing to use an index.)
        lastTS = doc.ts;
        docCallback(doc);
      } else {
        var newSelector = _.clone(cursorDescription.selector);
        if (lastTS) {
          newSelector.ts = {$gt: lastTS};
        }
        cursor = self._createSynchronousCursor(new CursorDescription(
          cursorDescription.collectionName,
          newSelector,
          cursorDescription.options));
        // MSSQL failover takes many seconds.  Retry in a bit.  (Without this
        // setTimeout, we peg the CPU at 100% and never notice the actual
        // failover.
        Meteor.setTimeout(loop, 100);
        break;
      }
    }
  };

  Meteor.defer(loop);

  return {
    stop: function () {
      stopped = true;
      cursor.close();
    }
  };
};

MSSQLConnection.prototype._observeChanges = function (
    cursorDescription, ordered, callbacks) {
  var self = this;

  if (cursorDescription.options.tailable) {
    return self._observeChangesTailable(cursorDescription, ordered, callbacks);
  }

  // You may not filter out _id when observing changes, because the id is a core
  // part of the observeChanges API.
  if (cursorDescription.options.fields &&
      (cursorDescription.options.fields._id === 0 ||
       cursorDescription.options.fields._id === false)) {
    throw Error("You may not observe a cursor with {fields: {_id: 0}}");
  }

  var observeKey = JSON.stringify(
    _.extend({ordered: ordered}, cursorDescription));

  var multiplexer, observeDriver;
  var firstHandle = false;

  // Find a matching ObserveMultiplexer, or create a new one. This next block is
  // guaranteed to not yield (and it doesn't call anything that can observe a
  // new query), so no other calls to this function can interleave with it.
  Meteor._noYieldsAllowed(function () {
    if (_.has(self._observeMultiplexers, observeKey)) {
      multiplexer = self._observeMultiplexers[observeKey];
    } else {
      firstHandle = true;
      // Create a new ObserveMultiplexer.
      multiplexer = new ObserveMultiplexer({
        ordered: ordered,
        onStop: function () {
          delete self._observeMultiplexers[observeKey];
          observeDriver.stop();
        }
      });
      self._observeMultiplexers[observeKey] = multiplexer;
    }
  });

  var observeHandle = new ObserveHandle(multiplexer, callbacks);

  if (firstHandle) {
    var matcher, sorter;
    var canUseOplog = _.all([
      function () {
        // At a bare minimum, using the oplog requires us to have an oplog, to
        // want unordered callbacks, and to not want a callback on the polls
        // that won't happen.
        return self._oplogHandle && !ordered &&
          !callbacks._testOnlyPollCallback;
      }, function () {
        // We need to be able to compile the selector. Fall back to polling for
        // some newfangled $selector that minimongo doesn't support yet.
        try {
          matcher = new Minimongo.Matcher(cursorDescription.selector);
          return true;
        } catch (e) {
          // XXX make all compilation errors MinimongoError or something
          //     so that this doesn't ignore unrelated exceptions
          return false;
        }
      }, function () {
        // ... and the selector itself needs to support oplog.
        return OplogObserveDriver.cursorSupported(cursorDescription, matcher);
      }, function () {
        // And we need to be able to compile the sort, if any.  eg, can't be
        // {$natural: 1}.
        if (!cursorDescription.options.sort)
          return true;
        try {
          sorter = new Minimongo.Sorter(cursorDescription.options.sort,
                                        { matcher: matcher });
          return true;
        } catch (e) {
          // XXX make all compilation errors MinimongoError or something
          //     so that this doesn't ignore unrelated exceptions
          return false;
        }
      }], function (f) { return f(); });  // invoke each function

    var driverClass = canUseOplog ? OplogObserveDriver : PollingObserveDriver;
    observeDriver = new driverClass({
      cursorDescription: cursorDescription,
      mongoHandle: self,
      multiplexer: multiplexer,
      ordered: ordered,
      matcher: matcher,  // ignored by polling
      sorter: sorter,  // ignored by polling
      _testOnlyPollCallback: callbacks._testOnlyPollCallback
    });

    // This field is only set for use in tests.
    multiplexer._observeDriver = observeDriver;
  }

  // Blocks until the initial adds have been sent.
  multiplexer.addHandleAndSendInitialAdds(observeHandle);

  return observeHandle;
};

// observeChanges for tailable cursors on capped collections.
//
// Some differences from normal cursors:
//   - Will never produce anything other than 'added' or 'addedBefore'. If you
//     do update a document that has already been produced, this will not notice
//     it.
//   - If you disconnect and reconnect from MSSQL, it will essentially restart
//     the query, which will lead to duplicate results. This is pretty bad,
//     but if you include a field called 'ts' which is inserted as
//     new MSSQLInternals.MSSQLTimestamp(0, 0) (which is initialized to the
//     current MSSQL-style timestamp), we'll be able to find the place to
//     restart properly. (This field is specifically understood by MSSQL with an
//     optimization which allows it to find the right place to start without
//     an index on ts. It's how the oplog works.)
//   - No callbacks are triggered synchronously with the call (there's no
//     differentiation between "initial data" and "later changes"; everything
//     that matches the query gets sent asynchronously).
//   - De-duplication is not implemented.
//   - Does not yet interact with the write fence. Probably, this should work by
//     ignoring removes (which don't work on capped collections) and updates
//     (which don't affect tailable cursors), and just keeping track of the ID
//     of the inserted object, and closing the write fence once you get to that
//     ID (or timestamp?).  This doesn't work well if the document doesn't match
//     the query, though.  On the other hand, the write fence can close
//     immediately if it does not match the query. So if we trust minimongo
//     enough to accurately evaluate the query against the write fence, we
//     should be able to do this...  Of course, minimongo doesn't even support
//     MSSQL Timestamps yet.
MSSQLConnection.prototype._observeChangesTailable = function (
    cursorDescription, ordered, callbacks) {
  var self = this;

  // Tailable cursors only ever call added/addedBefore callbacks, so it's an
  // error if you didn't provide them.
  if ((ordered && !callbacks.addedBefore) ||
      (!ordered && !callbacks.added)) {
    throw new Error("Can't observe an " + (ordered ? "ordered" : "unordered")
                    + " tailable cursor without a "
                    + (ordered ? "addedBefore" : "added") + " callback");
  }

  return self.tail(cursorDescription, function (doc) {
    var id = doc._id;
    delete doc._id;
    // The ts is an implementation detail. Hide it.
    delete doc.ts;
    if (ordered) {
      callbacks.addedBefore(id, doc, null);
    } else {
      callbacks.added(id, doc);
    }
  });
};

_.each(["insert", "update", "remove", "dropCollection", "dropDatabase"], function (method) {
  MSSQLConnection.prototype[method] = function (/* arguments */) {
    var self = this;
    return Meteor.wrapAsync(self["_" + method]).apply(self, arguments);
  };
});

// exposed for testing
MSSQLConnection._isCannotChangeIdError = function (err) {
  // First check for what this error looked like in MSSQL 2.4.  Either of these
  // checks should work, but just to be safe...
  if (err.code === 13596) {
    return true;
  }

  // MSSQL 3.2.* returns error as next Object:
  // {name: String, code: Number, err: String}
  // Older MSSQL returns:
  // {name: String, code: Number, errmsg: String}
  var error = err.errmsg || err.err;

  if (error.indexOf('cannot change _id of a document') === 0) {
    return true;
  }

  // Now look for what it looks like in MSSQL 2.6.  We don't use the error code
  // here, because the error code we observed it producing (16837) appears to be
  // a far more generic error code based on examining the source.
  if (error.indexOf('The _id field cannot be changed') === 0) {
    return true;
  }

  return false;
};

var replaceMeteorAtomWithMSSQL = function (document) {
  if (EJSON.isBinary(document)) {
    // This does more copies than we'd like, but is necessary because
    // MSSQLDB.BSON only looks like it takes a Uint8Array (and doesn't actually
    // serialize it correctly).
    return new MSSQLDB.Binary(new Buffer(document));
  }
  if (document instanceof Mongo.ObjectID) {
    return new MSSQLDB.ObjectID(document.toHexString());
  }
  if (document instanceof MSSQLDB.Timestamp) {
    // For now, the Meteor representation of a MSSQL timestamp type (not a date!
    // this is a weird internal thing used in the oplog!) is the same as the
    // MSSQL representation. We need to do this explicitly or else we would do a
    // structural clone and lose the prototype.
    return document;
  }
  if (EJSON._isCustomType(document)) {
    return replaceNames(makeMSSQLLegal, EJSON.toJSONValue(document));
  }
  // It is not ordinarily possible to stick dollar-sign keys into mongo
  // so we don't bother checking for things that need escaping at this time.
  return undefined;
};

// The write methods block until the database has confirmed the write (it may
// not be replicated or stable on disk, but one server has confirmed it) if no
// callback is provided. If a callback is provided, then they call the callback
// when the write is confirmed. They return nothing on success, and raise an
// exception on failure.
//
// After making a write (with insert, update, remove), observers are
// notified asynchronously. If you want to receive a callback once all
// of the observer notifications have landed for your write, do the
// writes inside a write fence (set DDPServer._CurrentWriteFence to a new
// _WriteFence, and then set a callback on the write fence.)
//
// Since our execution environment is single-threaded, this is
// well-defined -- a write "has been made" if it's returned, and an
// observer "has been notified" if its callback has returned.

var writeCallback = function (write, refresh, callback) {
  return function (err, result) {
    if (! err) {
      // XXX We don't have to run this on error, right?
      try {
        refresh();
      } catch (refreshErr) {
        if (callback) {
          callback(refreshErr);
          return;
        } else {
          throw refreshErr;
        }
      }
    }
    write.committed();
    if (callback) {
      callback(err, result);
    } else if (err) {
      throw err;
    }
  };
};

var bindEnvironmentForWrite = function (callback) {
  return Meteor.bindEnvironment(callback, "MSSQL write");
};

var isModificationMod = function (mod) {
  var isReplace = false;
  var isModify = false;
  for (var k in mod) {
    if (k.substr(0, 1) === '$') {
      isModify = true;
    } else {
      isReplace = true;
    }
  }
  if (isModify && isReplace) {
    throw new Error(
      "Update parameter cannot have both modifier and non-modifier fields.");
  }
  return isModify;
};

var transformResult = function (driverResult) {
  var meteorResult = { numberAffected: 0 };
  if (driverResult) {
    var mongoResult = driverResult.result;

    // On updates with upsert:true, the inserted values come as a list of
    // upserted values -- even with options.multi, when the upsert does insert,
    // it only inserts one element.
    if (mongoResult.upserted) {
      meteorResult.numberAffected += mongoResult.upserted.length;

      if (mongoResult.upserted.length == 1) {
        meteorResult.insertedId = mongoResult.upserted[0]._id;
      }
    } else {
      meteorResult.numberAffected = mongoResult.n;
    }
  }

  return meteorResult;
};

var NUM_OPTIMISTIC_TRIES = 3;


var simulateUpsertWithInsertedId = function (collection, selector, mod,
                                             isModify, options, callback) {
  // STRATEGY:  First try doing a plain update.  If it affected 0 documents,
  // then without affecting the database, we know we should probably do an
  // insert.  We then do a *conditional* insert that will fail in the case
  // of a race condition.  This conditional insert is actually an
  // upsert-replace with an _id, which will never successfully update an
  // existing document.  If this upsert fails with an error saying it
  // couldn't change an existing _id, then we know an intervening write has
  // caused the query to match something.  We go back to step one and repeat.
  // Like all "optimistic write" schemes, we rely on the fact that it's
  // unlikely our writes will continue to be interfered with under normal
  // circumstances (though sufficiently heavy contention with writers
  // disagreeing on the existence of an object will cause writes to fail
  // in theory).

  var newDoc;
  // Run this code up front so that it fails fast if someone uses
  // a MSSQL update operator we don't support.
  if (isModify) {
    // We've already run replaceTypes/replaceMeteorAtomWithMSSQL on
    // selector and mod.  We assume it doesn't matter, as far as
    // the behavior of modifiers is concerned, whether `_modify`
    // is run on EJSON or on mongo-converted EJSON.
    var selectorDoc = LocalCollection._removeDollarOperators(selector);

    newDoc = selectorDoc;

    // Convert dotted keys into objects. (Resolves issue #4522).
    _.each(newDoc, function (value, key) {
      var trail = key.split(".");

      if (trail.length > 1) {
        //Key is dotted. Convert it into an object.
        delete newDoc[key];

        var obj = newDoc,
            leaf = trail.pop();

        // XXX It is not quite certain what should be done if there are clashing
        // keys on the trail of the dotted key. For now we will just override it
        // It wouldn't be a very sane query in the first place, but should look
        // up what mongo does in this case.

        while ((key = trail.shift())) {
          if (typeof obj[key] !== "object") {
            obj[key] = {};
          }

          obj = obj[key];
        }

        obj[leaf] = value;
      }
    });

    LocalCollection._modify(newDoc, mod, {isInsert: true});
  } else {
    newDoc = mod;
  }

  var insertedId = options.insertedId; // must exist
  var mongoOptsForUpdate = {
    safe: true,
    multi: options.multi
  };
  var mongoOptsForInsert = {
    safe: true,
    upsert: true
  };

  var tries = NUM_OPTIMISTIC_TRIES;

  var doUpdate = function () {
    tries--;
    if (! tries) {
      callback(new Error("Upsert failed after " + NUM_OPTIMISTIC_TRIES + " tries."));
    } else {
      collection.update(selector, mod, mongoOptsForUpdate,
                        bindEnvironmentForWrite(function (err, result) {
                          if (err) {
                            callback(err);
                          } else if (result && result.result.n != 0) {
                            callback(null, {
                              numberAffected: result.result.n
                            });
                          } else {
                            doConditionalInsert();
                          }
                        }));
    }
  };

  var doConditionalInsert = function () {
    var replacementWithId = _.extend(
      replaceTypes({_id: insertedId}, replaceMeteorAtomWithMSSQL),
      newDoc);
    collection.update(selector, replacementWithId, mongoOptsForInsert,
                      bindEnvironmentForWrite(function (err, result) {
                        if (err) {
                          // figure out if this is a
                          // "cannot change _id of document" error, and
                          // if so, try doUpdate() again, up to 3 times.
                          if (MSSQLConnection._isCannotChangeIdError(err)) {
                            doUpdate();
                          } else {
                            callback(err);
                          }
                        } else {
                          callback(null, {
                            numberAffected: result.result.upserted.length,
                            insertedId: insertedId,
                          });
                        }
                      }));
  };

  doUpdate();
};

export let MSSQLConnection = MSSQLConnection;
