import Future from 'fibers/future';
import { replaceTypes, replaceMSSQLAtomWithMeteor } from './DocumentHelper';

export class SynchronousCursor
{
    constructor(dbCursor, cursorDescription, options) {
      var self = this;
      options = _.pick(options || {}, 'selfForIteration', 'useTransform');

      self._dbCursor = dbCursor;
      self._cursorDescription = cursorDescription;
      // The "self" argument passed to forEach/map callbacks. If we're wrapped
      // inside a user-visible Cursor, we want to provide the outer cursor!
      self._selfForIteration = options.selfForIteration || self;
      if (options.useTransform && cursorDescription.options.transform) {
        self._transform = LocalCollection.wrapTransform(
          cursorDescription.options.transform);
      } else {
        self._transform = null;
      }

      // Need to specify that the callback is the first argument to nextObject,
      // since otherwise when we try to call it with no args the driver will
      // interpret "undefined" first arg as an options hash and crash.
      self._synchronousNextObject = Future.wrap(dbCursor.nextObject.bind(dbCursor), 0);
      self._synchronousCount = Future.wrap(dbCursor.count.bind(dbCursor));
      self._visitedIds = new LocalCollection._IdMap;
    }
}

_.extend(SynchronousCursor.prototype, {
  _nextObject: function () {
    var self = this;

    while (true) {
      var doc = self._synchronousNextObject().wait();

      if (!doc) return null;
      doc = replaceTypes(doc, replaceMSSQLAtomWithMeteor);

      if (!self._cursorDescription.options.tailable && _.has(doc, '_id')) {
        // Did MSSQL give us duplicate documents in the same cursor? If so,
        // ignore this one. (Do this before the transform, since transform might
        // return some unrelated value.) We don't do this for tailable cursors,
        // because we want to maintain O(1) memory usage. And if there isn't _id
        // for some reason (maybe it's the oplog), then we don't do this either.
        // (Be careful to do this for falsey but existing _id, though.)
        if (self._visitedIds.has(doc._id)) continue;
        self._visitedIds.set(doc._id, true);
      }

      if (self._transform)
        doc = self._transform(doc);

      return doc;
    }
  },

  forEach: function (callback, thisArg) {
    var self = this;

    // Get back to the beginning.
    self._rewind();

    // We implement the loop ourself instead of using self._dbCursor.each,
    // because "each" will call its callback outside of a fiber which makes it
    // much more complex to make this function synchronous.
    var index = 0;
    while (true) {
      var doc = self._nextObject();
      if (!doc) return;
      callback.call(thisArg, doc, index++, self._selfForIteration);
    }
  },

  // XXX Allow overlapping callback executions if callback yields.
  map: function (callback, thisArg) {
    var self = this;
    var res = [];
    self.forEach(function (doc, index) {
      res.push(callback.call(thisArg, doc, index, self._selfForIteration));
    });
    return res;
  },

  _rewind: function () {
    var self = this;

    // known to be synchronous
    self._dbCursor.rewind();

    self._visitedIds = new LocalCollection._IdMap;
  },

  // Mostly usable for tailable cursors.
  close: function () {
    var self = this;

    self._dbCursor.close();
  },

  fetch: function () {
    var self = this;
    return self.map(_.identity);
  },

  count: function (applySkipLimit = false) {
    var self = this;
    return self._synchronousCount(applySkipLimit).wait();
  },

  // This method is NOT wrapped in Cursor.
  getRawObjects: function (ordered) {
    var self = this;
    if (ordered) {
      return self.fetch();
    } else {
      var results = new LocalCollection._IdMap;
      self.forEach(function (doc) {
        results.set(doc._id, doc);
      });
      return results;
    }
  }
});
