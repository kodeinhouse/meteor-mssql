import { CollectionHelper } from './CollectionHelper';

export class Cursor
{
    /**
     * @param {mssql} MSSQLConnection
     */
    constructor(mssql, cursorDescription) {
      var self = this;

      self._mssql = mssql;
      self._cursorDescription = cursorDescription;
      self._synchronousCursor = null;
    }
}

_.each(['forEach', 'map', 'fetch', 'count'], function (method) {
  Cursor.prototype[method] = function () {
    var self = this;

    // You can only observe a tailable cursor.
    if (self._cursorDescription.options.tailable)
      throw new Error("Cannot call " + method + " on a tailable cursor");

    if (!self._synchronousCursor) {
      self._synchronousCursor = self._mssql._createSynchronousCursor(
        self._cursorDescription, {
          // Make sure that the "self" argument to forEach/map callbacks is the
          // Cursor, not the SynchronousCursor.
          selfForIteration: self,
          useTransform: true
        });
    }

    return self._synchronousCursor[method].apply(
      self._synchronousCursor, arguments);
  };
});

// Since we don't actually have a "nextObject" interface, there's really no
// reason to have a "rewind" interface.  All it did was make multiple calls
// to fetch/map/forEach return nothing the second time.
// XXX COMPAT WITH 0.8.1
Cursor.prototype.rewind = function () {
};

Cursor.prototype.getTransform = function () {
  return this._cursorDescription.options.transform;
};

// When you call Meteor.publish() with a function that returns a Cursor, we need
// to transmute it into the equivalent subscription.  This is the function that
// does that.

Cursor.prototype._publishCursor = function (sub) {
  var self = this;
  var collection = self._cursorDescription.collectionName;
  return CollectionHelper._publishCursor(self, sub, collection);
};

// Used to guarantee that publish functions return at most one cursor per
// collection. Private, because we might later have cursors that include
// documents from multiple collections somehow.
Cursor.prototype._getCollectionName = function () {
  var self = this;
  return self._cursorDescription.collectionName;
};

Cursor.prototype.observe = function (callbacks) {
  var self = this;
  return LocalCollection._observeFromObserveChanges(self, callbacks);
};

Cursor.prototype.observeChanges = function (callbacks) {
  var self = this;
  var ordered = LocalCollection._observeChangesCallbacksAreOrdered(callbacks);
  return self._mssql._observeChanges(
    self._cursorDescription, ordered, callbacks);
};
