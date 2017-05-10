CollectionHelper = {};

CollectionHelper._publishCursor = function (cursor, sub, collection) {
  var observeHandle = cursor.observeChanges({
    added: function (id, fields) {
      sub.added(collection, id, fields);
    },
    changed: function (id, fields) {
      sub.changed(collection, id, fields);
    },
    removed: function (id) {
      sub.removed(collection, id);
    }
  });

  // We don't call sub.ready() here: it gets called in livedata_server, after
  // possibly calling _publishCursor on multiple returned cursors.

  // register stop callback (expects lambda w/ no args).
  sub.onStop(function () {observeHandle.stop();});

  // return the observeHandle in case it needs to be stopped early
  return observeHandle;
};

// protect against dangerous selectors.  falsey and {_id: falsey} are both
// likely programmer error, and not what you want, particularly for destructive
// operations.  JS regexps don't serialize over DDP but can be trivially
// replaced by $regex. If a falsey _id is sent in, a new string _id will be
// generated and returned; if a fallbackId is provided, it will be returned
// instead.
CollectionHelper._rewriteSelector = (selector, { fallbackId } = {}) => {
  // shorthand -- scalars match _id
  if (typeof selector != 'string' && LocalCollection._selectorIsId(selector))
    selector = {_id: selector};
  else
    if(typeof selector == 'string' && selector.length > 0) // Queries will be specified as strings
        return selector;

  if (_.isArray(selector)) {
    // This is consistent with the MSSQL console itself; if we don't do this
    // check passing an empty array ends up selecting all items
    throw new Error("MSSQL selector can't be an array.");
  }

  if (!selector || (('_id' in selector) && !selector._id)) {
    // can't match anything
    return { _id: fallbackId || Random.id() };
  }

      var ret = {};

      Object.keys(selector).forEach((key) => {
        const value = selector[key];
        // MSSQL supports both {field: /foo/} and {field: {$regex: /foo/}}
        if (value instanceof RegExp) {
          ret[key] = convertRegexpToMSSQLSelector(value);
        } else if (value && value.$regex instanceof RegExp) {
          ret[key] = convertRegexpToMSSQLSelector(value.$regex);
          // if value is {$regex: /foo/, $options: ...} then $options
          // override the ones set on $regex.
          if (value.$options !== undefined)
            ret[key].$options = value.$options;
        } else if (_.contains(['$or','$and','$nor'], key)) {
          // Translate lower levels of $and/$or/$nor
          ret[key] = _.map(value, function (v) {
            return CollectionHelper._rewriteSelector(v);
          });
        } else {
          ret[key] = value;
        }
      });

      return ret;
};

// convert a JS RegExp object to a MSSQL {$regex: ..., $options: ...}
// selector
function convertRegexpToMSSQLSelector(regexp) {
  check(regexp, RegExp); // safety belt

  var selector = {$regex: regexp.source};
  var regexOptions = '';
  // JS RegExp objects support 'i', 'm', and 'g'. MSSQL regex $options
  // support 'i', 'm', 'x', and 's'. So we support 'i' and 'm' here.
  if (regexp.ignoreCase)
    regexOptions += 'i';
  if (regexp.multiline)
    regexOptions += 'm';
  if (regexOptions)
    selector.$options = regexOptions;

  return selector;
}

export let CollectionHelper = CollectionHelper;
