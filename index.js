var through = require('through2')
var pumpify = require('pumpify')
var once = require('once')
var lexint = require('lexicographic-integer')
var mutex = require('level-mutex')
var byteStream = require('byte-stream')
var debug = require('debug')('level-dat')
var util = require('util')
var events = require('events')

var noop = function() {}

var TYPES = ['update', 'delete', 'create']

var WRITE_BUFFER_SIZE = 1024 * 1024 * 16
var SEP = '\xff'
var PREFIX = {
  change: 's',
  data: 'd',
  version:  'r',
  cur: 'c',
  meta: 'm'
}

var PREFIX_CHANGE = SEP+PREFIX.change+SEP
var PREFIX_DATA = SEP+PREFIX.data+SEP
var PREFIX_VERSION = SEP+PREFIX.version+SEP
var PREFIX_CUR = SEP+PREFIX.cur+SEP
var PREFIX_META = SEP+PREFIX.meta+SEP

var conflict = function(key, version) {
  var err =  new Error('Key conflict. A row with that key already exists and/or has a newer version.')
  err.key = key
  err.version = version
  err.status = 409
  err.conflict = true
  return err
}

var waiter = function(missing, cb) {
  var done = false
  return function(err) {
    if (done) return
    if (err) {
      done = true
      return cb(err)
    }
    if (--missing) return
    done = true
    cb()
  }
}

var deleted = function(cur) {
  return cur.length > 2 && cur[cur.length-2] === SEP && cur[cur.length-1] === '1'
}

var pack = function(n) {
  return lexint.pack(n, 'hex')
}

var unpack = function(n) {
  return lexint.unpack(n, 'hex')
}

var LevelDat = function(db, opts, onready) {
  if (!(this instanceof LevelDat)) return new LevelDat(db, opts, onready)
  if (typeof opts === 'function') return new LevelDat(db, null, onready)
  if (!opts) opts = {}

  var self = this

  this.db = db
  this.mutex = mutex(db)
  this.change = opts.change || -1
  this.defaults = opts

  this.mutex.peekLast({last:PREFIX_CHANGE+SEP}, function(err, key, value) {
    if (err && err.message !== 'range not found') return self.emit('error', err)

    self.change = value ? JSON.parse(value)[0] : 0
    debug('head change: %d', self.change)
    self.emit('ready', self)
  })

  if (onready) this.on('ready', onready)
}

util.inherits(LevelDat, events.EventEmitter)

LevelDat.prototype.createVersionStream = function(key, opts) {
  opts = this._mixin(opts)
  opts.start = PREFIX_DATA+key+SEP
  opts.end = PREFIX_DATA+key+SEP+SEP

  var stream = through.obj(function(data, enc, cb) {
    var vidx = data.key.lastIndexOf(SEP)

    data = {
      key: data.key.slice(PREFIX_DATA.length, vidx),
      version: unpack(data.key.slice(vidx+1)),
      value: data.value
    }

    debug('get version (key: %s, version: %d)', data.key, data.version)
    cb(null, data)
  })

  var rs = this.db.createReadStream(opts)

  return pumpify.obj(rs, stream)
}

LevelDat.prototype.valueStream =
LevelDat.prototype.createValueStream = function(opts) {
  opts = this._mixin(opts)
  opts.keys = false
  opts.values = true
  return this.createReadStream(opts)
}

LevelDat.prototype.keyStream =
LevelDat.prototype.createKeyStream = function(opts) {
  opts = this._mixin(opts)
  opts.keys = true
  opts.values = false
  return this.createReadStream(opts)
}

LevelDat.prototype.readStream =
LevelDat.prototype.createReadStream = function(opts) {
  opts = this._mixin(opts)

  var self = this
  var keys = opts.keys !== false
  var values = opts.values !== false

  var rs = self.db.createReadStream({
    start: PREFIX_CUR+(opts.start || ''),
    end: PREFIX_CUR+(opts.end || SEP)
  })

  var get = through.obj(function(data, enc, cb) {
    var val = data.value
    var key = data.key.slice(PREFIX_CUR.length)

    if (deleted(val)) return cb()

    var version = unpack(val)
    self.mutex.get(PREFIX_DATA+key+SEP+val, opts, function(err, data) {
      if (err) return cb(err)
      debug('get data.%s (version: %d)', key, version)

      if (values && !keys) return cb(null, data)
      if (keys && !values) return cb(null, key)

      cb(null, {
        key: key,
        version: version,
        value: data
      })
    })
  })

  return pumpify.obj(rs, get)
}

LevelDat.prototype.writeStream =
LevelDat.prototype.createWriteStream = function(opts) {
  this._assert()
  opts = this._mixin(opts)

  var self = this

  var buffer = byteStream({
    limit: WRITE_BUFFER_SIZE,
    time: 3000
  })

  var format = through.obj(function(data, enc, cb) {
    data.length = data.value ? data.value.length || 1 : 1
    cb(null, data)
  })

  var ws = through.obj(function(batch, enc, cb) {
    self.batch(batch, cb)
  })

  return pumpify.obj(format, buffer, ws)
}

LevelDat.prototype.createChangesWriteStream = function(opts) {
  this._assert()
  opts = this._mixin(opts)

  var self = this

  var buffer = byteStream({
    limit: WRITE_BUFFER_SIZE,
    time: 3000
  })

  var format = through.obj(function(data, enc, cb) {
    if (!data.value && data.type !== 'delete') return cb(new Error('data.value is required'))
    data.length = data.value ? data.value.length || 1 : 1
    cb(null, data)
  })

  var ws = through.obj(function(batch, enc, cb) {
    var wait = waiter(batch.length, cb)

    for (var i = 0; i < batch.length; i++) {
      var b = batch[i]
      var v = pack(b.version)

      debug('put change (change: %d, key: %s, version: %d, type: %s)', b.change, b.key, b.version, b.type)

      if (b.type === 'delete') {
        self.mutex.put(PREFIX_CHANGE+pack(b.change), JSON.stringify([b.change, b.key, b.version, 'delete']), noop)
        self.mutex.put(PREFIX_CUR+b.key, v+SEP+'1', wait)
      } else {
        self.mutex.put(PREFIX_CHANGE+pack(b.change), JSON.stringify([b.change, b.key, b.version, b.type]), noop)
        self.mutex.put(PREFIX_CUR+b.key, v, noop)
        self.mutex.put(PREFIX_DATA+b.key+SEP+v, b.value, opts, wait)
      }
    }
  })

  return pumpify.obj(format, buffer, ws)
}

LevelDat.prototype.createChangesReadStream = function(opts) {
  opts = this._mixin(opts)

  var self = this
  var addData = !!opts.data

  var rs = this.db.createReadStream({
    start: PREFIX_CHANGE+pack(opts.change || 0),
    end: PREFIX_CHANGE+SEP
  })

  var format = through.obj(function(data, enc, cb) {
    var value = JSON.parse(data.value)

    var data = {
      key: value[1],
      version: value[2],
      change: value[0],
      type: value[3]
    }

    debug('get change (change: %d, key: %s, version: %d, type: %s, data: %s)', data.change, data.key, data.version, data.type, addData)
    if (!addData || data.type === 'delete') return cb(null, data)

    self._get(data.key, opts, data.version, function(err, value) {
      if (err) return cb(err)
      data.value = value
      cb(null, data)
    })
  })

  return pumpify.obj(rs, format)
}

LevelDat.prototype.get = function(key, opts, cb, version) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  opts = this._mixin(opts)

  if (!opts.version) this._getLatest(key, opts, cb)
  else this._get(key, opts, opts.version, cb)
}

LevelDat.prototype._getLatest = function(key, opts, cb) {
  opts = this._mixin(opts)

  var self = this
  this.mutex.get(PREFIX_CUR+key, function(err, cur) {
    if (err) return cb(err)
    if (deleted(cur)) return cb(new Error('Key was deleted'))

    self._get(key, opts, unpack(cur), cb)
  })
}

LevelDat.prototype._get = function(key, opts, version, cb) {
  opts = this._mixin(opts)
  version = +version

  this.mutex.get(PREFIX_DATA+key+SEP+pack(version), opts, function(err, val) {
    if (err) return cb(err)

    debug('get data.%s (version: %d)', key, version)
    cb(null, val, version)
  })
}

LevelDat.prototype.put = function(key, value, opts, cb) {
  this._assert()
  if (typeof opts === 'function') return this.put(key, value, null, opts)
  if (!cb) cb = noop
  opts = this._mixin(opts)

  this._put(key, value, opts, opts.version, cb)
}

LevelDat.prototype.batch = function(batch, opts, cb) {
  if (typeof opts === 'function') return this.batch(batch, null, opts)
  if (!cb) cb = noop
  opts = this._mixin(opts)

  if (!batch.length) return cb()
  var wait = waiter(batch.length, cb)

  for (var i = 0; i < batch.length; i++) {
    var b = batch[i]
    if (b.type === 'delete') this.del(b.key, wait)
    else this._put(b.key, b.value, opts, b.version || 0, wait)
  }
}

LevelDat.prototype._put = function(key, value, opts, version, cb) {
  this._assert()
  if (!version) version = 1
  opts = this._mixin(opts)

  var self = this

  this.mutex.get(PREFIX_CUR+key, function(_, curV) {
    if (curV) curV = unpack(curV)
    if (curV) debug('put data.%s existing version exist (old-version: %d, new-version: %d)', key, curV, version)

    if (version < curV && !opts.force) return cb(conflict(key, version))
    if (version === curV) version++

    var v = pack(+version)
    var change = ++self.change

    debug('put data.%s (version: %d)', key, version)
    self.mutex.put(PREFIX_CHANGE+pack(change), JSON.stringify([change, key, version, curV ? 'update' : 'create']), noop)
    self.mutex.put(PREFIX_CUR+key, v, noop)
    self.mutex.put(PREFIX_DATA+key+SEP+v, value, opts, cb)
  })
}

LevelDat.prototype.del =
LevelDat.prototype.delete = function(key, cb) {
  this._assert()
  if (!cb) cb = noop

  var self = this

  this.mutex.get(PREFIX_CUR+key, function(err, v) {
    if (err) return cb(err)

    var change = ++self.change
    var version = unpack(v)

    debug('del data.%s', key)
    self.mutex.put(PREFIX_CHANGE+pack(change), JSON.stringify([change, key, version, 'delete']), noop)
    self.mutex.put(PREFIX_CUR+key, v+SEP+'1', cb)
  })
}

LevelDat.prototype._mixin = function(opts) {
  if (!opts) opts = {}
  if (!opts.valueEncoding) opts.valueEncoding = this.defaults.valueEncoding
  if (this.defaults.force && opts.force === undefined) opts.force = true
  return opts
}

LevelDat.prototype._assert = function() {
  if (this.change === -1) throw new Error('Database is not ready. Wait for the ready event.')
}

LevelDat.prototype.putMeta = function(key, value, opts, cb) {
  if (typeof opts === 'function') return this.putMeta(key, value, null, opts)
  if (!cb) cb = noop
  opts = this._mixin(opts)

  this.mutex.put(PREFIX_META+key, value, opts, cb)
}

LevelDat.prototype.getMeta = function(key, opts, cb) {
  if (typeof opts === 'function') return this.getMeta(key, null, cb)
  opts = this._mixin(opts)

  var self = this
  this.mutex.afterWrite(function() {
    debug('get meta.%s', key)
    self.mutex.get(PREFIX_META+key, opts, function(err, val) {
      if (err) return cb(err)
      cb(null, val)
    })
  })
}

LevelDat.prototype.count = function(cb) {
  var self = this

  cb = once(cb)
  this.getMeta('_count', {valueEncoding:'json'}, function(err, result) {
    if (!result) result = {count:0, change:0}
    if (result.change === self.change) return cb(null, result.count)

    var changes = self.createChangesReadStream({
      change: result.change
    })

    var persist = function(cb) {
      debug('put meta._count (change: %d, count: %d)', result.change, result.count)
      self.putMeta('_count', result, {valueEncoding:'json'}, function(err) {
        if (err) return cb(err)
        cb(null)
      })
    }

    var inc = 0
    var ondata = function(data, enc, cb) {
      result.change = data.change
      if (data.type === 'create') result.count++
      else if (data.type === 'delete') result.count--
      if (++inc % 5000) cb()
      else persist(cb)
    }

    changes.on('error', cb)
    changes.pipe(through.obj(ondata)).on('finish', function() {
      persist(function() {
        cb(null, result)
      })
    })
  })
}

module.exports = LevelDat