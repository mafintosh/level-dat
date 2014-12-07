var through = require('through2')
var pumpify = require('pumpify')
var pump = require('pump')
var once = require('once')
var lexint = require('lexicographic-integer')
var mutex = require('level-mutex')
var byteStream = require('byte-stream')
var multistream = require('multistream')
var duplexify = require('duplexify')
var from = require('from2')
var debug = require('debug')('level-dat')
var util = require('util')
var events = require('events')
var subset = require('./subset')
var stream = require('stream')
var mutexify = require('mutexify')

var noop = function() {}

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

var changeConflict = function() {
  var err = new Error('Change conflict. Your local dat has a different change feed than your remote dat.')
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

var fixRange = function(opts) {
  if (!opts.reverse) return opts

  if (opts.start < opts.end) {
    var tmp = opts.start
    opts.start = opts.end
    opts.end = tmp
  }

  return opts
}

var deleted = function(cur) {
  return cur.length > 2 && cur[cur.length-2] === SEP && cur[cur.length-1] === '1'
}

var pack = function(n) {
  if (typeof n !== 'number') throw new Error('pack(n) must be called with a number')
  return lexint.pack(n, 'hex')
}

var unpack = function(n) {
  return lexint.unpack(n, 'hex')
}

var LevelDat = function(db, opts, onready) {
  if (!(this instanceof LevelDat)) return new LevelDat(db, opts, onready)
  if (typeof opts === 'function') return new LevelDat(db, null, opts)
  if (!opts) opts = {}

  var self = this

  this.corked = 1
  this.waiting = []

  this.onchange = []
  this.onchangewrite = function() {
    self.changeFlushed = self.change
    for (var i = 0; i < self.onchange.length; i++) self.onchange[i]()
  }

  this.db = db
  this.mutex = mutex(db)
  this.change = opts.change || -1
  this.changeFlushed = this.change
  this.defaults = opts
  this.lock = mutexify()

  if (onready) this.on('ready', onready)

  this.mutex.peekLast({last:PREFIX_CHANGE+SEP}, function(err, key, value) {
    if (err && err.message !== 'range not found') return self.emit('error', err)

    self.change = value ? JSON.parse(value)[0] : 0
    self.changeFlushed = self.change
    debug('head change: %d', self.change)
    self.emit('ready', self)
    self.uncork()
  })
}

util.inherits(LevelDat, events.EventEmitter)

LevelDat.prototype.methods = LevelDat.methods = { // for multilevel
  createChangesReadStream: {type: 'readable'},
  createChangesWriteStream: {type: 'writable'},
  createVersionStream: {type: 'readable'},
  stat: {type: 'async'},
  approximateSize: {type: 'async'}
}

LevelDat.prototype.subset = function(name) {
  return subset(this, name)
}

LevelDat.prototype.cork = function() {
  this.corked++
}

LevelDat.prototype.uncork = function() {
  if (this.corked) this.corked--
  if (this.corked) return
  while (this.waiting.length) this.waiting.shift()()
}

LevelDat.prototype.createVersionStream = function(key, opts) {
  if (this.corked) return this._wait(this.createVersionStream, arguments, true)

  opts = this._mixin(opts)

  var prefix = PREFIX_DATA+(opts.subset || '')+SEP

  opts.start = prefix+key+SEP
  opts.end = prefix+key+SEP+SEP

  var stream = through.obj(function(data, enc, cb) {
    var vidx = data.key.lastIndexOf(SEP)

    data = {
      key: data.key.slice(prefix.length, vidx),
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
  if (this.corked) return this._wait(this.createValueStream, arguments, true)

  opts = this._mixin(opts)
  opts.keys = false
  opts.values = true
  return this.createReadStream(opts)
}

LevelDat.prototype.keyStream =
LevelDat.prototype.createKeyStream = function(opts) {
  if (this.corked) return this._wait(this.createKeyStream, arguments, true)

  opts = this._mixin(opts)
  opts.keys = true
  opts.values = false
  return this.createReadStream(opts)
}

LevelDat.prototype.readStream =
LevelDat.prototype.createReadStream = function(opts) {
  if (this.corked) return this._wait(this.createReadStream, arguments, true)
  opts = this._mixin(opts)

  var self = this
  var keys = opts.keys !== false
  var values = opts.values !== false
  var subset = opts.subset || ''

  var pre = PREFIX_CUR+subset+SEP
  var ropts = {}

  if (opts.start) ropts.start = pre+(opts.start || '')
  else if (opts.gte) ropts.gte = pre+opts.gte
  else ropts.gt = pre+(opts.gt || '')

  if (opts.end) ropts.end = pre+opts.end
  else if (opts.lte) ropts.lte = pre+opts.lte
  else ropts.lt = pre+(opts.lt || SEP)

  if (opts.reverse) ropts.reverse = true
  if (opts.limit) ropts.limit = opts.limit

  var rs = self.db.createReadStream(fixRange(ropts))

  var get = through.obj(function(data, enc, cb) {
    var val = data.value
    var key = data.key.slice(PREFIX_CUR.length)
    var subset = key.slice(0, key.indexOf(SEP))

    key = key.slice(subset.length+1)

    if (deleted(val)) return cb()

    var version = unpack(val)
    self.mutex.get(PREFIX_DATA+subset+SEP+key+SEP+val, opts, function(err, data) {
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
  if (this.corked) return this._wait(this.createWriteStream, arguments, true)
  opts = this._mixin(opts)

  var self = this
  var subset = opts.subset || ''

  return through.obj(function(data, enc, cb) {
    var next = function(err) {
      cb(err)
    }

    if (data.type === 'del') return self.del(data.key, next)
    self._put(data.key, data.value, opts, data.version || 0, subset, next)
  })
}

LevelDat.prototype.createChangesWriteStream = function(opts) {
  if (this.corked) return this._wait(this.createChangesWriteStream, arguments, true)
  opts = this._mixin(opts)

  var self = this

  var buffer = byteStream({
    limit: WRITE_BUFFER_SIZE,
    time: 3000
  })

  var format = through.obj(function(data, enc, cb) {
    if (!data.value && data.to !== 0) return cb(new Error('data.value is required'))
    data.length = data.value ? data.value.length || 1 : 1
    cb(null, data)
  })

  var ws = through.obj(function(batch, enc, cb) {
    var wait = waiter(batch.length, cb)

    for (var i = 0; i < batch.length; i++) {
      var b = batch[i]
      var subset = b.subset || ''

      debug('put change (change: %d, key: %s, to: %s, from: %s)', b.change, b.key, b.to, b.from)

      if (b.change !== self.change+1) return cb(changeConflict())
      self.change = b.change

      if (b.to === 0) {
        self._change(b.change, b.key, b.from, 0, subset, null)
        self.mutex.put(PREFIX_CUR+subset+SEP+b.key, pack(b.from)+SEP+'1', wait)
      } else {
        var v = pack(b.to)
        self._change(b.change, b.key, b.from, b.to, subset, b.value)
        self.mutex.put(PREFIX_CUR+subset+SEP+b.key, v, noop)
        self.mutex.put(PREFIX_DATA+subset+SEP+b.key+SEP+v, b.value, opts, wait)
      }
    }
  })

  return pumpify.obj(format, buffer, ws)
}

LevelDat.prototype.ready = function(cb) {
  if (this.corked) return this._wait(this.ready, arguments)
  cb()
}

LevelDat.prototype._tail = function(since) {
  var self = this
  var next

  var rs = from.obj(function(size, cb) {
    next = cb
    onchange()
  })

  var onchange = function() {
    if (!next || self.changeFlushed <= since) return
    var cb = next
    next = null
    self.mutex.get(PREFIX_CHANGE+pack(++since), function(err, val) {
      if (err) return rs.destroy(err)
      debug('get change live (change: %d)', since)
      cb(null, val)
    })
  }

  var cleanup = once(function() {
    debug('cleanup live changes feed')
    self.onchange.splice(self.onchange.indexOf(onchange), 1)
  })

  self.onchange.push(onchange)
  rs.on('end', cleanup)
  rs.on('close', cleanup)

  return rs
}

LevelDat.prototype.approximateSize = function(start, enc, cb) {
  if (typeof start === 'function') return this.approximateSize(SEP, SEP+SEP, start)
  if (!this.db.db || !this.db.db.approximateSize) return cb(new Error('approximateSize is not available'))
  this.db.db.approximateSize(start, enc, cb)
}

LevelDat.prototype.createChangesReadStream = function(opts) {
  if (this.corked) return this._wait(this.createChangesReadStream, arguments, true)
  opts = this._mixin(opts)

  if (typeof opts.tail === 'number') opts.since = this.changeFlushed - opts.tail
  if (opts.tail === true) opts.since = this.changeFlushed

  var self = this
  var addData = !!opts.data
  var since = opts.since || 0
  var lastChange = since

  var format = function(data, enc, cb) {
    var value = JSON.parse(data)
    if (value[0] <= since) return cb()

    lastChange = value[0]

    var data = {
      change: value[0],
      key: value[1],
      from: value[2],
      to: value[3]
    }

    if (value[4]) data.subset = value[4]

    debug('get change (change: %d, key: %s, to: %d, from: %d, data: %s)', data.change, data.key, data.to, data.from, addData)
    if (!addData || data.to === 0) return cb(null, data)

    self._get(data.key, opts, data.to, data.subset, function(err, value) {
      if (err) return cb(err)
      data.value = value
      cb(null, data)
    })
  }

  var dbStream = function() {
    var rs = self.db.createValueStream({
      start: PREFIX_CHANGE+pack(since),
      end: PREFIX_CHANGE+SEP
    })

    return pump(rs, through.obj(format))
  }

  var liveStream = function() {
    return pump(self._tail(lastChange), through.obj(format))
  }

  if (opts.tail === true) return liveStream()
  if (opts.live) return multistream.obj([dbStream, liveStream])

  return dbStream()
}

LevelDat.prototype.get = function(key, opts, cb, version) {
  if (this.corked) return this._wait(this.get, arguments)
  if (typeof opts === 'function') return this.get(key, null, opts)
  opts = this._mixin(opts)

  if (!opts.version) this._getLatest(key, opts, opts.subset, cb)
  else this._get(key, opts, opts.version, opts.subset, cb)
}

LevelDat.prototype._getLatest = function(key, opts, subset, cb) {
  opts = this._mixin(opts)
  if (!subset) subset = ''

  var self = this
  this.mutex.get(PREFIX_CUR+subset+SEP+key, function(err, cur) {
    if (err) return cb(err)
    if (deleted(cur)) return cb(new Error('Key was deleted'))

    self._get(key, opts, unpack(cur), subset, cb)
  })
}

LevelDat.prototype._get = function(key, opts, version, subset, cb) {
  opts = this._mixin(opts)
  version = +version
  if (!subset) subset = ''

  this.mutex.get(PREFIX_DATA+subset+SEP+key+SEP+pack(version), opts, function(err, val) {
    if (err) return cb(err)

    debug('get data.%s (version: %d)', key, version)
    cb(null, val, version)
  })
}

LevelDat.prototype.put = function(key, value, opts, cb) {
  if (this.corked) return this._wait(this.put, arguments)
  if (typeof opts === 'function') return this.put(key, value, null, opts)
  if (!cb) cb = noop
  opts = this._mixin(opts)

  this._put(key, value, opts, opts.version, opts.subset || '', cb)
}

LevelDat.prototype.batch = function(batch, opts, cb) {
  if (this.corked) return this._wait(this.batch, arguments)
  if (typeof opts === 'function') return this.batch(batch, null, opts)
  if (!cb) cb = noop
  opts = this._mixin(opts)

  if (!batch.length) return cb()

  var subset = opts.subset || ''

  var loop = function(err) {
    if (err) return cb(err)
    var b = batch.shift()
    if (!b) return cb()
    if (b.type === 'del') this.del(b.key, loop)
    else this._put(b.key, b.value, opts, b.version || 0, subset, loop)
  }

  loop()
}

LevelDat.prototype._put = function(key, value, opts, version, subset, cb) {
  var autoVersion = !version
  if (!version) version = 1
  if (!subset) subset = ''
  opts = this._mixin(opts)

  var self = this

  this.mutex.get(PREFIX_CUR+subset+SEP+key, function(_, curV) {
    if (curV) curV = unpack(curV)
    if (curV) debug('put data.%s existing version exist (to: %d, from: %d)', key, version, curV)

    if (opts.force) version = (curV || 0)+1
    if (version < curV) return cb(conflict(key, version))
    if (version === curV && autoVersion) return cb(conflict(key, version))
    if (version === curV) version++

    var v = pack(version)
    var change = ++self.change
    debug('put data.%s (version: %d)', key, version)

    self._change(change, key, curV || 0, version, subset, value)
    self.mutex.put(PREFIX_CUR+subset+SEP+key, v, noop)
    self.mutex.put(PREFIX_DATA+subset+SEP+key+SEP+v, value, opts, function(err) {
      if (err) return cb(err)
      cb(null, value, version)
    })
  })
}

LevelDat.prototype.del =
LevelDat.prototype.delete = function(key, opts, cb) {
  if (this.corked) return this._wait(this.del, arguments)
  if (typeof opts === 'function') return this.del(key, null, opts)
  if (!cb) cb = noop
  if (!opts) opts = {}

  var self = this
  var subset = opts.subset || ''

  this.mutex.get(PREFIX_CUR+subset+SEP+key, function(err, v) {
    if (err) return cb(err)

    var change = ++self.change
    var version = unpack(v)

    debug('del data.%s', key)
    self._change(change, key, version, 0, subset, null)
    self.mutex.put(PREFIX_CUR+subset+SEP+key, v+SEP+'1', cb)
  })
}

LevelDat.prototype._change = function(change, key, from, to, subset, value) {
  this.emit('change', {change:change, key:key, from:from, to:to, subset:subset, value:value})
  this.mutex.put(PREFIX_CHANGE+pack(change), JSON.stringify([change, key, from, to, subset]), this.onchangewrite)
}

LevelDat.prototype._mixin = function(opts) {
  if (!opts) opts = {}
  if (!opts.valueEncoding) opts.valueEncoding = this.defaults.valueEncoding
  if (!opts.subset) opts.subset = this.defaults.subset
  if (this.defaults.force && opts.force === undefined) opts.force = true
  return opts
}

LevelDat.prototype._wait = function(fn, args, isStream) {
  var self = this

  if (isStream) {
    var proxy = duplexify.obj()

    this.waiting.push(function() {
      if (proxy.destroyed) return
      var s = fn.apply(self, args)
      proxy.setWritable(s.writable ? s : false)
      proxy.setReadable(s.readable ? s : false)
    })

    return proxy
  } else {
    this.waiting.push(function() {
      fn.apply(self, args)
    })
  }
}

LevelDat.prototype._putMeta = function(key, val, cb) {
  this.mutex.put(PREFIX_META+key, val, {valueEncoding:'json'}, cb)
}

LevelDat.prototype._getMeta = function(key, cb) {
  this.mutex.get(PREFIX_META+key, {valueEncoding:'json'}, cb)
}

LevelDat.prototype.stat = function(cb) {
  if (this.corked) return this._wait(this.stat, arguments)
  var self = this

  this.lock(function(release) {
    cb = once(release.bind(null, cb))

    self._getMeta('stat', function(err, result) {
      if (!result) result = {change:0, rows:0, inserts:0, updates:0, deletes:0, size:0}
      if (result.change === self.change) return cb(null, result)

      var changes = self.createChangesReadStream({
        since: result.change,
        data: true,
        valueEncoding: 'binary'
      })

      var persist = function(cb) {
        debug('put meta.stat (change: %d, rows: %d, size: %d)', result.change, result.rows, result.size)
        self._putMeta('stat', result, function(err) {
          if (err) return cb(err)
          cb(null)
        })
      }

      var inc = 0
      var ondata = function(data, enc, cb) {
        result.change = data.change
        if (data.subset) return cb()
        if (data.value) result.size += data.value.length

        if (data.to !== 0 && data.from === 0) {
          result.rows++
          result.inserts++
        }

        if (data.to === 0 && data.from !== 0) {
          result.rows--
          result.deletes++
        }

        if (data.to !== 0 && data.from !== 0) {
          result.updates++
        }

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
  })
}

module.exports = LevelDat