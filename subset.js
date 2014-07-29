var Subset = function(db, subset) {
  if (!(this instanceof Subset)) return new Subset(db, subset)
  this.db = db
  this.subset = subset
}

Subset.prototype.put = function(key, val, opts, cb) {
  if (typeof opts === 'function') return this.put(key, val, null, opts)
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.put(key, val, opts, cb)
}

Subset.prototype.get = function(key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.get(key, opts, cb)
}

Subset.prototype.del =
Subset.prototype.delete = function(key, opts, cb) {
  if (typeof opts === 'function') return this.del(key, null, opts)
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.del(key, opts, cb)
}

Subset.prototype.batch = function(batch, opts, cb) {
  if (typeof opts === 'function') return this.batch(batch, null, opts)
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.batch(batch, opts, cb)
}

Subset.prototype.versionStream =
Subset.prototype.createVersionStream = function(opts) {
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.createVersionStream(opts)
}

Subset.prototype.readStream =
Subset.prototype.createReadStream = function(opts) {
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.createReadStream(opts)
}

Subset.prototype.valueStream =
Subset.prototype.createValueStream = function(opts) {
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.createValueStream(opts)
}

Subset.prototype.keyStream =
Subset.prototype.createKeyStream = function(opts) {
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.createKeyStream(opts)
}

Subset.prototype.writeStream =
Subset.prototype.createWriteStream = function(opts) {
  if (!opts) opts = {}
  opts.subset = this.subset
  return this.db.createWriteStream(opts)
}

module.exports = Subset