# level-dat

LevelDB storage backend for [Dat](https://github.com/maxogden/dat)

``` js
npm install level-dat
```

[![build status](http://img.shields.io/travis/mafintosh/level-dat.svg?style=flat)](http://travis-ci.org/mafintosh/level-dat)
![dat](http://img.shields.io/badge/Development%20sponsored%20by-dat-green.svg?style=flat)

## Usage

``` js
var ldat = require('level-dat')

db = ldat(db) // where db is levelup instance
db.createReadStream().on('data', console.log)
```

## API

In general the API is the same as the [levelup api](https://github.com/rvagg/node-levelup)

#### `db.put(key, value, [opts], [cb])`

Insert a key and value. Use `opts.version = number` to specify the version.

#### `db.get(key, [opts], cb)`

Get a key and value and version.

#### `db.del(key, [cb])`

Delete a key.

#### `var subdb = db.subset(name)`

Create a subset database that will be versioned and replicated as well

#### `db.stat(cb)`

Returns a digest of all rows in the database.

#### `db.createReadStream([opts])`

Create a read stream to the database. Data includes the version.

#### `db.createValueStream([opts])`

Only get the values.

#### `db.createKeyStream([opts])`

Only get the keys.

#### `db.createVersionStream(key, [opts])`

Get all stored versions of a key.

#### `db.createWriteStream([opts])`

Stream data into the database. Data can include versions.

#### `db.createChangesReadStream([opts])`

Get a change feed stream from the database. Set `options.since = change` to only get a partial stream and `options.data` to get the data as well.

#### `db.createChangesWriteStream([opts])`

Pipe a change feed into the change write stream to replicate a database.

# License

MIT

