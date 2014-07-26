var test = require('./helpers')
var concat = require('concat-stream')

test('.put + .get', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.get('foo', function(err, data) {
      t.notOk(err, 'no err')
      t.equal(data, 'bar')
      t.end()
    })
  })
})

test('.del', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.del('foo', function(err) {
      t.notOk(err, 'no err')
      db.get('foo', function(err) {
        t.ok(err, 'should error')
        t.end()
      })
    })
  })
})

test('.createReadStream', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.createReadStream().pipe(concat(function(rows) {
      t.equal(rows.length, 1, '1 row')
      t.ok(rows[0].key, '.key')
      t.ok(rows[0].value, '.value')
      t.end()
    }))
  })
})

test('.readStream', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.readStream().pipe(concat(function(rows) {
      t.equal(rows.length, 1, '1 row')
      t.ok(rows[0].key, '.key')
      t.ok(rows[0].value, '.value')
      t.end()
    }))
  })
})

test('.createValueStream', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.createValueStream().pipe(concat({encoding:'object'}, function(rows) {
      t.equal(rows.length, 1, '1 row')
      t.equal(rows[0], 'bar')
      t.end()
    }))
  })
})

test('.valueStream', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.valueStream().pipe(concat({encoding:'object'}, function(rows) {
      t.equal(rows.length, 1, '1 row')
      t.equal(rows[0], 'bar')
      t.end()
    }))
  })
})

test('.createKeyStream', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.createKeyStream().pipe(concat({encoding:'object'}, function(rows) {
      t.equal(rows.length, 1, '1 key')
      t.equal(rows[0], 'foo')
      t.end()
    }))
  })
})

test('.keyStream', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.keyStream().pipe(concat({encoding:'object'}, function(rows) {
      t.equal(rows.length, 1, '1 key')
      t.equal(rows[0], 'foo')
      t.end()
    }))
  })
})

test('.createWriteStream', function(t, db) {
  var ws = db.createWriteStream()

  ws.on('finish', function() {
    db.keyStream().pipe(concat({encoding:'object'}, function(rows) {
      t.equal(rows.length, 1, '1 key')
      t.equal(rows[0], 'foo')
      t.end()
    }))
  })

  ws.write({
    key: 'foo',
    value: 'hello'
  })

  ws.end()
})

test('.writeStream', function(t, db) {
  var ws = db.writeStream()

  ws.on('finish', function() {
    db.keyStream().pipe(concat({encoding:'object'}, function(rows) {
      t.equal(rows.length, 1, '1 key')
      t.equal(rows[0], 'foo')
      t.end()
    }))
  })

  ws.write({
    key: 'foo',
    value: 'hello'
  })

  ws.end()
})