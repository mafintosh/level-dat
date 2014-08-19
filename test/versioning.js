var test = require('./helpers')
var concat = require('concat-stream')

test('two versions', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.put('foo', 'baz', {version:1}, function(err) {
      t.notOk(err, 'no err')
      db.get('foo', function(err, val, version) {
        t.notOk(err, 'no err')
        t.same(val, 'baz')
        t.same(version, 2)
        t.end()
      })
    })
  })
})

test('conflict', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.put('foo', 'baz', function(err) {
      t.ok(err, 'conflict err')
      t.ok(err.conflict, 'conflict flag set')
      t.same(err.key, 'foo', 'key set')
      t.end()
    })
  })
})

test('no conflict on force', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.put('foo', 'baz', {force:true}, function(err) {
      t.notOk(err, 'no err')
      db.get('foo', function(err, val, version) {
        t.notOk(err, 'no err')
        t.same(val, 'baz')
        t.same(version, 2)
        t.end()
      })
    })
  })
})

test('version stream', function(t, db) {
  db.put('foo', 'bar', function(err) {
    t.notOk(err, 'no err')
    db.put('foo', 'baz', {version:1}, function(err) {
      t.notOk(err, 'no err')
      db.createVersionStream('foo').pipe(concat(function(versions) {
        t.same(versions.length, 2, '2 versions')
        t.same(versions[0], {key:'foo', value:'bar', version:1})
        t.same(versions[1], {key:'foo', value:'baz', version:2})
        t.end()
      }))
    })
  })
})

test('no conflict on force stream', function(t, db) {
  var ws = db.createWriteStream({force:true})

  ws.write({
    key: 'foo',
    value: '0'
  })

  ws.write({
    key: 'foo',
    value: '1'
  })

  ws.write({
    key: 'foo',
    value: '2'
  })

  ws.end(function() {
    db.createReadStream().on('data', function(data) {
      t.same(data, {key:'foo', value:'2', version:3})
      t.end()
    })
  })
})