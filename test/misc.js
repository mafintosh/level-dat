var test = require('./helpers')

test('count', function(t, db) {
  db.put('hi', 'world', function() {
    db.count(function(err, cnt) {
      t.notOk(err, 'no err')
      t.same(cnt, 1, 'count is 1')
      db.put('hello', 'world', function() {
        db.count(function(err, cnt) {
          t.notOk(err, 'no err')
          t.same(cnt, 2, 'count is 2')
          t.end()
        })
      })
    })
  })
})

test('count + del', function(t, db) {
  db.put('hi', 'world', function() {
    db.put('hello', 'world', function() {
      db.count(function(err, cnt) {
        t.notOk(err, 'no err')
        t.same(cnt, 2, 'count is 2')
        db.del('hello', function() {
          db.count(function(err, cnt) {
            t.notOk(err, 'no err')
            t.same(cnt, 1, 'count is 1')
            t.end()
          })
        })
      })
    })
  })
})

test('meta', function(t, db) {
  db.meta.put('hello', 'world', function(err) {
    t.notOk(err, 'no err')
    db.meta.get('hello', function(err, val) {
      t.notOk(err, 'no err')
      t.same(val, 'world')
      t.end()
    })
  })
})