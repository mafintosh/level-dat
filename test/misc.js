var test = require('./helpers')

test('stat', function(t, db) {
  db.put('hi', 'world', function() {
    db.stat(function(err, stat) {
      t.notOk(err, 'no err')
      t.same(stat.count, 1, 'count is 1')
      db.put('hello', 'world', function() {
        db.stat(function(err, stat) {
          t.notOk(err, 'no err')
          t.same(stat.count, 2, 'count is 2')
          t.same(stat.size, 10, 'size is 10')
          t.end()
        })
      })
    })
  })
})

test('stat + del', function(t, db) {
  db.put('hi', 'world', function() {
    db.put('hello', 'world', function() {
      db.stat(function(err, stat) {
        t.notOk(err, 'no err')
        t.same(stat.count, 2, 'count is 2')
        db.del('hello', function() {
          db.stat(function(err, stat) {
            t.notOk(err, 'no err')
            t.same(stat.count, 1, 'count is 1')
            t.same(stat.size, 10, 'size is 10')
            t.end()
          })
        })
      })
    })
  })
})
