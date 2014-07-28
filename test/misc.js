var test = require('./helpers')

test('stat', function(t, db) {
  db.put('hi', 'world', function() {
    db.put('hi', 'verden', {version:1}, function() {
      db.stat(function(err, stat) {
        t.notOk(err, 'no err')
        t.same(stat.rows, 1, 'rows is 1')
        db.put('hello', 'world', function() {
          db.stat(function(err, stat) {
            t.notOk(err, 'no err')
            t.same(stat.inserts, 2, '2 inserts')
            t.same(stat.updates, 1, '1 update')
            t.same(stat.rows, 2, 'rows is 2')
            t.same(stat.size, 16, 'size is 16')
            t.end()
          })
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
        t.same(stat.rows, 2, 'rows is 2')
        db.del('hello', function() {
          db.stat(function(err, stat) {
            t.notOk(err, 'no err')
            t.same(stat.rows, 1, 'rows is 1')
            t.same(stat.deletes, 1, '1 delete')
            t.same(stat.inserts, 2, '2 inserts')
            t.same(stat.size, 10, 'size is 10')
            t.end()
          })
        })
      })
    })
  })
})
