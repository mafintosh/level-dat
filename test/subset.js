var test = require('./helpers')
var concat = require('concat-stream')

test('subset put + get', function(t, db) {
  var s1 = db.subset('s1')
  var s2 = db.subset('s2')

  s1.put('hello', 's1', function() {
    s2.put('hello', 's2', function() {
      s1.get('hello', function(err, data) {
        t.notOk(err, 'no err')
        t.same(data, 's1')
        s2.get('hello', function(err, data) {
          t.notOk(err, 'no err')
          t.same(data, 's2')
          t.end()
        })
      })
    })
  })
})

test('subset changes', function(t, db) {
  var s1 = db.subset('s1')
  var s2 = db.subset('s2')

  s1.put('hello', 's1', function() {
    s2.put('hello', 's2', function() {
      db.createChangesReadStream().pipe(concat(function(changes) {
        t.same(changes.length, 2, '2 changes')
        t.same(changes[0], {key:'hello', subset:'s1', from:0, to:1, change:1})
        t.same(changes[1], {key:'hello', subset:'s2', from:0, to:1, change:2})
        t.end()
      }))
    })
  })
})

test('subset changes + regular', function(t, db) {
  var s1 = db.subset('s1')
  var s2 = db.subset('s2')

  s1.put('hello', 's1', function() {
    s2.put('hello', 's2', function() {
      db.put('hello', 'regular', function() {
        db.createChangesReadStream({data:true}).pipe(concat(function(changes) {
          t.same(changes.length, 3, '3 changes')
          t.same(changes[0], {key:'hello', subset:'s1', from:0, to:1, change:1, value:'s1'})
          t.same(changes[1], {key:'hello', subset:'s2', from:0, to:1, change:2, value:'s2'})
          t.same(changes[2], {key:'hello', from:0, to:1, change:3, value:'regular'})
          t.end()
        }))
      })
    })
  })
})