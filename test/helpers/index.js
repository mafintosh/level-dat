var tape = require('tape')
var memdown = require('memdown')
var levelup = require('levelup')
var dat = require('../../')

var create = function() {
  return dat(levelup('test.db', {db:memdown}))
}

module.exports = function(name, fn) {
  tape(name, function(t) {
    var db = create()
    db.on('ready', function() {
      fn(t, db, create)
    })
  })
}