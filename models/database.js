// Postgres setup

var dbconfig = require('config').database
  , util = require('util')
  , pg = require('pg').native

var db = {connection: pg};

db.conString = util.format('postgres://%s:%s@%s/%s', dbconfig.user, encodeURIComponent(dbconfig.password), dbconfig.host, dbconfig.database)

db.queryHandler = function (query, args, handler){
  return function(err, client, done) {
      if(err) {
        return console.error('error fetching client from pool', err);
      }

      client.query(query, args, handler);
      done();
  }
}


module.exports = db;
