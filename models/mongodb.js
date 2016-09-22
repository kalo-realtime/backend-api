// Mongodb setup
var MongoClient = require('mongodb').MongoClient
  , mongoConfig = require('config').mongodb
  , util = require('util')
  , mongodb = { db: null };
   
var mongoUrl = util.format('mongodb://%s:%s/%s', mongoConfig.host, mongoConfig.port, mongoConfig.database)

mongodb.defaultRadius = 3500;
mongodb.returnLimit = 10;

(mongodb.initConnection = function(conObj){
  /* Start mongo db pool */
  MongoClient.connect(mongoUrl, {server: {poolSize: 5}}, function(err, db) {
    if(err) throw err;

    //indexes
    db.collection('nodes').createIndex({driver_id: 1}, {w: 1, j: true, unique: true});

    db.collection('history').createIndex({driver_id: 1}, {w: 1});
    db.collection('history').createIndex({state: 1}, {w: 1});

    db.collection('cords').createIndex({loc : "2dsphere"});
    db.collection('cords').createIndex({driver_id: 1}, {w: 1, unique: true});
    db.collection('cords').createIndex({state: 1}, {w: 1});
    db.collection('cords').createIndex({timestamp: 1}, {w: 1});
    db.collection('cords').createIndex({"product.class_id": 1}, {w: 1});

    conObj.db = db;
    console.log("Mongo started on " + mongoUrl);
  });
})(mongodb);

// simple error handler for mongo
mongodb.errorHandler = function(printAll){
  return function(err, result) { 
    if (err) { 
      console.log("Mongo error: " + err)
    } 

    if (printAll){
      console.log(result);
    }
  }
}

mongodb.geoNear = function(field, cords, args, callback){
  console.log("Mongo query: " + JSON.stringify(cords) + ' ' + JSON.stringify(args));

  return this.db.command({ 
    geoNear : field, //'cords', 
    near : { type : "Point" , coordinates: cords },
    spherical : true, 
    maxDistance: args.maxDistance || mongodb.defaultRadius,
    limit: args.limit || mongodb.returnLimit,
    query: args.query || {}
  }, 
  callback
)};

module.exports = mongodb;
