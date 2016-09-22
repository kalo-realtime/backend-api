var express = require('express')
  , bodyParser = require('body-parser')
  , jsonParser = bodyParser.json()
  , router = express.Router()
  , db = require('../models/database.js')
  , autoDispatch = require('../models/auto_dispatch.js')
  , zmq = require('../models/zmq.js')
  , dealers = zmq.dealers()
  , resqueJob = require('../models/resque.js')
  , mongodb = require('../models/mongodb.js');

var redis = require('redis')
  , redisConfig = require('config').redis
  , redisClient = redis.createClient(redisConfig.port, redisConfig.host, {});

/* GET all drivers */
router.get('/', function(req, res, next) {
  res.send('Authorization key not found or invalid', 422);
});

/* ping a connected driver */
router.post('/ping/:id', jsonParser, function(req, res, next) {
  var hashKey = req.params.id
    , data = req.body;

  if (!data.event){
    sendError(res, 'Event is not defined', 422);
    return;
  }

  redisClient.hget('nodes', hashKey, function(err, nodeId){
    if (err){
      console.error(err);
      sendError(res, 'Redis failure', 500);
      return;
    }

    var msgId = zmq.messageId();
    if (nodeId != null){
      zmq.msgDriver(nodeId, msgId, 'driver', hashKey, data, msgResponse);

    }else{
      // check whether this driver's cordinates are available
      redisClient.hget('cords', 'gps_' + hashKey, function(err, cord){
        if (cord != null){
          zmq.msgAllDrivers(msgId, 'allDrivers', hashKey, data, msgResponse);
        }else{
          sendError(res, 'User not found', 401);
        }
      })
    }
  });

  function msgResponse(code, msg){
    res.status(code).send(msg);
  };
});

/* authenticate a driver */
router.get('/auth/:key', function(req, res, next){
  var key = req.params.key;

  if (!key || key.length > 25){
    //console.error(err.stack);
    res.send('Authorization key not found or invalid', 422);
    return;
  }

  //this initializes a connection pool
  var query = 'SELECT * FROM drivers WHERE authentication_token = $1 LIMIT 1;';
  //WHERE authentication_token="' + key + '"
  db.connection.connect(db.conString, db.queryHandler(query, [key], function(err, results){

    if(err) {
      console.error(err.message);
      sendError(res, 'Server error', 500);
      return;

    } else if (results.rows.length != 1){
      sendError(res, "Authentication failed", 401);
      return;
    }

    return res.status(200).json(results.rows[0]);
  }));
  
});

// Middleware that is specific to this router
router.use(function timeLog(req, res, next) {
  next();
});

/* Find the nearest driver to a given location */
router.post('/search', jsonParser, function(req, res, next) {
  var location, query;

  if (!(req.params.long && req.params.lat)){
    sendError(res, 'Location parameters are not given.', 422);
    return;
  }

  location = [req.params.long, req.params.lat];
  query = {maxDistance: 3500, limit: 10, query: {state: 'active', timestamp: {'$gt': (Date.now() - autoDispatch.DRIVER_EXPIRY)} }};

  mongodb.geoNear('cords', location, query, function(err, docs){
    return res.status(200).json(docs);
  });

});

/* Start dispatch process */
router.post('/dispatch/:id/start', jsonParser, function(req, res, next) {
  var bookingId = req.params.id
    , data = req.body;

    autoDispatch.start(bookingId, data, success, fail, error);

    function success(dispatchId){
      console.log("Starting dispatch for booking ID: " + bookingId + " dispatchID: " + dispatchId);
      res.status(200).json({});
    };

    function fail(dispatchId){
      sendError(res, "Failed to start the dispatch for booking ID: " 
                + bookingId + " dispatchID: " + dispatchId, 422);
    };

    function error(err, dispatchId){
      console.log(err.stack);
      sendError(res, err.message, 500);
    };
});

/* Stop a dispatch process for a booking id */
router.post('/dispatch/:id/stop', function(req, res, next) {
  var bookingId = req.params.id;

  autoDispatch.stop(bookingId, success, error);

  function success(){
    console.log("Stopping dispatch for booking ID: " + bookingId);
    return res.status(200).json({});
  };

  function error(err){
    sendError(res, err.message, 500);
  };
});

/* Give the next driver in the dispatch process */
router.post('/dispatch/:id/next', jsonParser, function(req, res, next) {
  var bookingId = req.params.id
    , data = req.body;

  autoDispatch.next(bookingId, data, success, error);

  function success(){
    console.log("Trying next dispatch for booking ID: " + bookingId);
    return res.status(200).json({});
  };

  function error(err){
    sendError(res, err.message, 500);
  };
});

/* Handle redis errors */
redisClient.on('error', function(err) {
  console.log(err);
});

/* handle server error responses */
function sendError(res, msg, status){
  console.error(msg);
  res.status(status).send('{"error": "' + msg + '"}')
}

module.exports = router;
