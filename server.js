var http = require('http')
  , express = require('express')
  , app = express()
  , config = require('config')
  , zmq = require('./models/zmq.js')
  , socket = zmq.subscribe()
  , util = require('util')
  , mongodb = require('./models/mongodb.js')
  , autoDispatch = require('./models/auto_dispatch.js');

var redis = require('redis')
  , redisConfig = config.get('redis')
  , redisClient = redis.createClient(redisConfig.port, redisConfig.host, {});

redisClient.on('error', function(err) {
  console.log(err);
});

var re = /^gps_/;

// when a message arrives to the subscriber
// broadcast it to all connected websockets 
socket.on('message', function(topic, message) {
  var msgObj;

  if (re.test(topic) || /^event_/.test(topic)){
    try{
      msgObj =  JSON.parse(message);
      msgObj.timestamp = Date.now();
    }catch(e){
      console.log(e);
      console.log(message.toString());
      return;
    }
  }

  if (re.test(topic)){
    msgObj.loc = {type: 'Point', coordinates: [msgObj.long, msgObj.lat]};

    if ('driver_id' in msgObj){
      //TODO: Make this one call. Fix the issue with replaceOne
      mongodb.db.collection('cords').deleteOne({driver_id: msgObj['driver_id']}, function(err, result){
        mongodb.db.collection('cords').insertOne(msgObj, mongodb.errorHandler(false));
      });

      mongodb.db.collection('history').insertOne(msgObj, mongodb.errorHandler(false));
      redisClient.hset('cords', topic, message);
    }

  } else if (/^event_/.test(topic)){ 
      console.log('received a message related to:', topic.toString(), 'containing message:', message.toString());

      if (topic == "event_new_driver"){
        mongodb.db.collection('nodes').replaceOne({driver_id: msgObj['driver_id']}, msgObj, {upsert: true}, mongodb.errorHandler(false));
        redisClient.hset('nodes', msgObj['driver_id'], msgObj['node']);

      } else if (topic == "event_drop_driver"){
        redisClient.DEL('nodes', msgObj['driver_id']);
        redisClient.DEL('cords', 'gps_' + msgObj['driver_id']);

        mongodb.db.collection('nodes').deleteOne({driver_id: msgObj['driver_id']}, mongodb.errorHandler(false));
        mongodb.db.collection('cords').deleteOne({driver_id: msgObj['driver_id']}, mongodb.errorHandler(false));
      }
  }

});

//TODO: Authenticate the query
/* listen to commands from downstream node and search drivers */
zmq.bindRouter(function(envelope, event, resourceId, msgId, data) {
  console.log('Router received ' +  event + ' ' + resourceId + ' ' + msgId + ' ' + data);
  var msgObj;
  var res = [event, msgId];
  var query = {maxDistance: 3500, limit: 10, 
                  query: {state: 'active', timestamp: {'$gt': (Date.now() - autoDispatch.DRIVER_EXPIRY)}}};
  try{
    msgObj = JSON.parse(data.toString());
    msgObj.results = [];

    if (/SEARCH/i.test(event.toString())){
      if (!msgObj.long || !msgObj.lat){
        errorResponse(422, 'Coordinates are not given');
        return;
      }

      if (msgObj.class_id){
        query.query['product.class_id'] = msgObj.class_id;
      }

      mongodb.geoNear('cords', [parseFloat(msgObj.long), parseFloat(msgObj.lat)], query, function(err, docs){
        if (err){
          errorResponse(500, err.message);
          return;
        }

        console.log(docs);
        msgObj.results = docs.results;
        respond([200, JSON.stringify(msgObj)]);
      });
    }else{
      errorResponse(422, 'Coordinates are not given');
      return;
    }
  }catch(e){
    errorResponse(500, e.message);
  }

  function errorResponse(code, message){
    console.log(e);
    msgObj.code = code;
    msgObj.error = message;
    respond([code, JSON.stringify(msgObj)]);
  }

  function respond(msg){
    zmq.router.send([envelope].concat(res.concat(msg)));
    console.log('Router response: '  + msg);
  }
});

//start server
var serverConfig = config.get('server');
var server = http.createServer(app);
server.listen(serverConfig.port);
 
module.exports = app;
