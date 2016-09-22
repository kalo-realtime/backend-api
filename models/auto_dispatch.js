var mongodb = require('./mongodb.js')
  , redis = require('redis')
  , redisConfig = require('config').redis
  , db = require('./database.js')
  , resque = require('./resque.js')
  , redisClient = redis.createClient(redisConfig.port, redisConfig.host, {})
  , zmq = require('./zmq.js')
  , autoDispatch = {};

// maximum number of attempts to search a driver
autoDispatch.MAX_ATTEMPTS = 3;
autoDispatch.DRIVER_EXPIRY = 20 * 1000; //driver expiry in miliseconds

// check whether there's a booking ID with given dispather ID
autoDispatch.checkAndRun = function(bookingId, dispatchId, success, fail, error){

  var query = 'SELECT * FROM bookings WHERE hash_key = $1 ';
  var args = [bookingId];

  if (dispatchId === null){
    query += 'AND dispatcher_id IS NULL';
  }else if (dispatchId > 0){
    query += 'AND dispatcher_id = $2';
    args.push(dispatchId);
  }

  query +=  ' LIMIT 1';

  db.connection.connect(db.conString, db.queryHandler(query, args, function(err, results){
    if(err) {
      console.error(err.message);
      error(err);

    } else if (!results.rows.length){
      fail();

    } else {
      success(results);
    }
  }));
}

// after the given timeout for the dispatch queue
// continue the search
autoDispatch.continueSearch = function(queue, job, data){
  var redisKey = 'dispatch-' + data.bookingId + '-' + data.dispatchId;
  // resque freezes parameters so making a copy
  var data = {bookingId: data.bookingId, dispatchId: data.dispatchId, 
    attempts: data.attempts, request: data.request};

  if (data.attempts > autoDispatch.MAX_ATTEMPTS){
    stopSearch();
    return;
  }

  autoDispatch.checkAndRun(data.bookingId, data.dispatchId, keepSearching, stopSearch, errorSearch);

  function keepSearching(results){
    console.log("Continuing dispatch...: " + data.bookingId);

    if (results.rows[0].dispatcher_attempt != data.attempts){
      console.log("Dispatcher attempt mismatch. closing.: " + data.bookingId);
      return;
    }

    // check driver is already attempted
    // ping the driver
    redisClient.smembers(redisKey, function(err, drivers){
      var query, coordinates;

      if (err){
        //FIXME: Load attempted driver list from postgres
        drivers = [];
      }

      //TODO: Improve the logic in selecting drivers (ie: busy but at the end of the trip .etc)
      coordinates = (results.rows[0].from_gps || '').split(',').reverse().map(function(o) {return parseFloat(o);});
      query = {maxDistance: 3500, limit: 1, query: 
                {driver_id: {'$not': {'$in': drivers} }, state: 'active', timestamp: {'$gt': (Date.now() - autoDispatch.DRIVER_EXPIRY)} }};
      if (data.request && data.request.booking && data.request.booking.class_id){
        query.query['product.class_id'] = data.request.booking.class_id;
      }

      //TODO: check the index performance
      mongodb.geoNear('cords', coordinates, query, function(err, docs){
        if (err){
          errorSearch(err);
          return;
        }

        if(docs.results.length > 0){
          var driver = docs.results[0].obj;
          redisClient.hget('nodes', driver.driver_id, 
                           zmq.reliableMsgDrivers(errorSearch, driver.driver_id, data.request, function(code, msg){
                             // check for message code and take action
                           }));

          data.attempts = data.attempts + 1;
          
          if (data.attempts > autoDispatch.MAX_ATTEMPTS){
            stopSearch();
          } else {
            updateDispatchAttempt(data.bookingId, data.dispatchId, data.attempts, function(){
              redisClient.sadd(redisKey, driver.driver_id, function(err, result){
                console.log("Added new driver " + driver.driver_id + " to booking dispatch " + data.bookingId);
              })
              resque.enqueueDispatch(data, data.lookup_time || 60);
            }, errorSearch)
          }

        } else {
          console.log('No results found for ' + data.bookingId);
          stopSearch();
        }
      });
    });
  }

  function stopSearch(){
    console.log("Stopping search: " + data.bookingId);
    stopSingleDispatch(data.bookingId, data.dispatchId);
  }

  function errorSearch(err){
    console.log("Error in dispatch continue: " + err + " Data: " + data);
    stopSearch();
  }

  function prepareMongoQuery(query, data){
  }
}

// start a dispatch process if there's no current dispatch for the booking
// FIXME: Use a read-write lock to make it atomic
autoDispatch.start = function(bookingId, request, success, fail, error){
  var dispatchId = new Date().getTime();

  // check if current dispatch running
  autoDispatch.checkAndRun(bookingId, null, initDispatch, fail, error);

    // call autoDispatch
  function initDispatch(){

    var data = {bookingId: bookingId, dispatchId: dispatchId, attempts: 0, request: request};
    var query = 'UPDATE bookings SET dispatcher_id = $1, dispatcher_attempt = 0 WHERE hash_key = $2;';

    db.connection.connect(db.conString, db.queryHandler(query, [dispatchId, bookingId], function(err){
      if(err) {
        console.error(err.message);
        if(typeof error === 'function'){ error(err); }
      } else{
          //start resque queue
        resque.enqueueDispatch(data, 0);
        if(typeof success === 'function'){ success(dispatchId); }
      }
    }));
  }
}

// search for the next available driver
// FIXME: Use locking to stop shared reading of booking
autoDispatch.next = function(bookingId, request, success, error){
  if (!bookingId){
    if(typeof error === 'function'){ error(new Error("No booking ID or data given")); }
    return;
  }
  
  autoDispatch.checkAndRun(bookingId, 0, nextAttempt, noCurrentDispatch, error);

  function nextAttempt(results){
    var data = {bookingId: bookingId, dispatchId: results.rows[0].dispatcher_id, 
                  attempts: results.rows[0].dispatcher_attempt || 0, request: request};
    data.attempts = data.attempts + 1;

    if (data.dispatchId){
      updateDispatchAttempt(bookingId, data.dispatchId, data.attempts, function(){
        resque.enqueueDispatch(data, 0);
        success();
      }, error)
    }else{
      if(typeof error === 'function'){ error(new Error("No dispatch running.")); }
    }
  }

  function noCurrentDispatch(){
    var msg = "No such booking: " + bookingId;
    console.log(msg);
    if(typeof error === 'function'){ error(new Error(msg)); }
  }
}

// stop dispatches for a booking
// might remove more than one dispatches currently running
autoDispatch.stop = function(bookingId, callback, error){
  if (!bookingId){
    if(typeof error === 'function'){ error(new Error("No booking ID given")); }
    return;
  }

  console.log("Stopping all booking dispatch: " + bookingId);
  var query = 'UPDATE bookings SET dispatcher_id = NULL, dispatcher_attempt = NULL WHERE hash_key = $1;';

  db.connection.connect(db.conString, db.queryHandler(query, [bookingId], function(err){
    if(err) {
      if(typeof error === 'function'){ error(err, bookingId); }
    }

    if(typeof callback === 'function'){ callback(bookingId); }
  }));
}

// stop an invidual dispatch job
function stopSingleDispatch(bookingId, dispatchId, success, error){
  console.log("Stopping search: " + bookingId + " dispatchId: " + dispatchId);

  var query = 'UPDATE bookings SET dispatcher_id = NULL, dispatcher_attempt = NULL WHERE hash_key = $1 AND dispatcher_id = $2;';
  var redisKey = 'dispatch-' + bookingId + '-' + dispatchId;

  db.connection.connect(db.conString, db.queryHandler(query, [bookingId, dispatchId], function(err){
    if(err) {
      console.log("Error stopping dispatch for booking: " + bookingId + " dispatchId: " + dispatchId + 
                  " Error: " + err);
      if(typeof error === 'function'){ error(err, bookingId, dispatchId); }

    } else{
      redisClient.del(redisKey, function(err, res){
        console.log("Removed data: " + bookingId + " dispatchId: " + dispatchId);

        if(typeof success === 'function'){ success(bookingId, dispatchId); }
      });
    }
  }));
}

// update dispatch attempt
function updateDispatchAttempt(bookingId, dispatchId, attempt, success, error){
  if (!bookingId || !dispatchId || attempt === undefined){
    err = "Can't update dispatch attempt " + bookingId + ' ' + dispatchId;
    console.log(err);
    if(typeof error === 'function'){ error(new Error(err), bookingId, dispatchId, attempt); }
    return;
  }

  var query = 'UPDATE bookings SET dispatcher_attempt = $3 WHERE hash_key = $1 AND dispatcher_id = $2;';

  db.connection.connect(db.conString, db.queryHandler(query, [bookingId, dispatchId, attempt], function(err){
    if(err) {
      console.log("Error updating dispatch attempt: " + err + ' ' + bookingId + ' ' + dispatchId);
      if(typeof error === 'function'){ error(err, bookingId, dispatchId, attempt); }

    } else{
      console.log("Updated dispatch attempt: " + bookingId + " dispatchId: " + dispatchId + ' ' + attempt);

      if(typeof success === 'function'){ success(bookingId, dispatchId, attempt); }
    }
  }));
}

//bind to worker events
resque.worker.on('success', autoDispatch.continueSearch);
resque.worker.on('failure', function(queue, job, failure){ 
  console.log("Error!: in " + queue + ": " + failure + " " + JSON.stringify(job)) 
  if (job && job.args.length > 0){
    stopSingleDispatch(job.args[0].bookingId, job.args[0].dispatchId);
  }
});

module.exports = autoDispatch;
