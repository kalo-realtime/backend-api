// Resque setup
var resqueConfig = require('config').resque
  , util = require('util')
  , db = require('./database.js')
  , NR = require("node-resque")
  , config = require('config').server
  , client = require('restler')
  , resque = {};
  
// resque task definition
var jobs = {
  check_booking: {
    perform: function(data, callback){
      console.log('Running NR perform: ' + JSON.stringify(data));
      callback(null, data);
    }
  }
};

resque.jobs = jobs;
resque.queueName = 'booking';

// Start a worker
var worker = new NR.worker({connection: resqueConfig, queues: [resque.queueName]}, jobs);
worker.connect(function(){
  worker.workerCleanup();
  worker.start();
});

resque.worker = worker;
    
// Start the scheduler
var scheduler = new NR.scheduler({connection: resqueConfig});
scheduler.connect(function(){
  scheduler.start();
});

resque.scheduler = scheduler;

resque.enqueueDispatch = function(data, runAfterSecs, callback){
  var queue = new NR.queue({connection: resqueConfig}, jobs);
  queue.on('error', function(error){ console.log(error); });
 
  // Connect to a queue
  queue.connect(function(){
    if (runAfterSecs == 0){
      queue.enqueue(resque.queueName, "check_booking", [data], callback);
    }else{
      queue.enqueueIn(runAfterSecs * 1000, resque.queueName, "check_booking", [data], callback);
    }
  });

  return queue;
}

worker.on('start',           function(){ console.log("worker started"); });
worker.on('end',             function(){ console.log("worker ended"); });
worker.on('cleaning_worker', function(worker, pid){ console.log("cleaning old worker " + worker); });
//worker.on('job',             function(queue, job){ console.log("working job " + queue + " " + JSON.stringify(job)); });
//worker.on('reEnqueue',       function(queue, job, plugin){ console.log("reEnqueue job (" + plugin + ") " + queue + " " + JSON.stringify(job)); });
//worker.on('success',         function(queue, job, result){ console.log("job success in " + queue + " " + JSON.stringify(job) + " >> " + result); });
worker.on('failure',         function(queue, job, failure){ console.log("job failure in " + queue + " " + JSON.stringify(job) + " >> " + failure); });
worker.on('error',           function(queue, job, error){ console.log("error in " + queue + " " + JSON.stringify(job) + " >> " + error); });


scheduler.on('start', function(){ console.log("scheduler started"); });
scheduler.on('end', function(){ console.log("scheduler ended"); });


// Node shutdown callback
/*
var shutdown = function(){
  scheduler.end(function(){
    worker.end(function(){
      console.log('bye.');
      process.exit();
    });
  });
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
*/

module.exports = resque;
