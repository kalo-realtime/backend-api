// zmq setup
var config = require('config')
  , driverConfig = config.get('driver')
  , zmqConfig = config.get('zmq')
  , zmqLib    = require('zmq')
  , subSocket  = zmqLib.socket('sub')
  , routerSocket= zmqLib.socket('router');

var dealerSockets = []
  , broadcastMessages = {}
  , zmq = {};

console.log("ZMQ: Driver node count: " + driverConfig.nodes.length);

// bind router to receive notifications to Driver
routerSocket.bind('tcp://' + zmqConfig.host + ':' + zmqConfig.routerPort, function(err) {
  if (err) throw err;
  console.log('Router bound to ' + zmqConfig.routerPort);
});

// binds a given socket to all driver nodes
function bindSocket(handler){
  // connect to all driver nodes (publishers)
  for (var i = 0, len = driverConfig.nodes.length; i < len; i++) {
    try{
      handler(i);
    }catch(ex){
      console.log('Error occurred while responding: ' + ex);
    }
  }
}

/* generate a random number to identify the outgoing message */
zmq.messageId = function(){
  return Math.floor((Math.random() * 10000000000) + 1);
}

/* send a message to the driver node through a selected dealder socket */
zmq.msgDriver = function(nodeId, msgId, event, hashKey, data, callback){
  var dealer = dealerSockets[nodeId];

  dealer.kaloConnections[msgId] = callback;
  dealer.send([event, hashKey, msgId, JSON.stringify(data)]);
}

/* broadcast a message to all driver nodes */
zmq.msgAllDrivers = function(msgId, event, hashKey, data, callback){
  for (i = 0; i < dealerSockets.length; i++){
    this.msgDriver(i, msgId, event, hashKey, data, callback);
  }
}

/** convenient method to call all drivers with higher reliability */
zmq.reliableMsgDrivers = function(errorFun, driverHashKey, data, callback){
  return function(err, nodeId){
    if (err){
      errorFun(err);
    }

    var msgId = zmq.messageId();
    if (nodeId != null){
      zmq.msgDriver(nodeId, msgId, 'driver', driverHashKey, data, callback);
    }else{
      zmq.msgAllDrivers(msgId, 'allDrivers', driverHashKey, data, callback);
    }
  };
}

// connect to all driver nodes (publishers)
zmq.subscribe = function(){

  bindSocket(bindSubscriber);

  function bindSubscriber(n){
    var endPoint = driverConfig.nodes[n].host + ':' + driverConfig.nodes[n].publishPort;
    subSocket.connect('tcp://' + endPoint);
    console.log('ZMQ(Sub): Subscribed to driver node: ' + endPoint);

    subSocket.subscribe('event');
    subSocket.subscribe('gps');
  }

  return subSocket;
};


// bind all dealers
zmq.dealers = function(){

  bindSocket(bindDealer);

  console.log('ZMQ(dealers): Connected');

  function bindDealer(n){
      var endPoint = driverConfig.nodes[n].host + ':' + driverConfig.nodes[n].routerPort;

      dealer = zmqLib.socket('dealer');
      dealer.kaloConnections = {};
      dealer.connect('tcp://'+ endPoint);

      dealer.on('message', function(event, id, code, msg) {
        console.log('Dealer: answer data ' + event + ' ' + id + ' ' + code +  ' ' + msg);
        var id = parseInt(id)

        if (event == 'driver'){
          if (id in this.kaloConnections && typeof this.kaloConnections[id] === 'function'){
            this.kaloConnections[id](code, msg);
            delete this.kaloConnections[id];
          }
        } else if (event == 'allDrivers'){
          if (!(id in broadcastMessages)){
            broadcastMessages[id] = [];
          }

          broadcastMessages[id].push([this, code, msg]);

          // finished receiving responses from all driver nodes
          if (broadcastMessages[id].length == dealerSockets.length){
            var response;

            for (i = 0; i < broadcastMessages[id].length; i++){
              response = broadcastMessages[id][i];

              if (response[1] == 200) break;
            }

            if (response && typeof response[0].kaloConnections[id] === 'function'){
              response[0].kaloConnections[id](response[1], response[2]);
            }

            delete broadcastMessages[id];
          }
        }
      });

      dealerSockets.push(dealer);
    }

    return dealerSockets;
};

zmq.bindRouter = function(handler){
  routerSocket.on('message', handler);
}

zmq.router = routerSocket;

// return a sub or dealer socket
module.exports = zmq;
