Dispatcher REST API 
=================

Responsibilities
----------------

1. Authenticate drivers/passengers/users
2. Get a diff of current drivers
3. Get all registered drivers
4. Ping a specific driver/passenger
5. Ping drivers within a defined perimeter

Installation
------------

#### Key Dependencies

* Redis - http://redis.io 
* MongoDB - https://docs.mongodb.com
* Postgres - https://www.postgresql.org
* ZMQ - http://zeromq.org
* Node - https://nodejs.org/en/download

#### Build Steps

1.&nbsp;Clone the library.

```
  git clone git@github.com:kalo-realtime/backend-api.git
```

2.&nbsp;Install dependencies.
```
  cd backend-api/
  npm install
```
3.&nbsp;Configure redis, mongo and postgres configs at config/default.json.

4.&nbsp;Make sure redis, mongo and postgres services are running and no other service is running on defined ports for zmq sockets.

5.&nbsp;Start the server

```
  npm start
```

TODO
----

### Abstraction layers
* Pubsub layer (kafka, zmq .etc)
* Transport layer (web socket, mqtt)
* Message type (text/binary)
* Message format (json, protocol buffers .etc)
* Message filtering
* Authentication
* Location storage (mongo, redis, postgis. etc)

License
=======
[Kalo is released under the MIT license](https://opensource.org/licenses/MIT)
