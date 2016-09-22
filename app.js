// driver app start script
var express = require('express')
  , logger = require('morgan')
  , path = require('path')
  , config = require('config').server
  , app = require('./server.js')
  , basicAuth = require('basic-auth');

app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');
app.use(express.static(path.join(__dirname, 'public')));
app.use(logger('dev'));

// authenticate with basic auth
var requireAuthentication = function (req, res, next) {
  function unauthorized(res) {
    res.set('WWW-Authenticate', 'Basic realm=Authorization Required');
    return res.sendStatus(401);
  };

  var user = basicAuth(req);
  console.log('Trying basic auth ' + JSON.stringify(user));

  if (!user || !user.name || user.name != config.apiKey) {
    return unauthorized(res);
  } else {
    return next();
  }
};

app.all('*', requireAuthentication);
app.use('/', require('./routes/index'));
app.use('/drivers', require('./routes/drivers'));
app.use('/users', require('./routes/users'));
app.use('/passengers', require('./routes/passengers'));

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
  app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
      message: err.message,
      error: err
    });
  });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
  res.status(err.status || 500);
  res.render('error', {
    message: err.message,
    error: {}
  });
});

console.log("Driver node server started...");
