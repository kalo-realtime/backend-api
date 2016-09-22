var express = require('express')
  , router = express.Router()
  , db = require('../models/database.js');

router.get('/:id', function(req, res, next) {
  var id = req.params.id;

  if (!id){
    console.error(err.stack);
    res.send('Authorization key not found or invalid', 422);
    return;
  }

  //this initializes a connection pool
  var query = 'SELECT * FROM users WHERE id = $1::int LIMIT 1;';
  //WHERE authentication_token="' + key + '"
  db.connection.connect(db.conString, db.queryHandler(query, [id], function(err, results){

    if(err) {
      res.status(500).send('Error occurred');
      return console.error('error running query', err);

    } else if (results.rows.length != 1){
      return res.status(401).send('Authentication failed');
      return;
    }

    return res.json(results.rows[0]);
  }));
});

module.exports = router;
