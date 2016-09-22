var express = require('express')
  , router = express.Router()
  , db = require('../models/database');


router.get('/:key', function(req, res, next) {
  var id = req.params.key,
      id_type = req.query.id_type,
      auth_string;

  if (!id){
    console.error(err.stack);
    res.send('Authorization key not found or invalid', 422);
    return;
  }

  //passenger fetch using token or user id
  if (id_type == 'token'){
    auth_string = 'authentication_token = $1';
  }else{
    auth_string = 'hash_key = $1';
  }

  var query = 'SELECT * FROM passengers WHERE ' + auth_string + ' LIMIT 1;';

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
