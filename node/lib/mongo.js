
var mongoose = require("mongoose");
const DB_URL = 'mongodb://localhost:27017/mappy';
var db = mongoose.createConnection(DB_URL);

db.on('connected', function(error){
  if (error) {
    console.log('connect to dbname failed：' + error)
    return
  }
})

module.exports = db