/**
 * model definition
 */
var mongoose = require("mongoose");
var db = require('../lib/mongo');
// reference a job
var JobSchema = new mongoose.Schema({
	func    : { type:String },
	reduce_func    : {type: String},
	args      : {type: Array},
	err_count         : { type:Number, default:0 },
	status : { type: String},
	user       : { type: Object },
	job_name      : { type: String },
	resources        : { type:Array, default:[]}
});

//Create Model
var JobModel = db.model("jobs",JobSchema);
module.exports = JobModel