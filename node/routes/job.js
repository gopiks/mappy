/**
 * Handler for /job/*
 */
var DataFrame = require('dataframe-js').DataFrame;
var express = require('express');
var router = express.Router();
var api = require('../lib/api');
const MAX_ALLOWED_ERRORS = 10;

/**
 * Create a job
 */
router.post('/', function (req, res, next) {
  //res.send('respond with a resource');
  //output the request data
  console.log(req.body);

  //request form data
  var args = req.body;
  //request json data
  if (req.body.data) {
    args = req.body.data;
  }

  var _args = JSON.parse(args.args);

  var argumentsArray = []
  if (_args) {

    _args.forEach(function (value) {
      var _temp = {
        'status': 'stored',
        'result': null,
        'users': [],
        'err_count': 0,
        'arg': value
      };
      argumentsArray.push(_temp)
    });
  }


  var job = {
    func: args.func,
    reduce_func: args.reduce_func || null,
    args: argumentsArray,
    err_count: 0,
    status: 'waiting',
    user: null,
    job_name: args.job_name,
    resources: args.resources || []
  };

  api.save(job).then(result => {
    res.json(result);
  }).catch(next);
});

/**
 * Find one random Job which is not complete
 */
router.get('/', function (req, res, next) {
  var data = { 'status': { '$ne': 'completed' }, 'err_count': { '$lt': MAX_ALLOWED_ERRORS + 1 } };
  api.find(data, { "_id": 1 }).then(result => {
	var randomItem = result[Math.floor(Math.random()*result.length)];
	if (randomItem==undefined || randomItem==null) res.json(null);
    else res.json(randomItem['_id']);
  }).catch(next);
});

/**
 * For a particular job id, this resource gives a sub task to the processing node calling this function
 */
router.get('/:object_id', function (req, res, next) {
  //var data = {'_id':req.params.object_id};
  api.findById(req.params.object_id).then(result => {
	arg_id=result['args'].findIndex(x=>(x['status']!='completed'));
	    if (arg_id==null || arg_id==undefined || arg_id==-1) return null;
    arg=result['args'][arg_id]['arg'];
    result['args'][arg_id]['status']='submitted';
    
    
    //mongo.mappy.jobs.update({'_id':ObjectId(job['_id'])},{'$set':{'args':job['args'],'status': 'submitted'}})

	result['args']=[];
	result['arg_id']=arg_id;
	result['arg']=arg;
    res.json({'arg_id':arg_id,arg:arg,func:result['func'],job_name:result['job_name']});
  }).catch(next);
});

/**
 * Delete a job for admins
 */
router.delete('/:object_id', function (req, res, next) {
  var data = {'_id':req.params.object_id};
  api.remove(data).then(result => {
    res.json(result);
  }).catch(next);
});

/**
 * get the list of reources required for a job
 */
router.get('/:object_id/resources', function (req, res, next) {
  //var data = {'_id':req.params.object_id};
  api.findById(req.params.object_id).then(result => {
    res.json(result.resources);
  }).catch(next);
});

router.get('/resource/:resource', function (req, res, next) {

});

router.get('/:object_id/status', function (req, res, next) {
  //var condition = {'_id':req.params.object_id};
  api.findById(req.params.object_id).then(result => {

    var statuses = [];
    result.args.forEach(function (value) {
      var _temp = {
        'status': value.status
      }
      statuses.push(_temp);
    });
    const df = new DataFrame(statuses);
    var counts = {};

    df.groupBy('status').aggregate((group) => {
        group.toArray().forEach(function (value) {
        counts[value] = group.count();
      });
    });
    res.json({'status':result['status'],"error_count":result["err_count"],'counts':counts});
    
  }).catch(next);
});


router.post('/:object_id/result', function (req, res, next) {
  var condition = {'_id':req.params.object_id};

  //request form data
  var args = req.body;
  //request json data
  if (req.body.data) {
    args = req.body.data;
  }
  var arg_id = args.arg_id, _result = args.result;

  api.findOne(condition).then(result => {
    result.args[arg_id]['result'] = _result;
    result.args[arg_id]['status'] = 'completed';
	result.status='incomplete';
    var statuses = [];
    result.args.forEach(function (value) {
      if (value.status != 'completed') {
        statuses.push(value.status)
      }
      
      

      
    });
	if (statuses.length == 0) {
        result.status = 'completed'
      }
    api.update(condition, result).then(result => {
      res.json(result);
    }).catch(next);
    
  }).catch(next);
});

router.get('/:object_id/result', function (req, res, next) {
  var condition = {'_id':req.params.object_id};
  api.findOne(condition).then(result => {

    /*if (result.status != 'completed') {
      res.json(null)
    } */
    var _result = [];
    result.args.forEach(function (value) {
		if(value.status=='completed')
      _result.push(value.result)
      
    });
    var reduce_func = result.reduce_func;
    //I can't find where is the reduce function defination ....
    if (reduce_func) {
		console.log(reduce_func);
      _result = _result.reduce(eval(reduce_func));
    }
    api.update(condition, result).then(result => {
      res.json(_result);
    }).catch(next);
    
  }).catch(next);
});

router.post('/:object_id/error', function (req, res, next) {
  var condition = {'_id':req.params.object_id};

  //request form data
  var args = req.body;
  //request json data
  if (req.body.data) {
    args = req.body.data;
  }

  var arg_id = args.arg_id;

  api.findOne(condition).then(result => {

    if (result.status == 'completed') {
      res.json({'message': 'success'})
    }

    if (result['args'][arg_id]['status'] != 'completed') {
      result['args'][arg_id]['status'] = 'error';
      var count = result['err_count'] || 0;
      result['err_count'] = count + 1;
      var _count = result['args'][arg_id]['err_count'] || 0;
      result['args'][arg_id]['err_count'] = _count + 1;

      result['status'] = "errors:" + result['err_count'];
      api.update(condition, result).then(result => {
        res.json({'message': 'success', 'result': result})
      }).catch(next);
    }

    res.json({'message': 'success'})
  }).catch(next);
});
module.exports = router;
