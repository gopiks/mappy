/**
 * Database interaction for job
 */

var JobModel = require('../models/job');
module.exports = {
	/**
	 * add job
	 * @param  {[type]} data the data that need to create
	 */
  save(data) {
    return new Promise((resolve, reject) => {
      //model.create(data,callback)
      JobModel.create(data, (error, doc) => {
        if (error) {
          reject(error)
        } else {
          resolve(doc['_id'])
        }
      })
    })
  },
  find(data = {}, fields = null, options = {}) {
    return new Promise((resolve, reject) => {
      //model.find(the object that need to find (if empty then find all data), property[optional], options[optional], callback)
      JobModel.find(data, fields, options, (error, doc) => {
        if (error) {
          reject(error)
        } else {
          resolve(doc)
        }
      })
    })
  },
  findOne(data) {
    return new Promise((resolve, reject) => {
      //model.findOne(the data object that need to find,callback)
      JobModel.findOne(data, (error, doc) => {
        if (error) {
          reject(error)
        } else {
          resolve(doc)
        }
      })
    })
  },
  findById(data) {
    return new Promise((resolve, reject) => {
      //model.findById(the data object's id that need to find ,callback)
      JobModel.findById(data, (error, doc) => {
        if (error) {
          reject(error)
        } else {
          resolve(doc)
        }
      })
    })
  },
  update(conditions, update) {
    return new Promise((resolve, reject) => {
      //model.update(query condition,the data need to update,callback)
      JobModel.update(conditions, update, (error, doc) => {
        if (error) {
          reject(error)
        } else {
          resolve(doc)
        }
      })
    })
  },
  remove(conditions) {
    return new Promise((resolve, reject) => {
      //model.remove(query condition,callback)
      JobModel.remove(conditions, (error, doc) => {
        if (error) {
          reject(error)
        } else {
          resolve(doc)
        }
      })
    })
  }
}