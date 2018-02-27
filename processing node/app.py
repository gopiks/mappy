# -*- coding: utf-8 -*-
"""
Created on Tue Feb 06 19:56:30 2018

@author: Gopi

This file has routing for Python flask app. 
It provides access to mappy fucnctions through http.
"""
import traceback

from flask import Flask,send_from_directory,request
app = Flask(__name__)
import map as mp
import json
from flask_cors import CORS,cross_origin


## Basic favicon for static html files
@app.route("/favicon.ico")
def favicon():

    try:
        return send_from_directory("static/img/","icon.png")
    except:
        print traceback.format_exc()    
    
## Nothing much here.. just a logo and links
@app.route("/")
def hello():
    import stun
    print stun.get_ip_info("0.0.0.0",5000)
    return send_from_directory("static/","index.html")
    
## /job is the main resource. POST method is used for posting a new job
## GET method is used for getting a new job from the server by the nodes.
@app.route("/job",methods=['POST'])
def post_job():

    try:
        print request.args
        data = request.get_json(silent=True)
        args= json.loads(request.data) if data is not None else request.form
        #files=request.files
        print request.form,request.data,data
        func=args['func']
        reduce_func=args['reduce_func'] if 'reduce_func' in args else None
        job_name=args['job_name'] if 'job_name' in args else None
        resources=args['resources'] if 'resources' in args else []
        args=json.loads(args['args'])
        return json.dumps(mp.map_job(func,args,reduce_func,None,job_name,resources=resources)) 
    except:
        print traceback.format_exc()
@app.route("/job",methods=['GET'])
@cross_origin()
def get_job():
    try:
        return json.dumps(mp.get_job()) 
    except:
        print traceback.format_exc()
        
## For a praticular job id, this resource gives a sub task to the processing node calling this function
@app.route("/job/<job_id>",methods=['GET'])
@cross_origin()
def get_sub_task(job_id):
    try:
        return json.dumps(mp.get_sub_task(job_id,None)) 
    except:
        print traceback.format_exc()

## This is for admins to delete a job. Will not be used right now
@app.route("/job/<job_id>",methods=['DELETE'])
def delete_job(job_id):
    try:
        return json.dumps(mp.delete_job(job_id,None)) 
    except:
        print traceback.format_exc()

## This gives list of resources (files to be imported for running computation)
@app.route("/job/<job_id>/resources")        
@cross_origin()
def get_resource_list(job_id):
    try:
        x= mp.get_resource_list(job_id)
        return json.dumps(x)         
    except:
        print traceback.format_exc()
        return "not found",404

## Some .js libraries will be stored in static/lib folder. This method will give access to them
@app.route("/resource/<resource>")        
@cross_origin()
def get_resource(resource):
    return send_from_directory("static/lib/",resource)

## This method is used for getting status of a job and sub-tasks
@app.route("/job/<job_id>/status")
def job_status(job_id):
    try:
        x= mp.job_status(job_id)
        return json.dumps(x) 
    except:
        print traceback.format_exc()
        return "not found",404

### /job/<job_id>/result is for accessing result of a job
### POST method is used by processing node to post the result after computation of a sub-task
### GET method is used by job node to access final result. If all sub-tasks are not completed it will return null
@app.route("/job/<job_id>/result",methods=['POST'])
@cross_origin()
def post_result(job_id):
    try:
        data = request.get_json(silent=True)
        args= json.loads(request.data) if data is not None else request.form
        files=request.files
        
        result=args['result']
        arg_id=int(args['arg_id'])

        x= mp.post_result(job_id,arg_id,result)
        return json.dumps(x) 
    except:
        print traceback.format_exc()

@app.route("/job/<job_id>/result",methods=['GET'])
def job_result(job_id):
    try:
        x= mp.job_result(job_id)
        return json.dumps(x) 
    except:
        print traceback.format_exc()

### This method is for posting error to a sub-task
@app.route("/job/<job_id>/error",methods=['POST'])
@cross_origin()
def job_error(job_id):
    try:    
        data = request.get_json(silent=True)
        args= json.loads(request.data) if data is not None else request.form
        arg_id=int(args['arg_id'])
        mp.job_error(job_id,arg_id,None)
        return "success"
    except:
        print traceback.format_exc()    

        
if __name__=="__main__":
    #pass
    app.run(host="0.0.0.0")