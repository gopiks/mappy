# -*- coding: utf-8 -*-
"""
Created on Tue Feb 06 19:56:30 2018

@author: Gopi
"""

from pymongo import MongoClient
from numpy import random
import pandas as pd
from bson.objectid import ObjectId
min_redundency=1
max_allowed_errors=10
#pip install 'pubnub>=4.0.13'


## gets a function and arguments, stores in the database and returns the id
def map_job(func,args,reduce_func=None,user=None,job_name=None,resources=[]):
    # save the functions and args in a db
    # give a job id
    job={'func':func,"reduce_func":reduce_func,'args':[{'arg':arg,'status':'stored','result':None,'users':[],'err_count':0} for arg in args],'err_count':0,'status':'waiting','user':user,'job_name':job_name,'resources':resources}
    mongo=MongoClient()
    x=mongo.mappy.jobs.insert_one(job)
    jobid=str(x.inserted_id)
    return jobid

## Gets a random unfinshed job from the database
def get_job():
    mongo=MongoClient()
    ### jobs should not be taken in memory.. this needs to change   
    jobs=mongo.mappy.jobs.find({'status':{'$ne':'completed'},'err_count':{'$lt':max_allowed_errors+1}},{"_id":1})
    num_jobs=jobs.count()
    if num_jobs==0:
        return None
    job=jobs[random.randint(0,num_jobs)]
    return str(job['_id'])

## Deletes a job
def delete_job(job_id,user):
    result=mongo.mappy.jobs.remove({"_id":ObjectId(job_id),"user":user})
    return result

## for a job id gets a random incomplete sub-task from the database
def get_sub_task(job_id,user):
    mongo=MongoClient()    
    job=mongo.mappy.jobs.find_one({'_id':ObjectId(job_id)})
    arg_id=next( (x for x in range(0,len(job['args'])) if job['args'][x]['status']!='completed' and len(job['args'][x]['users'])<min_redundency), None)
    if arg_id==None:
        arg_id=next( (x for x in range(0,len(job['args'])) if job['args'][x]['status']!='completed'), None)
        if arg_id==None:
            return None
    arg=job['args'][arg_id]['arg']
    job['args'][arg_id]['status']='submitted'
    job['args'][arg_id]['users'].append(user)
    job_name=job['job_name'] if 'job_name' in job else None
    mongo.mappy.jobs.update({'_id':ObjectId(job['_id'])},{'$set':{'args':job['args'],'status': 'submitted'}})
    return {'func': job['func'], 'arg': arg, 'job_id':str(job['_id']),'arg_id':arg_id,'job_name':job_name}

## get the list of reources required for a job
def get_resource_list(job_id):
    mongo=MongoClient()    
    job=mongo.mappy.jobs.find_one({'_id':ObjectId(job_id)})
    resources = job['resources']
    return resources    
    
### staores the result of a sub-task for a job in the database
def post_result(job_id,arg_id,response):
    mongo=MongoClient()
    
    job=mongo.mappy.jobs.find_one({'_id':ObjectId(job_id)})
    job['args'][arg_id]['result']=response
    job['args'][arg_id]['status']='completed'
    statuses=[x['status'] for x in job['args'] if  x['status']!='completed']
    print ObjectId(job_id),job_id,arg_id,job
    if len(statuses)==0:
        job['status']='completed'
    mongo.mappy.jobs.update({'_id':job['_id']},job) 
    return True

## gives the status of a job from the database
def job_status(job_id):
    mongo=MongoClient()
    job=mongo.mappy.jobs.find_one({'_id':ObjectId(job_id)})
    statuses=[{'status':x['status']} for x in job['args']]
    counts=pd.DataFrame(statuses).groupby('status')['status'].count().to_json()
    return {'status':job['status'],"error_count":job["err_count"],'counts':counts }
    
## gives final result of a job. If it is not complere None is returned
def job_result(job_id):
    mongo=MongoClient()
    job=mongo.mappy.jobs.find_one({'_id':ObjectId(job_id)})
    if job['status']!='completed':
        return None
    result=[x['result'] for x in job['args'] ]
    reduce_func=job['reduce_func']
    if reduce_func:
        result=reduce(eval(reduce_func),result)
    return result

## stores in DB that an error was encountered
def job_error(job_id,arg_id,user):
    mongo=MongoClient()
    job=mongo.mappy.jobs.find_one({'_id':ObjectId(job_id)}) 
    if job['status']=='completed':
        return True
    if job['args'][arg_id]['status']!='completed':   
        job['args'][arg_id]['status']='error'    
        job['err_count']=job['err_count']+1 if 'err_count' in job else 1
        job['args'][arg_id]['err_count']=job['args'][arg_id]['err_count']+1 if 'err_count' in job['args'][arg_id] else 1
        job['status']="errors:"+str(job['err_count'])    
        mongo.mappy.jobs.update({'_id':ObjectId(job['_id'])},job) 
    return True
    
if __name__=="__main__":
    now=pd.datetime.now()
    sum_=0
    import numpy as np
    for i in range(0,1000000):
        rnd=np.exp(np.random.random()*0.1+0.1)*100
        sum_=sum_+(rnd-100 if(rnd>100) else 0)
        
    sum_=sum_/1000000
    print pd.datetime.now()-now
    print sum_
    
    