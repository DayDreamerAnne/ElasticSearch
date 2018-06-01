from flask import Flask,render_template,make_response,redirect,url_for,session,flash,request

import math
import csv
from elasticsearch import Elasticsearch
import pandas as pd

INDEX_JOBS= 'testjospostinginserts'
doc_jobs='jobposting'
INDEX_RESUMES='resume'
doc_resume = 'stu_resume' ##the dosc_type of resume table
doc_recomends = 'stu_recomends'
INDEX_RECOMDS = 'recomends'

es = Elasticsearch()
sizePage = 20

class ES:
    def __init__(self):
        self.index=None
        self.doc_type=None
        self.body=None

    def create_index(self,es, IndexName):
        if es.indices.exists(index=IndexName):
            es.indices.delete(index=IndexName)
            res = es.indices.create(index=IndexName)
        else:

            res = es.indices.create(index=IndexName)
        return res

    def insert_into_es(es, dict_data, IndexName, doc):
        if es.indices.exists(index=IndexName):
            if len(dict_data) > 2:
                keys_id = list(dict_data.keys())
                for ids in keys_id:
                    res = es.index(index=IndexName, doc_type=doc, id=ids, body=dict_data[ids])
            else:
                res = es.index(index=IndexName, doc_type=doc, id=dict_data['student_name'], body=dict_data)
            return res
        else:
            create_index(es, IndexName)
            res = insert_into_es(dict_data, IndexName)
            return res
es = Elasticsearch()

def create_index(es,IndexName):
    if es.indices.exists(index=IndexName):
        es.indices.delete(index=IndexName)
        res = es.indices.create(index=IndexName)
    else:
        res=es.indices.create(index=IndexName)
    return res

def insert_into_es(es,IndexName,doc,dict_data):
    if es.indices.exists(index=IndexName):
        if len(dict_data)>2:
            # insert recomendations
            keys_id = list(dict_data.keys())
            for ids in keys_id:
                res = es.index(index=IndexName, doc_type=doc, id=ids, body=dict_data[ids])
        else:
            #store resume
            res = es.index(index=IndexName, doc_type=doc, id=dict_data['student_name'], body=dict_data)
        return res
    else:
        create_index(es,IndexName)
        res = insert_into_es(es,IndexName,doc,dict_data)
        return res

def search_existing(es,IndexName,doc,keywords):
    #search by keywords
    if es.indices.exists(index=IndexName):
        if keywords:
            res = es.search(index=IndexName, doc_type=doc,body={"query": {"term": {"student_name": keywords}}})
            total = res['hits']['total']  # get the total number of jobs that match query
            if total > 0:
                return True
            else:
                return False
        else:
            res = es.search(index=IndexName, doc_type=doc, body={"query": {"match_all": {}}})
            total = res['hits']['total']  # get the total number of jobs that match query
            return True


    else:
        return False

def get_data(es,IndexName,doc,keywords):
    # search by keywords
    resume=''
    #dict_index={'recomends':1,'resume':2,'job':3}
    if IndexName =='recomends':
        res = es.search(index=IndexName, doc_type=doc, body={"size": sizePage,
                                                             "sort": [{"sim_score": "desc"}],
                                                             "query": {"bool": {"must": {"match": {"student_name": keywords}}}}}, scroll='1m')
        total = res['hits']['total']  # get the total number of jobs that match query
        # only one version of resume in elasticsearch
        if total > 1:
            count = math.ceil(total / sizePage)
            list_searched = []  # contains related job posting based on keywords search
            for round in range(0, count):
                for hit in res['hits']['hits']:
                    list_temp = {}
                    for data in hit["_source"]:
                        # hit["_source"]: the real data wrapped in json
                        list_temp[data] = hit['_source'][data]
                    list_searched.append(list_temp)
                # use scroll to get the following records
                scroll = res['_scroll_id']
                res = es.scroll(scroll_id=scroll, scroll='1m')
            return list_searched

    elif IndexName == 'resume':
        resumes=[]
        if keywords:
            per_page = sizePage
            res = es.search(index=IndexName, doc_type=doc, body={"size": sizePage, "query": {"term": {"student_name": keywords}}},scroll='1m')
            for hit in res['hits']['hits']:
                resume = hit['_source']['resume']
            return resume
        else:
            per_page = 1000
            res = es.search(index=IndexName, doc_type=doc,body={"size": 100, "query": {"match_all": {}}},scroll='1m')
            for i in range(int(res['hits']['total'] / 1000) + 1):
                for hit in res['hits']['hits']:
                    header = list(hit['_source'].keys())
                    list_temp = []
                    for data in hit["_source"]:
                        list_temp.append(hit['_source'][data])
                    resumes.append(list_temp)
                    # use scroll to get the following records
                scroll = res['_scroll_id']
                res = es.scroll(scroll_id=scroll, scroll='1m')


    elif IndexName == INDEX_JOBS:
        jobs = []
        if keywords:
            per_page=sizePage
            res = es.search(index=IndexName, doc_type=doc, body={"size":sizePage,"query": {"term": {"student_name": keywords}}},scroll='1m')
        else:
            per_page = 1000
            res = es.search(index=IndexName, doc_type=doc,body={"size":1000,"query": {"match_all": {}}},scroll='1m')
        i=0
        for i in range(int(res['hits']['total']/per_page)+1):
            for hit in res['hits']['hits']:
                header = list(hit['_source'].keys())
                list_temp = []
                for data in hit["_source"]:
                    list_temp.append(hit['_source'][data])
                jobs.append(list_temp)
                # use scroll to get the following records
            scroll = res['_scroll_id']
            res = es.scroll(scroll_id=scroll, scroll='1m')
        # convert records into a dataframe
        df = pd.DataFrame.from_records(data=jobs, columns=header)
        return df

def delete_index(es,IndexName):
    if es.indices.exists(index=IndexName):
        res=es.indices.delete(IndexName)
        return res
    else:
        return 'no such index'

def read_csv(file):
    dict_data = {}
    with open(file,'r') as csvfile:
        reader = csv.reader(csvfile)
        header=next(reader)
        i = 0
        # reader = csv.DictReader(csvfile, field_name)
        for row in reader:
            dict_inner = {}
            for item in range(len(header)):
                dict_inner[header[item]]=row[item]
            dict_data[i]=dict_inner
            i+=1
    return dict_data

#read file and insert into elasticsearch
file_path = "/Users/Faye/Downloads/indeed_raw_data.csv"
dic_data=read_csv(file_path)
insert_into_es(es,'testjospostinginserts','jobposting',dic_data)

""" 
          for hit in res['hits']['hits']:
              header = list(hit['_source'].keys())
              list_temp = []
              for data in hit["_source"]:
                  list_temp.append(hit['_source'][data])
              resumes.append(list_temp)
              # use scroll to get the following records
          scroll = res['_scroll_id']
          res = es.scroll(scroll_id=scroll, scroll='1m')
          # convert records into a dataframe
          df = pd.DataFrame.from_records(data=resumes, columns=header)
          return resumes
          """