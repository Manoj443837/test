

# manin.py

# exec(open('C:/Projects/FCA/Codes/Repo/EmissionLogic/src/1_Inputs.txt').read())
# from cgi import parse_multipart
# from multiprocessing.util import LOGGER_NAME
# import sys
# from unittest import FunctionTestCase
# sys.path.insert(0, "C:/Projects/FCA/Codes/Repo/EmissionLogic/src/")
# from Environment_Setup import *
# logger.info('========Environment Setup======')
# ========================================

# env.setup


# def install_and_import(package):
#     import importlib
#     try:
#         importlib.import_module(package)
#     except ImportError:
#         from pip._internal import main as pipmain
#         pipmain(['install', package])
#     finally:
#         globals()[package] = importlib.import_module(package)

# results = map(install_and_import, ('json','os','sys','pandas','warnings','logging','datetime', 'numpy','timeit',
#                                    'copy','scipy','sklearn','sqlalchemy'))
# set(results)

# from timeit import default_timer as timer
# import pandas as pd
# warnings.filterwarnings('ignore')
# from datetime import datetime
# import numpy as np
# from sqlalchemy import create_engine


# ### User Defined Functions
# def read_db(table_name, connection_string, schema= None, cols = None):
#     con = create_engine(connection_string)
#     table = pd.read_sql_table(table_name, con,columns= cols, schema=schema)
#     return table

# def read_db_query(table_name, filter_string, connection_string):
#     con = create_engine(connection_string)
#     table = pd.read_sql_query("SELECT * FROM %s WHERE %s;" % (table_name, filter_string), con)
#     return table

# def read_db_schema(table_name, connection_string):
#     con = create_engine(connection_string)
#     table = pd.read_sql_query("SELECT * FROM %s ;" % (table_name), con)
#     return table

# def write_db(data, table_name, connection_string):
#     con = create_engine(connection_string)
#     data.to_sql(name=table_name, con=con, if_exists='replace', index=False)

# def write_db_schema(data, table_name,schema, connection_string):
#     con = create_engine(connection_string)
#     data.to_sql(name=table_name, con=con, schema=schema, if_exists='replace', index=False)


# # ### Set up log file
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# # create a file handler
# handler = logging.FileHandler('Log_file_'+str(datetime.now().strftime('%Y-%m-%d-%H%M%S'))+'.log')
# handler.setLevel(logging.INFO)

# # create a logging format
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)

# # add the handlers to the logger
# logger.addHandler(handler)

# logger.info('Start')








# =-====================================================================================
#  app.py


# def install_and_import(package):
#     import importlib
#     try:
#         importlib.import_module(package)
#     except ImportError:
#         from pip._internal import main as pipmain
#         pipmain(['install', package])
#     finally:
#         globals()[package] = importlib.import_module(package)

# results = map(install_and_import, ('flask', 'os', 'sys', 'tablib','pandas','json', 'scipy', 'sklearn', 'sqlalchemy'))
# set(results)

# import pandas as pd
# from flask import Flask, request, jsonify, render_template

# app = Flask(__name__)
# app.config['JSON_AS_ASCII'] = False

# # User defined Functions

# def get_current_dir():
#    return os.path.dirname(os.path.realpath(__file__))

# # Change possible windows path to unix-like paths
# def get_current_dir_unixy():
#    return get_current_dir().replace('\\', '/')

# def get_working_dir():
#     working_dir = get_current_dir_unixy()
#     if not sys.platform.startswith('win'):
#         working_dir = working_dir + '/'
#     return working_dir


# working_dir = get_working_dir()
# # exec(open('C:/Projects/FCA/Codes/Repo/EmissionLogic/src/1_Inputs.txt').read())
# inputfile = open(working_dir+"/1_Inputs.txt")
# exec(inputfile.read(), globals())

# sys.path.insert(0, working_dir)
# from Initialize import *
# from Compute_co2 import *
# from Compute_co2_rerun import *
# from Reporting_Linearize import *
# from Reporting_Combination_Matrix import *

# ===============================================================





# =======================================================================




# ++++++++++++++++++++++++++++++++++

# API LOGGER_NAME

# import logging
# from logging.handlers import RotatingFileHandler


# class BCGApiLogger(object):
#     """ This is BCGApiiLogger class , to catch the error
#         while accessing the bulk API
#     """

#     def __init__(self):
#         self.formatter = logging.Formatter("[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
#         self.logHandler = RotatingFileHandler('info.log', mode='a', maxBytes=0, backupCount=0, encoding=None,
#                                               delay=False)
#         self.logHandler.setLevel(logging.INFO)
#         self.logHandler.setFormatter(self.formatter)
#         self.info = logging.INFO
#         self.error = logging.ERROR





# ===========================================================================================================================================

# config parmams.




# import json
# import os


# class ConfigFile(object):

#     def __init__(self, prop_file_path, service_name=''):
#         self.prop_file_name = str(service_name) + "_prop.json"
#         self.configuration_file_path = os.path.join(prop_file_path, self.prop_file_name)

#     def get_keys_from_prop_file(self):
#         """To retrieve the property file to get the configuration

#         """
#         try:
#             with open(self.configuration_file_path) as json_data:
#                 prop_dict = json.load(json_data)
#         except IOError:
#             print("Property File Not Found!!")
#             return {}
#         return prop_dict









# =========================================================================================================







# sample data movement and FunctionTestCas


# import requests
# import json
# import sys, os
# from simple_salesforce import Salesforce
# from flask import jsonify
# import pandas as pd
# import fileinput
# import boto
# import boto.s3
# from boto.s3.key import Key
# from time import gmtime, strftime

# """ OutBoundData class in progress
# """


# class OutBoundData(object):

#     def __init__(self, app=None):
#         self.sf_config_file = "login_config.json"
#         self.outbound_config_file = "outbound_config.json"
#         if app:
#             self.app = app
#             self.store_engine = app.config['ENGINE']

#     def salesforce_prop(self):
#         """To retrieve the property file to get the configuration

#         """
#         config_file = os.path.join(os.path.join(os.getcwd(),'Config'),self.sf_config_file)
#         try:
#             with open(config_file) as json_data:
#                 prop_dict = json.load(json_data)
#         except IOError:
#             self.app.logger.error("Config File Not Found!!")
#             return {}
#         return prop_dict

#     def outbound_prop(self):
#         """To retrieve the property file to get the configuration

#         """
#         config_file = os.path.join(os.path.join(os.getcwd(),'Config'),self.outbound_config_file)
#         try:
#             with open(config_file) as json_data:
#                 prop_dict = json.load(json_data)
#         except IOError:
#             self.app.logger.error("Config File Not Found!!")
#             return {}
#         return prop_dict

#     def get_acc_token_with_url(self):
#         """ To send post request to salesforce login to generate the access token
#         """
#         try:
#             sp = self.salesforce_prop()
#             token_response = requests.post(sp.get('login_url'), headers=sp.get('header_type'), data=sp.get('payload'))
#             instance_url = token_response.json()['instance_url']
#             body = json.loads(token_response.content)
#             token = body["access_token"]
#             sf = Salesforce(instance_url=instance_url, session_id=token)
#             return sf
#         except requests.exceptions.RequestException as e:
#             self.app.logger.error("Getting the Error Code : {0}".format(e))
#             sys.exit(1)


#     def create_outbound_data(self):
#         """To pull the data from SF and stored into SQl table

#         """
#         try:
#             sp = self.outbound_prop()
#             boh_tables = sp.get('boh_tables')
#             sf_query = self.get_acc_token_with_url()
#             self.app.logger.info('sf_query............{}'.format(sf_query))

#             for table_name in boh_tables:
#                 sf_query_to_execute = sp.get(table_name)
#                 # records = sf_query.query("SELECT Version_Code__c, Price__c, Id, PriceBook_Id__c FROM Price_Book_Scenario_Version__c WHERE PriceBook_Id__c = 'a00f400000C0h6U'")
#                 records = sf_query.query(sf_query_to_execute)
#                 data_str = json.dumps(records.get('records'))
#                 # convert sql data str to pandas dataframe
#                 data_frame = pd.read_json(data_str)
#                 del data_frame['attributes']
#                 data_frame.to_sql(name=table_name, con=self.store_engine, if_exists='append', index=False)

#             self.app.logger.info('Data Pushed to SQL successfully')
#             return jsonify({"records": "Data Pushed to SQL successfully"})
#         except Exception as e:
#             self.app.logger.error('Create OutBound Date Exception Error: {}'.format(e))
#             return jsonify({"error": "Exception Error"})

#     def get_outbound_file(self):
#         try:
#             sp = self.outbound_prop()
#             outbound_file = sp.get('outbound_file')+strftime("%Y%m%d%H%M%S", gmtime())+ ".txt"
#             ### write a code for existing file
#             boh_tables = sp.get('boh_tables')
#             for table_name in boh_tables:
#                 outbound_df = pd.read_sql("select * from {}".format(table_name), con=self.store_engine)
#                 outbound_df.to_csv(outbound_file, header=None, index=None, sep='|', mode='a')
#             with fileinput.FileInput(outbound_file, inplace=True) as file:
#                 for line in file:
#                     print(line.replace('|',''), end='')
#             return outbound_file
#         except Exception as e:
#             self.app.logger.error('Get Outbound File Exception Error: {}'.format(e))
#             return jsonify({"error": "Exception Error"})

#     def save_output_to_s3(self):
#         try:
#             outbound_file = self.get_outbound_file()
#             sp = self.salesforce_prop()
#             AWS_ACCESS_KEY_ID = sp.get('AWS_ACCESS_KEY_ID')
#             AWS_SECRET_ACCESS_KEY = sp.get('AWS_SECRET_ACCESS_KEY')
#             bucket_name = sp.get('bucket_name')

#             conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
#             bucket = conn.create_bucket(bucket_name,location=boto.s3.connection.Location.DEFAULT)


#             self.app.logger.info('Uploading %s to Amazon S3 bucket %s' % (outbound_file, bucket_name))

#             def percent_cb(complete, total):
#                 sys.stdout.write('.')
#                 sys.stdout.flush()

#             k = Key(bucket)
#             k.key = 'outbound_temp/'+outbound_file
#             k.set_contents_from_filename(outbound_file,
#                                          cb=percent_cb, num_cb=10)
#             if k:
#                 os.remove(outbound_file)

#             return jsonify({"success": "File created on S3"})
#         except Exception as e:
#             self.app.logger.error('Save FIle Exception Error: {}'.format(e))
#             self.app.logger.error('Removing Generated FIle.... {}'.format(outbound_file))
#             os.remove(outbound_file)
#             return jsonify({"error": "Exception Error"})
