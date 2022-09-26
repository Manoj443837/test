
"""
bapi
"""
from flask import Flask, render_template, jsonify, request
import os
from api_logger import BCGApiLogger
import sys
from sf_sync import SalesforceSync
import sqlalchemy
from sqlalchemy import create_engine
import yaml

app = Flask(__name__)
log_ = BCGApiLogger()
app.logger.setLevel(log_.info)
app.logger.addHandler(log_.logHandler)


def get_url(db_config):
    return "mssql+pymssql://{}:{}{}/{}" \
        .format(db_config["user_name"],
                db_config["password"],
                db_config["host"],
                db_config["db_name"])



def connect_db():
    """
    config_file = os.getenv('CONFIG_FILE_PATH')
    app.logger.error(config_file)
    with open(config_file, 'rb') as yaml_content:
        app.config.update(yaml.load(yaml_content))
    database_uri = get_url(app.config["db"])
    """
    #database_uri = "mssql+pymssql://somnath.majumder@niit-tech.com:Ghftlk76sh6@sql.msppricingcatalyst-dev.bcgtools.com/dev_tenant14"
    database_uri = "mssql+pymssql://fca_bulk_api_user:xnzp7G8q706aaoIg@sql.msppricingcatalyst-dev.bcgtools.com/dev_tenant14"
    app.logger.info('database...{0}'.format(database_uri))
    if database_uri:
        engine = create_engine(database_uri, echo=True)
        app.config.update(dict(
            DATBASE_ENGINE=engine
        ))
        metadata = sqlalchemy.MetaData()
        metadata.bind = engine
        app.logger.info("Database successfully logged in ")
        return engine.connect()
    else:
        app.logger.info("No database to connect to")
        sys.exit(0)

app.config.update(dict(
    GET_DB=connect_db()
))

prop_file_path = sys.argv[1]
log_file = os.path.join(prop_file_path, 'info.log')

"""App takes service name and one input as parameter and can deliver 
    the complete content in json.
"""


@app.route('/controller/bcg_bulk_api/<string:service_name>')
def bcg_bulk_api(service_name):
    """ To pull data from sql server and insert into salesforce
    """
    batch_id = request.args.get('BatchID')
    key = 'BatchID' if batch_id else 'BatchRefreshDate'
    key_value = request.args.get(key)
    app.logger.info("service_name......{0}".format(service_name))
    sf_obj = SalesforceSync(prop_file_path, service_name)
    res = sf_obj.sf_add_rest(app, key, key_value)
    return res

@app.route("/hello")
def hello():
    """
    config_file = os.getenv('CONFIG_FILE_PATH')
    app.logger.error(config_file)
    with open(config_file, 'rb') as yaml_content:
        app.config.update(yaml.load(yaml_content))

    dburl = get_url(app.config["db"])
    """
    dburl="mssql+pymssql://fca_bulk_api_user:xnzp7G8q706aaoIg@sql.msppricingcatalyst-dev.bcgtools.com/dev_tenant14"
    app.logger.error(dburl)
    return "<h1 style='color:blue'>{}</h1>".format(dburl)


@app.route('/health')
def health():
    return jsonify({'status': 'RUNNING'}), 200

@app.errorhandler(404)
def page_not_found(error):
    app.logger.error('Page not found')
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(error):
    app.logger.error('Internal Server Error: %s', (error))
    return render_template('500.html'), 500


if __name__ == '__main__':
    """Invoke the application
    """
    app.run(debug=True, host="0.0.0.0", port=5000)



============================================================================================================================================

ssync
import requests
import json
import sys
from flask import jsonify
from get_confg_param import ConfigFile
from simple_salesforce import Salesforce
import pandas as pd
import pandas.io.sql as psql

""" SalesforceOAuth2Client class generates the access token and inserts
    the data with this token into salesforce Account API
"""


class SalesforceSync(object):

    def __init__(self, prop_file_path, service_name):
        self.prop_file_path = prop_file_path
        self.service_name = service_name

    def get_access_token(self):
        """ To send post request to salesforce login to generate the access token
        """
        try:
            token_response = requests.post(self.login_url, headers=self.headers, data=self.payload)
            instance_url = token_response.json()['instance_url']
            body = json.loads(token_response.content)
            token = body["access_token"]
            return token, instance_url
        except requests.exceptions.RequestException as e:
            print("Getting the Error Code : {0}".format(e))
            sys.exit(1)

    def sf_login_response(self, prop_dict):
        access_token_url = prop_dict.get('login_url')
        data = prop_dict.get('payload')[0]
        headers = prop_dict.get('header_type')[0]
        req = requests.post(access_token_url, data=data, headers=headers)
        response = req.json()
        return response

    def sf_add_rest(self, app, key, key_value):
        config_obj = ConfigFile(self.prop_file_path, self.service_name)
        prop_dict = config_obj.get_keys_from_prop_file()
        schema = prop_dict.get('schema')  ##TODO
        view_cols = prop_dict.get(self.service_name+"_view_cols")  ##TODO
        # try:
        #db_connection = app.config['GET_DB']
        engine = app.config['DATBASE_ENGINE']
        cs = prop_dict.get('chunk_size')
        view_name = self.service_name + "__c"
        #db_table = schema + "."+self.service_name
        # create_view_query = "create view {} as SELECT '{}' as [attributes.type], newid() as [attributes. referenceId], {} from {};".format(view_name, view_name,
        #     view_cols, db_table)
        # db_connection.execute(create_view_query)
        #app.logger.info("View Created and view query is : {0}".format(create_view_query))
        db_query = "SELECT * from {}  where BatchId__c={}".format(view_name, key_value)
        # app.logger.info("db_query ........ .Database Query: {0}".format(db_query))
        # db_data = db_connection.execute(db_query)
        # print("json obj...............{}".format(db_data))
        # data = [dict(r) for r in db_data]
        # print("data.............{}".format(data))
        # return json.dumps(data)
        for df_chunk in pd.read_sql_query(db_query, engine, chunksize=cs):
            # col_names = [map_fields.get(c) if map_fields.get(c) else c for c in list(df_chunk)]
            # df_chunk.columns = col_names
            dj = df_chunk.to_json(orient='records')
            db_rs = json.loads(dj.replace('"{','{').replace('}"','}').replace('\\',''))
            return jsonify({'records': db_rs})


        # except Exception as e:
        #     app.logger.error("Failed ........ .Error: {0}".format(str(e)))
        #     return "Failed ........ .Error Message: {0}".format(str(e))

    def bcg_add_rest(self, access_token, data_table):
        pass

        """ To Fetch Data from Account and insert into sql server acc table : TODO
        """

        # sf = Salesforce(instance_url=self.get_access_token()[1], session_id=self.get_access_token()[0])
        # records = sf.query("SELECT Name, AccountNumber,  FROM Account") ##
==========================================================================================================================


sd_utls



import requests
import json
import sys, os, time
from simple_salesforce import Salesforce
from flask import jsonify
import pandas as pd
import fileinput
import boto
import boto.s3
from boto.s3.key import Key
from time import gmtime, strftime

""" OutBoundData class in progress
"""


class OutBoundData(object):

    def __init__(self, app=None):
        self.sf_config_file = "login_config.json"
        self.outbound_config_file = "outbound_config.json"
        self.sf_q = {
            "OutboundBOH1": "SELECT Id, Price_Book_Scenario__r.Price_Book__r.Brand_Code__c, Price_Book_Scenario__r.Price_Book__r.Model_Code__c, Price_Book_Scenario__r.Price_Book__r.Series_Code__c,Price_Book_Scenario__r.Price_Book__r.Market_Code__c, Version_Code__c, Price_Book_Scenario__r.Price_Book__r.Vehicle_Type__c,Price_Book_Scenario__r.Price_Book__r.Approval_Date_Time__c,Price_Book_Scenario__r.Price_Book__r.Sent_to_CFA_Date__c,Price_Book_Scenario__r.Price_Book__c, Price_Book_Scenario__r.Price_Book__r.Validity_Date__c  FROM Price_Book_Scenario_Version__c WHERE Price_Book_Scenario__r.Price_Book__r.Send_to_FCA__c = true AND Price_Book_Scenario__r.Price_Book__r.Status__c = 'Approved' AND Price_Book_Scenario__r.Status__c = 'Active' AND Special_Series_Code__c = Null",
            "OutboundBOH2": "SELECT Id, Price_Book_Scenario__r.Price_Book__r.Brand_Code__c, Price_Book_Scenario__r.Price_Book__r.Model_Code__c,Price_Book_Scenario__r.Price_Book__r.Series_Code__c,Price_Book_Scenario__r.Price_Book__r.Market_Code__c,Version_Code__c, Price_Book_Scenario__r.Price_Book__r.CreatedDate, Price_Book_Scenario__r.Price_Book__r.Currency_Code__c, Price_Book_Scenario__r.Price_Book__r.DETAX_flag__c, Detax_Price__c,Price_Book_Scenario__r.Price_Book__r.Approval_Date_Time__c,Price_Book_Scenario__r.Price_Book__r.Sent_to_CFA_Date__c,Price_Book_Scenario__r.Price_Book__c, Price_Book_Scenario__r.Price_Book__r.Validity_Date__c FROM Price_Book_Scenario_Version__c WHERE Price_Book_Scenario__r.Price_Book__r.Send_to_FCA__c = true AND Price_Book_Scenario__r.Price_Book__r.Status__c = 'Approved' AND Price_Book_Scenario__r.Status__c = 'Active' AND Special_Series_Code__c = Null",
            "OutboundBOH4": "SELECT Id, Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Brand_Code__c, Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Model_Code__c, Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Series_Code__c,Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Market_Code__c, Pricebook_Version__r.Version_Code__c,Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.DETAX_flag__c,Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.CreatedDate,Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Currency_Code__c, Option_Detax_Price__c, Code__c, Current_Status__c, Description__c,Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Approval_Date_Time__c, Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Sent_to_CFA_Date__c,Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Validity_Date__c,Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__c FROM Option_Availability__c WHERE Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Send_to_FCA__c = true AND Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Status__c = 'Approved'  AND Pricebook_Version__r.Price_Book_Scenario__r.Status__c = 'Active' AND Pricebook_Version__r.Special_Series_Code__c = Null AND (Current_Status__c = 'Tassativo' OR  Current_Status__c = 'Optional') AND is_PackChild__c =false"
        }
        self.sql_q = {
            "OutboundBOH1": "SELECT {} FROM {} as A inner join {} as B ON A.MarketCode = B.MarketCode and  A.BrandCode = B.BrandCode where A.FlagMarketCFA = '1'and B.FileName is NULL and B.FlagToSend is NULL",
            "OutboundBOH2": "SELECT {} FROM {} B join {} A ON A.MarketCode = B.MarketCode and A.BrandCode = B.BrandCode where A.FlagMarketCFA = '1' and B.FileName is NULL and B.FlagToSend is NULL",
            #"OutboundBOH2": "SELECT {} FROM OutboundBOH1 bh1 join {} B on bh1.PriceBookID = B.PriceBookID join {} A ON A.MarketCode = B.MarketCode and A.BrandCode = B.BrandCode where A.FlagMarketCFA = '1' and B.FileName is NULL and B.FlagToSend is NULL",
            "OutboundBOH4": "SELECT {} FROM {} B join {} A ON A.MarketCode = B.MarketCode and A.BrandCode = B.BrandCode where A.FlagMarketCFA = '1' and B.FileName is NULL and B.FlagToSend is NULL"
            #"OutboundBOH4": "SELECT {} FROM OutboundBOH1 bh1 join OutboundBOH2 bh2 on bh1.PriceBookID = bh2.PriceBookID join {} B on bh1.PriceBookID =B.PriceBookID join {} A ON A.MarketCode = B.MarketCode and A.BrandCode = B.BrandCode where A.FlagMarketCFA = '1' and B.FileName is NULL and B.FlagToSend is NULL"
        }
        self.q_appvd_pricebook = "SELECT Id FROM Price_Book__c WHERE Status__c = 'Approved'"
        if app:
            self.app = app
            self.store_engine = app.config['ENGINE']

    def salesforce_prop(self):
        """To retrieve the property file to get the configuration

        """
        config_file = os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Config'),
                                   self.sf_config_file)
        try:
            with open(config_file) as json_data:
                prop_dict = json.load(json_data)
        except IOError:
            self.app.logger.error("Config File Not Found!!")
            return {}
        return prop_dict

    def outbound_prop(self):
        """To retrieve the property file to get the configuration

        """
        config_file = os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Config'),
                                   self.outbound_config_file)
        try:
            with open(config_file) as json_data:
                prop_dict = json.load(json_data)
        except IOError:
            self.app.logger.error("Config File Not Found!!")
            return {}
        return prop_dict

    def get_acc_token_with_url(self):
        """ To send post request to salesforce login to generate the access token
        """
        try:
            sp = self.salesforce_prop()
            token_response = requests.post(sp.get('login_url'), headers=sp.get('header_type'), data=sp.get('payload'))
            instance_url = token_response.json()['instance_url']
            body = json.loads(token_response.content)
            token = body["access_token"]
            sf = Salesforce(instance_url=instance_url, session_id=token)
            return sf
        except requests.exceptions.RequestException as e:
            self.app.logger.error("Getting the Error Code : {0}".format(e))
            sys.exit(1)

    def get_boh_data(self, table, sf_query, sf_query_to_execute):
        self.app.logger.info('sf_query_to_execute....{}'.format(sf_query_to_execute))
        records = sf_query.query_all(sf_query_to_execute)
        records = records.get('records')
        print('length in original...', len(records))
        if table == "OutboundBOH1":
            data = [{"BrandCode": i["Price_Book_Scenario__r"]["Price_Book__r"]["Brand_Code__c"],
                     "MarketCode": i["Price_Book_Scenario__r"]["Price_Book__r"]["Market_Code__c"],
                     "Series": i["Price_Book_Scenario__r"]["Price_Book__r"]["Series_Code__c"] if
                     i["Price_Book_Scenario__r"]["Price_Book__r"]["Series_Code__c"] else '0',
                     "Model": i["Price_Book_Scenario__r"]["Price_Book__r"]["Model_Code__c"],
                     # "ApprovedDate": i["Price_Book_Scenario__r"]["Price_Book__r"]["Approval_Date_Time__c"],
                     "Sent_to_CFA_Date": i["Price_Book_Scenario__r"]["Price_Book__r"]["Sent_to_CFA_Date__c"],
                     "ID": i["Id"],
                     "PriceBookID": i["Price_Book_Scenario__r"]["Price_Book__c"],
                     "Version": i["Version_Code__c"],
                     "VehicleType": "31" if i["Price_Book_Scenario__r"]["Price_Book__r"][
                                                "Vehicle_Type__c"] == "LCV" else "00"}
                    for i in records]
        elif table == "OutboundBOH2":
            data = [{"BrandCode": i["Price_Book_Scenario__r"]["Price_Book__r"]["Brand_Code__c"],
                     "MarketCode": i["Price_Book_Scenario__r"]["Price_Book__r"]["Market_Code__c"],
                     "Series": i["Price_Book_Scenario__r"]["Price_Book__r"]["Series_Code__c"] if
                     i["Price_Book_Scenario__r"]["Price_Book__r"]["Series_Code__c"] else '0',
                     "Model": i["Price_Book_Scenario__r"]["Price_Book__r"]["Model_Code__c"],
                     # "ApprovedDate": i["Price_Book_Scenario__r"]["Price_Book__r"]["Approval_Date_Time__c"],
                     "Sent_to_CFA_Date": i["Price_Book_Scenario__r"]["Price_Book__r"]["Sent_to_CFA_Date__c"],
                     # "ValidityDate": i["Price_Book_Scenario__r"]["Price_Book__r"]["CreatedDate"],
                     "ValidityDate": i["Price_Book_Scenario__r"]["Price_Book__r"]["Validity_Date__c"] if
                     i["Price_Book_Scenario__r"]["Price_Book__r"]["Validity_Date__c"] else None,
                     "CurrencyCode": i["Price_Book_Scenario__r"]["Price_Book__r"]["Currency_Code__c"] if
                     i["Price_Book_Scenario__r"]["Price_Book__r"]["Currency_Code__c"] else '000',
                     "ID": i["Id"],
                     "PriceBookID": i["Price_Book_Scenario__r"]["Price_Book__c"],
                     "Version": i["Version_Code__c"],
                     "DetassOutSign": "-" if i["Detax_Price__c"] and int(i["Detax_Price__c"]) < 0 else "+",
                     "Price": i["Detax_Price__c"] if i["Detax_Price__c"] and int(i["Detax_Price__c"]) > 0 else i[
                         "Detax_Price__c"]
                     } for i in records]
        else:
            data = [{"BrandCode": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"]["Brand_Code__c"],
                     "MarketCode": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"][
                         "Market_Code__c"],
                     "Series": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"]["Series_Code__c"] if
                     i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"]["Series_Code__c"] else 0,
                     "ID": i["Id"],
                     "PriceBookID": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__c"],
                     "CurrencyCode": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"][
                         "Currency_Code__c"] if i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"][
                         "Currency_Code__c"] else '000',
                     "Model": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"]["Model_Code__c"],
                     # "ApprovedDate": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"]["Approval_Date_Time__c"],
                     "Sent_to_CFA_Date": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"][
                         "Sent_to_CFA_Date__c"],
                     # "ValidityDate": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"]["CreatedDate"],
                     "ValidityDate": i["Pricebook_Version__r"]["Price_Book_Scenario__r"]["Price_Book__r"][
                         "Validity_Date__c"],
                     "DetassOutSign": "-" if i["Option_Detax_Price__c"] and int(
                         i["Option_Detax_Price__c"]) < 0 else "+",
                     "Price": 0 if i["Current_Status__c"] == 'Tassativo' else i["Option_Detax_Price__c"],
                     "OptionCode": i["Code__c"], "OptionDesc": i["Description__c"],
                     "Version": i["Pricebook_Version__r"]["Version_Code__c"]} for i in
                    records]
        print('data length...', len(data))
        data_frame = pd.read_json(json.dumps(data))
        return data_frame

    def data_to_sql(self, price_book_id=''):
        """To pull the data from SF and stored into SQl table

        """
        sp = self.outbound_prop()
        boh_tables = sp.get('boh_tables')
        concat = False
        sf_query = self.get_acc_token_with_url()
        load_status = {}
        for i, table_name in enumerate(boh_tables):
            self.app.logger.info('BOH table ..............{}'.format(table_name))
            sf_query_to_execute = self.sf_q.get(table_name)
            Sent_to_CFA_df = pd.read_sql("select max(Sent_to_CFA_Date) as Sent_to_CFA_Date from {}".format(table_name),
                                      con=self.store_engine)
            max_app_date = Sent_to_CFA_df['Sent_to_CFA_Date'][0]
            if max_app_date:
                if table_name == "OutboundBOH1" or table_name == "OutboundBOH2":
                    sf_query_to_execute = sf_query_to_execute + " AND Price_Book_Scenario__r.Price_Book__r.Sent_to_CFA_Date__c > {}".format(
                        max_app_date)
                else:
                    sf_query_to_execute = sf_query_to_execute + " AND Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__r.Sent_to_CFA_Date__c > {}".format(
                        max_app_date)
                cols = sp.get('sql_' + table_name)
                sql_df = pd.read_sql(
                    "select {} from {} where Sent_to_CFA_Date > '{}' ".format(cols, table_name, max_app_date),
                    con=self.store_engine)
                if not sql_df.empty:
                    concat = True
            if price_book_id:
                if table_name == "OutboundBOH2" or table_name == "OutboundBOH1":
                    sf_query_to_execute = sf_query_to_execute + " AND Price_Book_Scenario__r.Price_Book__c = '{}'".format(
                        price_book_id)
                else:
                    sf_query_to_execute = sf_query_to_execute + " AND Pricebook_Version__r.Price_Book_Scenario__r.Price_Book__c = '{}'".format(
                        price_book_id)
            data_frame = self.get_boh_data(table_name, sf_query, sf_query_to_execute)

            if not data_frame.empty:
                # convert sql data str to pandas dataframe
                if table_name == "OutboundBOH2" or table_name == "OutboundBOH4":
                    data_frame['Price'].fillna(0, inplace=True)
                    data_frame['Price'] = data_frame['Price'].apply(str).apply(
                        lambda x: str(x).replace('-', ''))
                    # data_frame['Price'] = data_frame['Price'].apply(str).apply(
                    #     lambda x: "%.2f" %(round(int(x.replace(',', '.')), 2)) if x !='nan' else 0)
                    data_frame['Price'] = data_frame['Price'].apply(lambda x: '%.2f' % round(float(x), 2))
                    data_frame['Price'] = data_frame['Price'].apply(str).apply(lambda x: x.replace('.', ''))
                    data_frame['Price'] = data_frame['Price'].apply(str).apply(lambda x: x.zfill(13))
                    data_frame['CurrencyCode'] = data_frame['CurrencyCode'].apply(str).apply(lambda x: x.zfill(3))
                    data_frame["ValidityDate"] = pd.to_datetime(data_frame["ValidityDate"]).dt.strftime('%Y%m%d')
                elif table_name == "OutboundBOH1":
                    data_frame['VehicleType'] = data_frame['VehicleType'].apply(str).apply(
                        lambda x: x.zfill(2).strip())
                data_frame['BrandCode'] = data_frame['BrandCode'].apply(str).apply(lambda x: x.zfill(2))
                if concat:
                    diff_df = data_frame[~(data_frame['PriceBookID'].isin(sql_df['PriceBookID']))].reset_index(
                        drop=True)
                    data_frame = diff_df
                self.app.logger.info('DataFrame for table: {} \n {}'.format(table_name, data_frame))
                # try:
                data_frame.to_sql(name=table_name, con=self.store_engine, if_exists='append', index=False,
                                  chunksize=1000)
                load_status[table_name] = "Data Loaded"
                self.app.logger.info('Data Pushed to SQL successfully for {}'.format(table_name))
                # except Exception as e:
                #     self.app.logger.info("DB Error {}\n".format(e))
                #     load_status[table_name] = "Data Value Or some other error"
            else:
                load_status[table_name] = "Data Not Found"
        return jsonify(load_status)

    def get_outbound_file_and_update_outbound_table(self, app, each_pricbook):

        self.app.logger.info('Outbound file for PriceBookID {}...'.format(each_pricbook))
        sp = self.outbound_prop()
        outbound_file = sp.get('outbound_file') +str(each_pricbook)[-10:]+'_'+strftime("%Y%m%d%H%M", gmtime()) + ".txt"
        boh_tables = sp.get('boh_tables')
        boh_cols = sp.get('outbound_boh_cols')
        outboundCfa_table = sp.get('outboundCfa_table')
        for table_name in boh_tables:
            sql_to_exec = self.sql_q.get(table_name)
            if each_pricbook:
                sql_to_exec = self.sql_q.get(table_name) + " and B.PriceBookID = '{}'".format(each_pricbook)
            if table_name == 'OutboundBOH1':
                sql_to_exec = sql_to_exec.format(boh_cols, outboundCfa_table, table_name)
                outbound_df = pd.read_sql(sql_to_exec, con=self.store_engine)
            else:
                sql_to_exec = sql_to_exec.format(boh_cols, table_name, outboundCfa_table)
                outbound_df = pd.read_sql(sql_to_exec, con=self.store_engine)
            self.app.logger.info("tabel {}: executed sql for s3...{}".format(table_name, sql_to_exec))
            if outbound_df.empty:
                time.sleep(2)
                return False
            del outbound_df['PriceBookID']
            outbound_df.to_csv(outbound_file, header=None, index=None, sep='|', mode='a')
            time.sleep(10)
        with fileinput.FileInput(outbound_file, inplace=True) as file:
            for line in file:
                print(line.replace('|', ''), end='')
        time.sleep(10)
        return outbound_file

    def get_latest_appv_pricebook(self):
        sp = self.outbound_prop()
        table_name = sp.get('boh_tables')[0]
        quer_for_first_run = "select count(*) as count1 from {} where FileName is not null and  FlagToSend is not Null".format(
            table_name)
        first_run_count = pd.read_sql(quer_for_first_run, con=self.store_engine)
        count_f = first_run_count['count1'][0]
        if not count_f:
            self.q_appvd_pricebook = self.q_appvd_pricebook
            sf_query = self.get_acc_token_with_url()
            records = sf_query.query(self.q_appvd_pricebook)
            records = records.get('records')
            PriceBookIDs = [i["Id"] for i in records]
            return PriceBookIDs
        else:
            query = "select max(Sent_to_CFA_Date) as Sent_to_CFA_Date from {} where FileName is not null and  FlagToSend is not Null".format(
                table_name)
            Sent_to_CFA_df = pd.read_sql(query, con=self.store_engine)
            max_app_date = Sent_to_CFA_df['Sent_to_CFA_Date'][0]
            self.app.logger.info('max_app_date............{}'.format(max_app_date))
            if max_app_date:
                self.q_appvd_pricebook = self.q_appvd_pricebook + " AND Sent_to_CFA_Date__c > {}".format(max_app_date)
                sf_query = self.get_acc_token_with_url()
                records = sf_query.query_all(self.q_appvd_pricebook)
                records = records.get('records')
                PriceBookIDs = [i["Id"] for i in records]
                return PriceBookIDs
        return []

    def outbound_to_s3(self, app, per_price_book='', vaidity_date=''):
        sp_ = self.outbound_prop()
        if per_price_book:
            appv_price_books = [per_price_book]
            if vaidity_date:
                tables = sp_.get('boh_tables')[1:]
                db_connection = app.config['GET_DB'].connect()
                for table in tables:
                    query = "update {} set ValidityDate = '{}' where PriceBookID = '{}'".format(table, vaidity_date,
                                                                                                per_price_book)
                    db_connection.execute(query)
                    time.sleep(5)
                db_connection.close()
        else:
            appv_price_books = self.get_latest_appv_pricebook()
        self.app.logger.info('appv_price_books............{}'.format(appv_price_books))
        upload_status = {}
        err_msg = ''
        sp = self.salesforce_prop()
        if appv_price_books:
            for each_pricbook in appv_price_books:
                err_msg = "File not found please check"
                time.sleep(5)
                outbound_file = self.get_outbound_file_and_update_outbound_table(app, each_pricbook)
                self.app.logger.info('outbound_file: {}'.format(outbound_file))
                outbound_path = sp.get('outbound_path')
                self.app.logger.info('outbound_path: {}'.format(outbound_path))
                os.chdir(outbound_path)
                try:
                    outbound_file_path = os.path.join(outbound_path, outbound_file)
                except:
                    return jsonify({"Message": "File Not Found!!"})
                if os.path.exists(outbound_file_path):
                    self.app.logger.info('current_path: {}'.format(os.getcwd()))
                    AWS_ACCESS_KEY_ID = sp.get('AWS_ACCESS_KEY_ID')
                    AWS_SECRET_ACCESS_KEY = sp.get('AWS_SECRET_ACCESS_KEY')
                    REGION_HOST = sp.get('REGION_HOST')
                    bucket_name = sp.get('bucket_name')
                    self.app.logger.info('REGION_HOST...{}'.format(REGION_HOST))
                    # s3_connection = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
                    try:
                        s3_connection = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, host=REGION_HOST)
                        bucket = s3_connection.get_bucket(bucket_name)
                    except:
                        s3_connection = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
                        bucket = s3_connection.create_bucket(bucket_name)
                    self.app.logger.info('connection...{}'.format(s3_connection))
                    self.app.logger.info('bucket_name...{}'.format(bucket_name))
                    self.app.logger.info('Uploading %s to Amazon S3 bucket %s' % (outbound_file_path, bucket_name))

                    def percent_cb(complete, total):
                        sys.stdout.write('.')
                        sys.stdout.flush()

                    try:
                        k = Key(bucket)
                        self.app.logger.info('inside key...{}'.format(outbound_file))
                        k.key = 'outbound/' + str(outbound_file)
                        self.app.logger.info('inside key exception...{}'.format(outbound_file_path))
                        k.set_contents_from_filename(outbound_file_path,
                                                     cb=percent_cb, num_cb=10)
                        err_msg = "uploaded successfully"
                    except Exception as e:
                        err_msg = str(e)
                    if k:
                        db_connection = app.config['GET_DB'].connect()
                        sp = self.outbound_prop()
                        boh_tables = sp.get('boh_tables')
                        sql_update_query = "update B set B.FileName = CASE WHEN A.FlagMarketCFA = '1' THEN '{}' ELSE 'N' END,\
                                 B.FlagToSend = CASE WHEN A.FlagMarketCFA = '1' THEN 'Y' ELSE 'N' END from {} as B inner join {} as A \
                                 ON A.MarketCode = B.MarketCode and A.BrandCode = B.BrandCode where B.FileName is NULL and B.FlagToSend is NULL and B.PriceBookID='{}'"
                        outboundCfa_table = sp.get('outboundCfa_table')
                        for table in boh_tables:
                            db_connection.execute(
                                sql_update_query.format(outbound_file, table, outboundCfa_table, each_pricbook))
                        db_connection.close()
                        time.sleep(10)
                        # if outbound_file:
                        #     os.remove(str(outbound_file))
                        self.app.logger.info({"success": "File created on S3 for {}".format(each_pricbook)})
                upload_status[each_pricbook] = err_msg
            return jsonify(upload_status)
        else:
            return jsonify({"Message": "PriceBookID Not Found!!"})



+++++===================================================================================
co2comp



# -*- coding: utf-8 -*-
import copy
import datetime
# import julian
import numpy as np
import pandas as pd
from sklearn import linear_model
import sys
import os
from timeit import default_timer as timer

"""
Created on Mon May 14 11:15:08 2018

@author: Caldana Ruggero
"""
'''
FCA CO2 estimation algorthm
=====
Provides
   1. FCA CO2 estimation algorthm
How to use the documentation
----------------------------
Documentation is available in docstrings provided with the code

Required external packages
--------
NUMPY (1.13.3), PANDAS (0.21.0), COPY, SKLEARN (0.19.1), DATETIME
'''
# Import modules
# exec(open('C:/Projects/FCA/Codes/Repo/EmissionLogic/src/fca_co2_config.txt').read())
#inputfile = open(sys.argv[1] + "/fca_co2_config.txt")
inputfile = open(os.getcwd() + "/src/fca_co2_config.txt")
exec(inputfile.read(), globals())

# =============================================================================
# initialize mmvs function
# =============================================================================


def initialize_mmvs(mmvs, df_co2, mmvs_df_opt, input_path):
    '''
    inizialize data at the book creation, returning as main output:
    - the linearized table for CO2
    - a reduced CO2 dataframe for the specific mmvs
    - the matrix of historically observed options per configuration
    -------
    Parameters
    mmvs = vector with details on brand, model, version, series,
    steering wheel, market equipment and market code
    df_co2 = FCA co2 dataframe
    mmvs_df_opt = dataframe with option status (from the grid)
    -------
    Returns
    mmvs_df_co2 = rediced co2 dataframe for the specific mmvs
    mmvs_df_linearization = dataframe with linearized co2
    hist_opt_availab = dataframe with basket of comparable mmvs having the same
    co2 relevant options as selected through the best hierarchy
    hist_opt_vol = dataframe with historical volume of option per configuration
    computed over the basket of comparable models
    mmvs_df_opt = dataframe with option status and estimated initial take rates
    co2_rel_opt = list of co2 relevant options
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
# =============================================================================
#     # extract the data for the x selected MMVS
# =============================================================================
    # find index in the original data frame
    mmvs_index = (df_co2[_MOD_FIELD] == mmvs[_MOD_FIELD][0]) & \
        (df_co2[_VER_FIELD] == mmvs[_VER_FIELD][0]) & \
        (df_co2[_SER_FIELD] == mmvs[_SER_FIELD][0]) & \
        (df_co2[_G_FIELD] == mmvs[_G_FIELD][0]) & \
        (df_co2[_ALMC_FIELD] == mmvs[_ALMC_FIELD][0]) & \
        df_co2[_CO2_FIELD].notnull()
    # extract CO2 information corresponding to the MMVS
    mmvs_df_co2 = copy.deepcopy(df_co2[[_BRAND_FIELD, _MOD_FIELD, _VER_FIELD,
                                        _SER_FIELD, _G_FIELD,
                                        _ALMC_FIELD, _OPTCONF_FIELD,
                                        _OPTCONF2_FIELD, _CO2_FIELD
                                        ]][mmvs_index])
    # force CO2 values to float, the must be numeric
    mmvs_df_co2[[_CO2_FIELD]] = \
        mmvs_df_co2[[_CO2_FIELD]].astype("float64")
    # assign label to the (grid) base version
    mmvs_df_co2.loc[mmvs_df_co2[_OPTCONF2_FIELD].isnull(), _OPTCONF2_FIELD] = \
        _BASE_FIELD
    # keep track with a flag that these configurations are available
    mmvs_df_co2["available"] = 1
    # get the list of co2 relevant options
    co2_rel_opt = get_co2_rel_opt(mmvs_df_co2)
    # remove from co2 dataframe those options with not available status as
    # they come from the grid
    mmvs_df_co2, co2_rel_opt, opt_intersect_list = \
        filter_out_na_opt(mmvs_df_opt, mmvs_df_co2, _TR_STATUS_FIELD[0],
                          co2_rel_opt)
    # in case we need to manage "tassativi di griglia"
    mmvs_df_co2 = set_mkt_base_version(mmvs_df_opt, mmvs_df_co2,
                                       _TR_STATUS_FIELD[0], opt_intersect_list)
    # compute the cut-off date:
    # we should start considering historical data after this date (commented)
    # time_0 = datetime.date.today() - datetime.timedelta(days=_T)
# =============================================================================
#   !!!
#   Then here developers have to create a query in pricing tool DB to extract
#   the list of vehicles (with same Brand and Model) having the CO2 relevant
#   options. For this use case the output of the query is as in the csv
#   "config_catalog_opt.csv". The query will be a funtion of time time_0
    # and list of intersected relevant options opt_intersect_list
    # quering for all available mmvs updated after time_O having an option in
    # the list opt_intersect_list,
    # for now we load it from an external file
    hist_opt_availab = pd.read_csv(input_path+_PATH_PROD_CAT_REF, sep=";",
                                   dtype="str")
# =============================================================================
    # data clening: in case dat_fin_ord is empty we set it as today
    hist_opt_availab[_AVA_OPT_FIELD[6]
                     ][hist_opt_availab[_AVA_OPT_FIELD[6]
                                        ].isnull()] = datetime.date.today()
    # data clening: change date format from string to date
    hist_opt_availab[_AVA_OPT_FIELD[5:7]] = \
        hist_opt_availab[_AVA_OPT_FIELD[5:7]].astype(dtype="datetime64[ns]")
    # find those options that are available in opt_intersect_list, meaning
    # that the user didn't set them as not available
    index = hist_opt_availab[_AVA_OPT_FIELD[7]].apply(lambda x: x in
                                                      opt_intersect_list)
    hist_opt_availab = hist_opt_availab.loc[index]
# =============================================================================
#   !!!
#   Then here developers have to create a query in pricing tool DB to extract
#   the list of orders. For this use case the output of the query is as in the
#   csv "ita_orders_export.csv" the query will be a funtion of time_0
#   looking fo all orders (with same Brand and Model) signed after time_O,
#   but for now we load it from an external file
    hist_orders = pd.read_csv(input_path+_PATH_ORDERS_REF, sep=";", dtype="str")
# =============================================================================
    # the julian date conversion shold not be needed as the date will be
    # received in the right format
#    hist_orders[_DATE_FIELD] = hist_orders[_DATE_FIELD].astype(dtype="float")\
#        .apply(lambda x: julian.from_jd(x, fmt='jd'))

    hist_orders[_DATE_FIELD] = hist_orders[_DATE_FIELD].astype(dtype="datetime64[ns]")
    # function to obtain the historical volumes
    hist_vol_opt, hist_opt_availab = get_mmvs_hist_opt_vol(mmvs, hist_opt_availab, hist_orders,
                              opt_intersect_list, mmvs_df_co2)
    # estimate option take rates from order time series
    mmvs_df_opt[_TR0_FIELD] = pd.Series(hist_vol_opt.sum() /
                                        hist_opt_availab.shape[0],
                                        name=_TR0_FIELD)
    # linearize co2 table
    mmvs_df_linearization = linearize_co2(mmvs_df_co2)
    # extend the set of feasible configurations
    mmvs_df_co2 = enlarge_config(mmvs_df_co2, mmvs_df_linearization)
    return mmvs_df_co2, mmvs_df_linearization, hist_opt_availab, hist_vol_opt,\
        mmvs_df_opt, co2_rel_opt


# =============================================================================
# CO2 estimation function
# =============================================================================


def compute_co2_mmvs(mmvs_df_co2, mmvs_volume, mmvs_df_opt, hist_vol_opt,
                     co2_rel_opt):
    '''
    Compute co2 kpi for a selected mmvs
    -------
    Parameters
    mmvs_df_co2 = reduced CO2 dataframe, specif for the mmvs
    mmvs_df_opt = dataframe with option status and take rates
    mmvs_volume = forecasted volume for the specific version
    hist_opt_vol = dataframe with historical volume of option per configuration
    computed over the basket of comparable models
    co2_rel_opt = list of co2 relevant options
    -------
    Returns
    mmvs_df_co2 = dataframe of co2 distribution for the mmvs
    kpi = main kpis (quantiles for boxplot and average)
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
# =============================================================================
#    compute the number of parts from the initial take rates
# =============================================================================
    mmvs_df_opt[[_TR0_FIELD, _TR_FIELD]] = \
        mmvs_df_opt[[_TR0_FIELD, _TR_FIELD]].astype("float64")
    mmvs_df_opt[_VOL_OPT0] = mmvs_volume * mmvs_df_opt[_TR0_FIELD]
# =============================================================================
#     retrieve configuration reference matrix and allocate options on feasible
#     configurations
# =============================================================================
    mmvs_hist_opt_vol = hist_vol_opt.transpose()
    mmvs_hist_opt_vol = mmvs_hist_opt_vol.astype("float64")
    # normalize the combination matrix
    mmvs_hist_opt_conf = mmvs_hist_opt_vol.apply(lambda x: x/sum(x), axis=1)
    mmvs_hist_opt_conf[mmvs_hist_opt_conf.isnull()] = 0
    # filter out Not available options
    mmvs_df_co2, co2_rel_opt, opt_intersect_list = \
        filter_out_na_opt(mmvs_df_opt, mmvs_df_co2, _TR_STATUS_FIELD[1],
                          co2_rel_opt)
    # in case we need to manage "tassativi di griglia"
    mmvs_df_co2 = set_mkt_base_version(mmvs_df_opt, mmvs_df_co2,
                                       _TR_STATUS_FIELD[1], opt_intersect_list)
    # compute number of options to allocate based on forecasted volumes and
    # historical take rates
    aux = mmvs_hist_opt_conf.join(mmvs_df_opt)[_VOL_OPT0]
    # allocate options on the configurations based on historical combination
    # ratios
    for i in mmvs_hist_opt_conf.index:
        mmvs_hist_opt_conf.loc[i, :] = mmvs_hist_opt_conf.loc[i, :]*aux[i]
    # retrieve the denominator
    xx = pd.concat([mmvs_df_co2[_OPTCONF2_FIELD],
                    pd.Series(mmvs_df_co2[_OPTCONF2_FIELD
                                          ].apply(lambda x: x.count(",") + 1),
                              name="count")],
                   axis=1).set_index(_OPTCONF2_FIELD)["count"]
    # average by the number available configurations
    dist_vol = mmvs_hist_opt_conf.sum() / xx
    # clean NAN data given by /0 division
    dist_vol[np.isnan(dist_vol) == 1] = 0
    # add shock given by differences with the estimated history
    dist = add_not_combined_allocation(mmvs_df_opt, mmvs_volume, dist_vol,
                                       opt_intersect_list)
    # volume of base version
    base_vol = max(mmvs_volume - dist.sum(), 0)
    dist[_BASE_FIELD] = base_vol

    # normalize the distribution
    dist = pd.DataFrame(dist / (dist.sum()), columns=["distr"])
    # attach distribution to MMVS CO2 dataset
    dist[_OPTCONF2_FIELD] = dist.index
    mmvs_df_co2 = mmvs_df_co2.merge(dist)
# =============================================================================
#     output preparation
# =============================================================================
    # sort by co2 value
    mmvs_df_co2 = mmvs_df_co2.sort_values(_CO2_FIELD)
    # compute the cumulated distribution
    mmvs_df_co2["cum_distr"] = mmvs_df_co2["distr"].cumsum()
    # index to filter out singular configuration
    no_zero_index = (mmvs_df_co2["distr"] != 0)
    # index to filter out unavailable configurations
    available_index = (mmvs_df_co2["available"] != 0)
    # initialize the KPI data structure
    kpi = [[0.05, 0], [0.25, 0], [0.5, 0], [0.75, 0], [0.95, 0], ['avg', 0]]
    # compute quantiles
    for i in range(5):
        kpi[i][1] = mmvs_df_co2[_CO2_FIELD][(mmvs_df_co2["cum_distr"] >=
                                             kpi[i][0]) & (no_zero_index)
                                            ].min()
        # sanity check: kpi's  live in the min-max range of available values
        kpi[i][1] = min(max(kpi[i][1], mmvs_df_co2.loc[available_index,
                                                       _CO2_FIELD].min()),
                        mmvs_df_co2.loc[available_index, _CO2_FIELD].max())
    # compute the average
    kpi[5][1] = np.dot(mmvs_df_co2['distr'], mmvs_df_co2[_CO2_FIELD])
    return mmvs_df_co2, kpi, opt_intersect_list


def compute_co2_mmvs_reduced(mmvs_df_co2, mmvs_volume, mmvs_df_opt,
                             hist_vol_opt, opt_intersect_list):
    '''
    Reduced function to tompute co2 kpi for a selected mmvs (reduced) in case
    no user input option status is modified
    -------
    Parameters
    mmvs_df_co2 = reduced CO2 dataframe, specif for the mmvs
    mmvs_df_opt = dataframe with option status and take rates
    mmvs_volume = forecasted volume for the specific version
    hist_opt_vol = dataframe with historical volume of option per configuration
    computed over the basket of comparable models
    co2_rel_opt = list of co2 relevant options
    -------
    Returns
    mmvs_df_co2 = dataframe of co2 distribution for the mmvs
    kpi = main kpis (quantiles for boxplot and average)
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
# =============================================================================
#    compute the number of parts from the initial take rates
# =============================================================================
    mmvs_df_opt[[_TR0_FIELD, _TR_FIELD]] = \
        mmvs_df_opt[[_TR0_FIELD, _TR_FIELD]].astype("float64")
    mmvs_df_opt[_VOL_OPT0] = mmvs_volume * mmvs_df_opt[_TR0_FIELD]
# =============================================================================
#     retrieve configuration reference matrix and allocate options on feasible
#     configurations
# =============================================================================
    mmvs_hist_opt_vol = hist_vol_opt.transpose()
    mmvs_hist_opt_vol = mmvs_hist_opt_vol.astype("float64")
    # normalize the combination matrix
    mmvs_hist_opt_conf = mmvs_hist_opt_vol.apply(lambda x: x/sum(x), axis=1)
    mmvs_hist_opt_conf[mmvs_hist_opt_conf.isnull()] = 0
    # compute number of options to allocate based on forecasted volumes and
    # historical take rates
    aux = mmvs_hist_opt_conf.join(mmvs_df_opt)[_VOL_OPT0]
    # allocate options on the configurations based on historical combination
    # ratios
    for i in mmvs_hist_opt_conf.index:
        mmvs_hist_opt_conf.loc[i, :] = mmvs_hist_opt_conf.loc[i, :]*aux[i]
    # drop columns from previous calculation
    mmvs_df_co2 = mmvs_df_co2.drop(['distr', 'cum_distr'], axis=1)
    # retrieve the denominator
    xx = pd.concat([mmvs_df_co2[_OPTCONF2_FIELD],
                    pd.Series(mmvs_df_co2[_OPTCONF2_FIELD
                                          ].apply(lambda x: x.count(",") + 1),
                              name="count")],
                   axis=1).set_index(_OPTCONF2_FIELD)["count"]
    # average by the number available configurations
    dist_vol = mmvs_hist_opt_conf.sum() / xx
    # clean NAN data given by /0 division
    dist_vol[np.isnan(dist_vol) == 1] = 0
    # add shock given by differences with the estimated history
    dist = add_not_combined_allocation(mmvs_df_opt, mmvs_volume, dist_vol,
                                       opt_intersect_list)
    # volume of base version
    base_vol = max(mmvs_volume - dist.sum(), 0)
    dist[_BASE_FIELD] = base_vol

    # normalize the distribution
    dist = pd.DataFrame(dist / (dist.sum()), columns=["distr"])
    # attach distribution to MMVS CO2 dataset
    dist[_OPTCONF2_FIELD] = dist.index
    mmvs_df_co2 = mmvs_df_co2.merge(dist)
# =============================================================================
#     output preparation
# =============================================================================
    # sort by co2 value
    mmvs_df_co2 = mmvs_df_co2.sort_values(_CO2_FIELD)
    # copute the cumulated distribution
    mmvs_df_co2["cum_distr"] = mmvs_df_co2["distr"].cumsum()
    # index to filter out singular configuration
    no_zero_index = (mmvs_df_co2["distr"] != 0)
    # index to filter out unavailable configurations
    available_index = (mmvs_df_co2["available"] != 0)
    # initialize the KPI data structure
    kpi = [[0.05, 0], [0.25, 0], [0.5, 0], [0.75, 0], [0.95, 0], ['avg', 0]]
    # compute quantiles
    for i in range(5):
        kpi[i][1] = mmvs_df_co2[_CO2_FIELD][(mmvs_df_co2["cum_distr"] >=
                                             kpi[i][0]) & (no_zero_index)
                                            ].min()
        # sanity check: kpi's  live in the min-max range of available values
        kpi[i][1] = min(max(kpi[i][1], mmvs_df_co2.loc[available_index,
                                                       _CO2_FIELD].min()),
                        mmvs_df_co2.loc[available_index, _CO2_FIELD].max())
    # compute the average
    kpi[5][1] = np.dot(mmvs_df_co2['distr'], mmvs_df_co2[_CO2_FIELD])
    return mmvs_df_co2, kpi

def linearize_reporting(mmvs, df_co2, mmvs_df_opt):
    '''
    inizialize data at the book creation, returning as main output:
    - the linearized table for CO2
    - a reduced CO2 dataframe for the specific mmvs
    - the matrix of historically observed options per configuration
    -------
    Parameters
    mmvs = vector with details on brand, model, version, series,
    steering wheel, market equipment and market code
    df_co2 = FCA co2 dataframe
    mmvs_df_opt = dataframe with option status (from the grid)
    -------
    Returns
    mmvs_df_co2 = rediced co2 dataframe for the specific mmvs
    mmvs_df_linearization = dataframe with linearized co2
    hist_opt_availab = dataframe with basket of comparable mmvs having the same
    co2 relevant options as selected through the best hierarchy
    hist_opt_vol = dataframe with historical volume of option per configuration
    computed over the basket of comparable models
    mmvs_df_opt = dataframe with option status and estimated initial take rates
    co2_rel_opt = list of co2 relevant options
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
# =============================================================================
#     # extract the data for the x selected MMVS
# =============================================================================
    # find index in the original data frame
    mmvs_index = (df_co2[_MOD_FIELD] == mmvs[_MOD_FIELD][0]) & \
        (df_co2[_VER_FIELD] == mmvs[_VER_FIELD][0]) & \
        (df_co2[_SER_FIELD] == mmvs[_SER_FIELD][0]) & \
        (df_co2[_G_FIELD] == mmvs[_G_FIELD][0]) & \
        (df_co2[_ALMC_FIELD] == mmvs[_ALMC_FIELD][0]) & \
        df_co2[_CO2_FIELD].notnull()
    # extract CO2 information corresponding to the MMVS
    mmvs_df_co2 = copy.deepcopy(df_co2[[_BRAND_FIELD, _MOD_FIELD, _VER_FIELD,
                                        _SER_FIELD, _G_FIELD,
                                        _ALMC_FIELD, _OPTCONF_FIELD,
                                        _OPTCONF2_FIELD, _CO2_FIELD
                                        ]][mmvs_index])
    # force CO2 values to float, the must be numeric
    mmvs_df_co2[[_CO2_FIELD]] = \
        mmvs_df_co2[[_CO2_FIELD]].astype("float64")
    # assign label to the (grid) base version
    mmvs_df_co2.loc[mmvs_df_co2[_OPTCONF2_FIELD].isnull(), _OPTCONF2_FIELD] = \
        _BASE_FIELD
    # keep track with a flag that these configurations are available
    mmvs_df_co2["available"] = 1
    # get the list of co2 relevant options
    co2_rel_opt = get_co2_rel_opt(mmvs_df_co2)
    # remove from co2 dataframe those options with not available status as
    # they come from the grid
    mmvs_df_co2, co2_rel_opt, opt_intersect_list = \
        filter_out_na_opt(mmvs_df_opt, mmvs_df_co2, _TR_STATUS_FIELD[0],
                          co2_rel_opt)
    # in case we need to manage "tassativi di griglia"
    mmvs_df_co2 = set_mkt_base_version(mmvs_df_opt, mmvs_df_co2,
                                       _TR_STATUS_FIELD[0], opt_intersect_list)
    # linearize co2 table
    mmvs_df_linearization = linearize_co2(mmvs_df_co2)
    return mmvs_df_linearization



# =============================================================================
# Auxiliary functions
# =============================================================================


def linearize_co2(mmvs_df_co2):
    '''
    Linearize co2 data for a set of relevant co2 options
    -------
    Parameters
    mmvs_df_co2 = reduced CO2 dataframe, specif for the mmvs
    -------
    Returnsc
    df_linearized = dataframe of linearized co2 and linear regression output
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # compute the delta between configurations and base version
    mmvs_df_co2["delta"] = mmvs_df_co2[_CO2_FIELD] - \
        mmvs_df_co2[mmvs_df_co2[_OPTCONF2_FIELD] ==
                    _BASE_FIELD][_CO2_FIELD].values[0]
    # get co2 relevant options
    co2_rel_opt = get_co2_rel_opt(mmvs_df_co2)
    # build the network matrix for option and configurations
    X = get_opt_network_matrix(mmvs_df_co2, co2_rel_opt)
    # fit the linear regression
    reg = linear_model.LinearRegression(fit_intercept=False)
    reg.fit(X.values.transpose(), mmvs_df_co2["delta"])
    df_linearized = pd.DataFrame(index=co2_rel_opt, data=reg.coef_,
                                 columns=[_LINEAR_FIELDS[1]])
    # fill the linearized estimation
    df_linearized = pd.merge(df_linearized, mmvs_df_co2[[_OPTCONF2_FIELD, 'delta']], left_index=True,
                             right_on=_OPTCONF2_FIELD, how='left')
    df_linearized.set_index(_OPTCONF2_FIELD, inplace=True)
    df_linearized.rename(columns={'delta': _LINEAR_FIELDS[0]}, inplace=True)
    # If single value configuration is not available use linear regrassion as a proxy
    df_linearized[_LINEAR_FIELDS[0]][np.isnan(df_linearized[_LINEAR_FIELDS[0]])] = df_linearized[_LINEAR_FIELDS[1]][
        np.isnan(df_linearized[_LINEAR_FIELDS[0]])]

    # # initialize the linearized estimation
    # df_linearized[_LINEAR_FIELDS[0]] = 0
    # # fill the linearized estimation
    # for i in range(len(co2_rel_opt)):
    #     ii = (df_linearized.index[i] == mmvs_df_co2[_OPTCONF2_FIELD])
    #     # use single value configuration if available
    #     if (ii.sum() > 0):
    #         df_linearized.iloc[i, df_linearized.columns.get_loc(
    #                 _LINEAR_FIELDS[0])] = \
    #             mmvs_df_co2['delta'][ii].values
    #     # otherwise use linear regrassion as a proxy
    #     else:
    #         df_linearized.iloc[i, df_linearized.columns.get_loc(
    #                 _LINEAR_FIELDS[0])] = \
    #             df_linearized.iloc[i, df_linearized.columns.get_loc(
    #                     _LINEAR_FIELDS[1])]
    # return linearized table
    return df_linearized


def enlarge_config(mmvs_df_co2, df_linearization):
    '''
    Function to enlarge the reference co2 dataframe in order to contain also
    the configuration that are not feasible, but are required to estimate the
    sensitivity of a difference between the historical and the user input take
    rate
    -------
    Parameters
    mmvs_df_co2 = co2 dataframe for a specific mmvs
    df_linearization = dataframe of linearized co2 and linear regression output
    -------
    Returns
    co2 dataframe for a specific mmvs, attached with unfeasible basis
    configurations
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # iterate over options
    for i in range(df_linearization.shape[0]):
        # if the single option configuration does not exist
        if ((mmvs_df_co2[_OPTCONF2_FIELD] ==
             df_linearization.index[i]).sum() == 0):
            # build it and append it to the data frame
            aux = [mmvs_df_co2[_BRAND_FIELD].iloc[0],
                   mmvs_df_co2[_MOD_FIELD].iloc[0],
                   mmvs_df_co2[_VER_FIELD].iloc[0],
                   mmvs_df_co2[_SER_FIELD].iloc[0],
                   mmvs_df_co2[_G_FIELD].iloc[0],
                   mmvs_df_co2[_ALMC_FIELD].iloc[0],
                   df_linearization.index[i],
                   df_linearization.index[i],
                   mmvs_df_co2[mmvs_df_co2[_OPTCONF2_FIELD] ==
                               _BASE_FIELD][_CO2_FIELD].values[0] +
                   df_linearization.loc[df_linearization.index[i],
                                        _LINEAR_FIELDS[1]], 0,
                   df_linearization.loc[df_linearization.index[i],
                                        _LINEAR_FIELDS[1]]]
            row = pd.DataFrame(data=[aux], columns=mmvs_df_co2.columns)
            mmvs_df_co2 = mmvs_df_co2.append(row)
    return mmvs_df_co2


def add_not_combined_allocation(mmvs_df_opt, mmvs_volume, dist_vol,
                                opt_intersect_list):
    '''
    Function allocating parts exceeding the historical reference value
    -------
    Parameters
    mmvs_df_opt = dataframe containing options take rates
    mmvs_volume = forecasted volume for the specific version
    dist_vol = volume allocation after application of the historical reference
    structure
    opt_intersect_list = list of relevant options (the intersection between
    co2 relevant and selected by the user)

    -------
    Returns
    dist_out = modified distribution of volumes
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''

# =============================================================================
#   compute the extra options wrt the reference allocation
# =============================================================================
    # filter on intersection between co2 relevant and available in the grid
    temp = copy.copy(mmvs_df_opt.loc[opt_intersect_list, :])
    # filter also in case user modified the status from optional to mandatory
    temp = temp[temp[_TR_STATUS_FIELD[1]] == _OPTIONAL_FIELD]
    # compute the difference beetween historical take rates configuration and
    # user take rate
    temp[_VOL_OPT_DELTA] = round(mmvs_volume*(temp[_TR_FIELD] -
                                 temp[_TR0_FIELD]), 0)
# =============================================================================
#  allocate extra options into not combined configurations
# =============================================================================
    # initialize dist_out
    dist_out = pd.Series(np.repeat(0, dist_vol.shape[0]),
                         index=dist_vol.index)
    # iterate over option
    for i in range(temp.shape[0]):
        # for configuration equal to the single option case
        ii = (dist_out.index == temp.iloc[i].name)
        # allocate the difference between reference and forecasted parts
        dist_out.iloc[ii] = temp[_VOL_OPT_DELTA].iloc[i]
        # and then set the delta to zero
        temp.iloc[i, temp.columns.get_loc(_VOL_OPT_DELTA)] = 0
    # add the not combined density to the reference density
    dist_out = dist_out + dist_vol
# =============================================================================
#   in case of negative values set to zero (may happen when user take rates is
#   much smaller than the historical take rates!)
# =============================================================================
    dist_out[dist_out < 0] = 0
    return dist_out


def get_opt_network_matrix(mmvs_df_co2, co2_rel_opt):
    '''
    Auxiliary function to build the network matrix for option and cofigurations
    -------
    Parameters
    mmvs_df_co2 = co2 dataframe for a specific mmvs
    co2_rel_opt = list of co2 relevant options
    -------
    Returns
    X = matrix option x configuration with 1 in position i,j case the option i
    belongs to configuration j
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # initialize network matrix
    # matrix of zeros (nr of co2 relevant opt) X (available configurations)
    X = pd.DataFrame(np.zeros([len(co2_rel_opt),
                               len(mmvs_df_co2[_OPTCONF2_FIELD])],
                              dtype="int"),
                     index=co2_rel_opt, columns=mmvs_df_co2[_OPTCONF2_FIELD])
    # build matrix: if the co2 relevant option is contained in the
    # configuration set to 1
    for i in X.index:
        jj = [i in x for x in X.columns]
        X.loc[i, X.columns[jj]] = 1
    return X


def get_co2_rel_opt(mmvs_df_co2):
    '''
    Auxiliary function to extract the list of co2 relevant options
    -------
    Parameters
    mmvs_df_co2 = co2 dataframe for a specific mmvs
    -------
    Returns
    ...
    co2_rel_opt = list of co2 relevant options
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # extract the list of CO2 relevant option for the MMVS
    co2_rel_opt = pd.Series(''.join([''.join([num, ","]) for num in
                                     mmvs_df_co2[_OPTCONF2_FIELD]]
                                    ).split(",")).unique()
    # exclude empty cell and set to string format
    co2_rel_opt = co2_rel_opt[co2_rel_opt != ''].astype("str")
    # return the list of CO2 relevant option for the MMVS
    return co2_rel_opt


def get_mmvs_hist_opt_vol(mmvs, hist_opt_availab, hist_orders,
                          opt_intersect_list, mmvs_df_co2):
    '''
    Function identifying the basket of comparable mmvs and computing the
    historical reference volumes per configuration for co2 relevant options
    -------
    Parameters
    mmvs = vector with details on brand, model, version, series,
    steering wheel, market equipment and market code
    hist_opt_availab = product catalog indicating those version/series/market
    having in scope options under status O or S, for specific time period.
    Brand and Model must be fixed and equal over the whole dataframe.
    hist_orders = dataframe wih the list of historical orders for a given
    brand / model
    opt_intersect_list = list of relevant options (the intersection between
    co2 relevant and selected by the user)
    mmvs_df_co2 = co2 dataframe for a specific mmvs
    -------
    Returns
    ...
    out = dataframe with basket of comparable mmvs having the same
    co2 relevant options as selected through the best hierarchy
    volume_matrix = dataframe with historical volume of option per
    configuration computed over the basket of comparable models
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
# =============================================================================
#    find those versions having at least _TOL% of the options we are
#    interested in. This is actually the basket of proxies
# =============================================================================
    # apply the criteria
    similar_vehicles = (hist_opt_availab.groupby(_AVA_OPT_FIELD[0:7])
                        [_AVA_OPT_FIELD[7]].nunique() >=
                        _TOL[0]*len(opt_intersect_list))
    # trasform format froma a multindex series in a data frame
    similar_vehicles = pd.DataFrame(similar_vehicles)
    similar_vehicles.reset_index(inplace=True)
    # filter based on the criteria
    similar_vehicles = similar_vehicles[similar_vehicles[_AVA_OPT_FIELD[7]]]
    # further data frame reduction: we may have different period of availabiliy
    # for option on a specific MVS. Then we take the largest period that is
    # minimum starting date and maximum end date
    similar_vehicles = pd.concat([similar_vehicles.groupby(_AVA_OPT_FIELD[0:5])
                                 [_AVA_OPT_FIELD[5]].min(),
                                 similar_vehicles.groupby(_AVA_OPT_FIELD[0:5])
                                 [_AVA_OPT_FIELD[6]].max()], axis=1)
    # trasform format froma a multindex series in a data frame
    similar_vehicles = pd.DataFrame(similar_vehicles)
    similar_vehicles.reset_index(inplace=True)
    # key variable is the concatenation of _AVA_OPT_FIELD
    similar_vehicles["key"] = \
        similar_vehicles[_AVA_OPT_FIELD[0]] + \
        similar_vehicles[_AVA_OPT_FIELD[1]] + \
        similar_vehicles[_AVA_OPT_FIELD[2]] + \
        similar_vehicles[_AVA_OPT_FIELD[3]] + \
        similar_vehicles[_AVA_OPT_FIELD[4]]
# =============================================================================
#    Select orders history or the nearest approximation
# =============================================================================
    # case 1: the version under analysis has a history
    criteria = \
        ((hist_orders[_MKT_FIELD] == mmvs[_MKT_FIELD].iloc[0]) &
         (hist_orders[_BRAND_FIELD] == mmvs[_BRAND_FIELD].iloc[0]) &
         (hist_orders[_MOD_FIELD] == mmvs[_MOD_FIELD].iloc[0]) &
         (hist_orders[_SER_FIELD] == mmvs[_SER_FIELD].iloc[0]) &
         (hist_orders[_VER_FIELD] == mmvs[_VER_FIELD].iloc[0]))

    hist_orders["key"] = hist_orders[_MKT_FIELD] + \
        hist_orders[_BRAND_FIELD] + hist_orders[_MOD_FIELD] + \
        hist_orders[_SER_FIELD] + hist_orders[_VER_FIELD]
    out = get_nearest_approx(criteria, hist_orders, similar_vehicles)
    # verify that if the output exists it has the minimum depth
    if (out.shape[0] < _TOL[1]):
        # No history then case 2
        # case 2: look at different versions of the same market, brand, model,
        # series
        criteria = \
            ((hist_orders[_MKT_FIELD] == mmvs[_MKT_FIELD].iloc[0]) &
             (hist_orders[_BRAND_FIELD] == mmvs[_BRAND_FIELD].iloc[0]) &
             (hist_orders[_MOD_FIELD] == mmvs[_MOD_FIELD].iloc[0]) &
             (hist_orders[_SER_FIELD] == mmvs[_SER_FIELD].iloc[0]))
        out = get_nearest_approx(criteria, hist_orders, similar_vehicles)
        # verify that if the output exists it has the minimum depth
        if (out.shape[0] < _TOL[1]):
            # case 3: look at different versions of the same brand, model,
            # series
            criteria = \
                ((hist_orders[_BRAND_FIELD] == mmvs[_BRAND_FIELD].iloc[0]) &
                 (hist_orders[_MOD_FIELD] == mmvs[_MOD_FIELD].iloc[0]) &
                 (hist_orders[_SER_FIELD] == mmvs[_SER_FIELD].iloc[0]))
            out = get_nearest_approx(criteria, hist_orders, similar_vehicles)
            # verify that if the output exists it has the minimum depth
            if (out.shape[0] < _TOL[1]):
                # case 3: look at different versions of the same brand, model
                criteria = \
                    ((hist_orders[_BRAND_FIELD] == mmvs[_BRAND_FIELD].iloc[0])
                     & (hist_orders[_MOD_FIELD] == mmvs[_MOD_FIELD].iloc[0]))
                out = get_nearest_approx(criteria, hist_orders,
                                         similar_vehicles)
                # verify that if the output exists it has the minimum depth
                if (out.shape[0] < _TOL[1]):
                    # fallback case
                    # inizializing the matrix of zeros
                    # (nr of available co2 relevant opt) X
                    # (available configurations)
                    volume_matrix = \
                        pd.DataFrame(np.zeros((len(mmvs_df_co2[_OPTCONF_FIELD
                                                               ]),
                                              len(opt_intersect_list))),
                                     columns=opt_intersect_list,
                                     index=mmvs_df_co2[_OPTCONF2_FIELD])
                    # select with 1 the not combined configuration related to
                    # a specific available co2 relevant opt
                    volume_matrix[volume_matrix.apply(lambda x: x.name ==
                                                      x.index)]=1
                    # if fallback solution exit function here
                    return volume_matrix, out
    # inizialize an auxiliary zero table to identify the observed options that
    # we concatenate to the original orders dataframe
    out = pd.concat([out, pd.DataFrame(np.zeros((out.shape[0],
                                                 len(opt_intersect_list))),
                    columns=opt_intersect_list, index=out.index)], axis=1)
    out[opt_intersect_list] = out[opt_intersect_list].astype(dtype="int")
    # search for relevant options into the historical orders (OPTIONAL)
    for i in range(len(opt_intersect_list)):
        j = [opt_intersect_list[i] in x for x in out[_OPT_LIST_FIELD[0]]]
        out.loc[j, opt_intersect_list[i]] = 1
    # search for relevant options into the historical orders (STRUCTURAL)
        jj = [opt_intersect_list[i] in x for x in out[_OPT_LIST_FIELD[1]]]
        out.loc[jj, opt_intersect_list[i]] = 1
# =============================================================================
#   Count volume matrix based on the identified nearest approximation
# =============================================================================
    # reference table to identify available configurations in the order history
    # inizialize an auxiliary zero table to identify the observed options that
    # we concatenate to the original orders dataframe
    ref = pd.concat([mmvs_df_co2[_OPTCONF2_FIELD],
                     pd.DataFrame(np.zeros((mmvs_df_co2.shape[0],
                                            len(opt_intersect_list)),
                                           dtype="int"),
                                  columns=opt_intersect_list,
                                  index=mmvs_df_co2.index)], axis=1)
    # search for relevant options into the reference table to map
    # the combinations
    for i in range(len(opt_intersect_list)):
        j = [opt_intersect_list[i] in x for x in ref[_OPTCONF2_FIELD]]
        ref.loc[j, opt_intersect_list[i]] = 1
    # join the history of orders with the configuration information
    res = pd.merge(ref, out, how='inner', left_on=opt_intersect_list,
                   right_on=opt_intersect_list)
    res[opt_intersect_list] = res[opt_intersect_list].astype(dtype="int")
    # count volumes per configuration
    volume_matrix = res.groupby(_OPTCONF2_FIELD)[opt_intersect_list].sum()
    return volume_matrix, out


def get_nearest_approx(criteria, hist_orders, similar_vehicles):
    '''
    Auxiliary function identifying the nearest_approximation in the orders
    historory as of selected criteria
    -------
    Parameters
    criteria = logical vector selecting the orders according to the hierarchy
    hist_orders = dataframe wih the list of historical orders for a given
    brand / model
    similar_vehicles
    -------
    Returns
    ...
    out = dataframe with basket of comparable mmvs having the same
    co2 relevant options as selected through the best hierarchy
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # join by key
    out = \
        hist_orders[criteria].set_index('key').\
        join(similar_vehicles[["key", _AVA_OPT_FIELD[5], _AVA_OPT_FIELD[6]]
                              ].set_index('key'), how="inner")
    # active orders are those belonging to the availability period
    active = (out[_DATE_FIELD] >= out[_AVA_OPT_FIELD[5]]) & \
        (out[_DATE_FIELD] <= out[_AVA_OPT_FIELD[6]])
    out = out[active]
    return out


def filter_out_na_opt(mmvs_df_opt, mmvs_df_co2, status_col, co2_rel_opt):
    '''
    Auxiliary function to exclude from the co2 mmvs dataframe those options
    having not available status
    -------
    Parameters
    mmvs_df_opt = dataframe containing options status and take rates
    mmvs_df_co2 = co2 dataframe for a specific mmvs
    status_col = _TR_STATUS_FIELD[0] or _TR_STATUS_FIELD[1]
    co2_rel_opt = list of co2 relevant options
    -------
    Returns
    mmvs_df_co2 = co2 dataframe for a specific mmvs (filtered)
    co2_rel_opt = list of co2 relevant options (excluded not available)
    opt_intersect_list = list of relevant options (the intersection between
    co2 relevant and selected by the user)
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # identify the list of not available options as in (grid/user) status
    na_list = mmvs_df_opt[mmvs_df_opt[status_col] == _NOT_AVA_FIELD].index
    # identify co2 relevant configurations having those not available options
    for i in na_list:
        j = [i not in x for x in mmvs_df_co2[_OPTCONF_FIELD]]
        # and remove them
        mmvs_df_co2 = mmvs_df_co2[j]
        #co2_rel_opt = co2_rel_opt[co2_rel_opt != i]
    # the algorithm works on the intersection between relevent options and
    # options set as available by the user
    opt_intersect_list = [val for val in co2_rel_opt if val not in na_list]
    co2_rel_opt = opt_intersect_list.copy()
    return mmvs_df_co2, co2_rel_opt, opt_intersect_list


def set_mkt_base_version(mmvs_df_opt, mmvs_df_co2, status_col,
                         opt_intersect_list):
    '''
    Auxiliary function to modify the base version depending on user selection
    of "Tassativo" (i.e. mandatory) options. The old Base version is dropped
    and the new base version is identified. In case the selected options
    identify a configuration not in the dataframe, the base version is
    estimated through linearization
    -------
    Parameters
    mmvs_df_opt = dataframe containing options take rates
    mmvs_df_co2 = co2 dataframe for a specific mmvs
    status_col = _TR_STATUS_FIELD[0] or _TR_STATUS_FIELD[1]
    opt_intersect_list = list of relevant options (the intersection between
    co2 relevant and selected by the user)
    -------
    Returns
    mmvs_df_co2 = co2 dataframe for a specific mmvs (with modified base
    version)
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # extract the list of CO2 relevant options
    mmvs_df_co2 = copy.deepcopy(mmvs_df_co2)
    # in case some options are set to "Tassativo"
    if (mmvs_df_opt.loc[opt_intersect_list, status_col] ==
            _TASS_FIELD).sum() > 0:
        # inizializing the matrix of zeros
        # (nr of available co2 relevant opt) X (nr available configurations)
        aux2 = pd.DataFrame(np.zeros((mmvs_df_co2.shape[0],
                                      len(opt_intersect_list))),
                            index=mmvs_df_co2.index,
                            columns=opt_intersect_list)
        # iterate on available co2 relevant opt
        for i in range(len(opt_intersect_list)):
            # find configurations containing the option and set value to 1
            j = [opt_intersect_list[i] in x for x in
                 mmvs_df_co2[_OPTCONF2_FIELD]]
            aux2.loc[j, opt_intersect_list[i]] = 1
        # identify tassativo options
        aux3 = mmvs_df_opt[mmvs_df_opt[status_col] == _TASS_FIELD].index
        try:
            # identify the position of the new base versions
            base_pos = aux2.loc[(aux2.sum(axis=1) == len(aux3)) &
                                (aux2[aux3].sum(axis=1) == len(aux3)),
                                aux3].sum(axis=1).argmax()
        # in case the tassativo status identify a configuration not present in
        # the dataframe we need to estimate it from the linearization
        except:
            df_linearized = linearize_co2(mmvs_df_co2)
            # set the value as the former base version + opt linearized
            base_value = \
                mmvs_df_co2.loc[mmvs_df_co2[_OPTCONF2_FIELD] == _BASE_FIELD,
                                _CO2_FIELD] + \
                df_linearized.loc[aux3, "linearization"].sum()
            # assign the new base value
            mmvs_df_co2.loc[mmvs_df_co2[_OPTCONF2_FIELD] == _BASE_FIELD,
                            _CO2_FIELD] = base_value
            # update the option list
            mmvs_df_co2.loc[mmvs_df_co2[_OPTCONF2_FIELD] == _BASE_FIELD,
                            _OPTCONF_FIELD] = ",".join(aux3)
            return mmvs_df_co2

        # rule out the former base version row
        mmvs_df_co2 = mmvs_df_co2[mmvs_df_co2[_OPTCONF2_FIELD] != _BASE_FIELD]
        # set the new base versions
        mmvs_df_co2.loc[aux2.index[aux2.index == base_pos], _OPTCONF2_FIELD] =\
            _BASE_FIELD
    return mmvs_df_co2

# =============================================================================
# Specific functions for reports page
# =============================================================================


def combination_report_func(mmvs, orders_ts, df_co2):
    '''
    Function providing the information required to populate the combination
    report
    -------
    Parameters
    mmvs = vector with details on brand, model, version, series,
    steering wheel, market equipment and market code
    orders_ts = time series of orders for the selected mmvs for a specific
    time period
    df_co2 = FCA co2 dataframe
    -------
    Returns
    mmvs_hist_opt_conf = matrix with option vs configuration showing for each
    option the percentage of allocation on each specific configuration
    hist_config_distr = distribution of sales over the different configurations
    opt_vol = number of options sold
    opt_take_rates = estimated option take rate
    --------
    Author(s)
    Caldana, Ruggero <caldana.ruggero@bcg.com>
    '''
    # find index in the original data frame
    mmvs_index = (df_co2[_MOD_FIELD] == mmvs[_MOD_FIELD][0]) & \
        (df_co2[_VER_FIELD] == mmvs[_VER_FIELD][0]) & \
        (df_co2[_SER_FIELD] == mmvs[_SER_FIELD][0]) & \
        (df_co2[_G_FIELD] == mmvs[_G_FIELD][0]) & \
        (df_co2[_ALMC_FIELD] == mmvs[_ALMC_FIELD][0]) & \
        df_co2[_CO2_FIELD].notnull()
    # extract CO2 information corresponding to the MMVS
    mmvs_df_co2 = copy.deepcopy(df_co2[[_BRAND_FIELD, _MOD_FIELD, _VER_FIELD,
                                        _SER_FIELD, _G_FIELD,
                                        _ALMC_FIELD, _OPTCONF_FIELD,
                                        _OPTCONF2_FIELD, _CO2_FIELD
                                        ]][mmvs_index])
    # force CO2 values to float, the must be numeric
    mmvs_df_co2[[_CO2_FIELD]] = \
        mmvs_df_co2[[_CO2_FIELD]].astype("float64")
    # assign label to the (grid) base version
    mmvs_df_co2.loc[mmvs_df_co2[_OPTCONF2_FIELD].isnull(), _OPTCONF2_FIELD] = \
        _BASE_FIELD
    # obtain the list of co2 relevant options
    co2_rel_opt = get_co2_rel_opt(mmvs_df_co2)
    co2_rel_opt = [i for i in co2_rel_opt]
    # attach a mapping matrix to the list of orders
    orders_ts = \
        pd.concat([orders_ts, pd.DataFrame(np.zeros((orders_ts.shape[0],
                                                     len(co2_rel_opt))),
                                           columns=co2_rel_opt,
                                           index=orders_ts.index)], axis=1)
    orders_ts[co2_rel_opt] = orders_ts[co2_rel_opt].astype(dtype="int")
    # search for relevant options into the historical orders (OPTIONAL)
    for i in range(len(co2_rel_opt)):
        j = [co2_rel_opt[i] in x for x in orders_ts[_OPT_LIST_FIELD[0]]]
        orders_ts.loc[j, co2_rel_opt[i]] = 1
        # search for relevant options into the historical orders (STRUCTURAL)
        jj = [co2_rel_opt[i] in x for x in orders_ts[_OPT_LIST_FIELD[1]]]
        orders_ts.loc[jj, co2_rel_opt[i]] = 1
    # reference table to identify available configurations in the order history
    # inizialize an auxiliary zero table to identify the observed options that
    # we concatenate to the original orders dataframe
    ref = pd.concat([mmvs_df_co2[_OPTCONF2_FIELD],
                     pd.DataFrame(np.zeros((mmvs_df_co2.shape[0],
                                            len(co2_rel_opt)),
                                           dtype="int"),
                                  columns=co2_rel_opt,
                                  index=mmvs_df_co2.index)], axis=1)
    # search for relevant options into the reference table to map
    # the combinations
    for i in range(len(co2_rel_opt)):
        j = [co2_rel_opt[i] in x for x in ref[_OPTCONF2_FIELD]]
        ref.loc[j, co2_rel_opt[i]] = 1
    # join the history of orders with the configuration information
    res = pd.merge(ref, orders_ts, how='inner', left_on=co2_rel_opt,
                   right_on=co2_rel_opt)
    res[co2_rel_opt] = res[co2_rel_opt].astype(dtype="int")
    # count volumes per configuration
    mmvs_hist_opt_vol = res.groupby(_OPTCONF2_FIELD)[co2_rel_opt].sum()
    # transpose and change type
    mmvs_hist_opt_vol = mmvs_hist_opt_vol.transpose()
    mmvs_hist_opt_vol = mmvs_hist_opt_vol.astype("float64")
    # normalize the combination matrix
    mmvs_hist_opt_conf = mmvs_hist_opt_vol.apply(lambda x: x/sum(x), axis=1)
    mmvs_hist_opt_conf[mmvs_hist_opt_conf.isnull()] = 0
    # estimate the distribution of historical configurations
    hist_config_distr = mmvs_hist_opt_vol.max(axis=0)/orders_ts.shape[0]
    hist_config_distr[_BASE_FIELD] = 1 - hist_config_distr.sum()
    # historical option volume
    opt_vol = mmvs_hist_opt_vol.sum(axis=1)
    # estimated historical take rate
    opt_take_rates = mmvs_hist_opt_vol.sum(axis=1)/orders_ts.shape[0]

    return mmvs_hist_opt_conf, hist_config_distr, opt_vol, opt_take_rates

==================================

logger from envsetup

# ### Set up log file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('Log_file_'+str(datetime.now().strftime('%Y-%m-%d-%H%M%S'))+'.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

logger.info('Start')


=====================================================

imprt logger

  from Environment_Setup import logger, pd, os, read_db, write_db_schema
    logger.info('========Environment Setup======')
