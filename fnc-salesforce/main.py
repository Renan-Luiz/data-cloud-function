import json, os
from simple_salesforce import Salesforce, SalesforceLogin
from datetime import datetime, timedelta
import logging
import pandas as pd

def main(request):

    class GetSalesforceData():
        def __init__(self):
            self.domain = request.json["domain"]
            self.version = request.json["version"]
            self.tableSalesforce= request.json["tableSalesforce"]
            self.fields = request.json["fields"]
            self.fieldDelta = str(request.json['fieldDelta']).lower()
            self.dtini = (datetime.today() + timedelta(days=int(request.json['dtini']))).strftime("%Y-%m-%dT00:00:00.000+0000")
            self.dtfim = (datetime.today() + timedelta(days=int(request.json['dtfim']))).strftime("%Y-%m-%dT23:59:59.999+0000")
            self._credential = json.loads(os.environ["salesforcekey"])
            self._username = self._credential['username']
            self._password = self._credential['password']
            self._security_token = self._credential['security_token']
        
        def connect(self):
            session_id, instance = SalesforceLogin(username=self._username, password=self._password, security_token=self._security_token, domain=self.domain)
            return Salesforce(session_id=session_id, instance=instance, version=self.version)
        
        def extract(self, sf_connection):
            try:
                query = f"SELECT {self.fields} FROM {self.tableSalesforce} WHERE {self.fieldDelta} >= {self.dtini} AND {self.fieldDelta} <= {self.dtfim}"
                result = sf_connection.query(query)
                
            except Exception as e:
                logging.error('Erro ao obter registros da API Salesforce.')
                raise Exception(e)
        
            size = result['totalSize']
            if size > 0:
                try:
                    if result['done'] == True:
                        df = pd.DataFrame(result['records'], dtype=str)
                        return df

                    else:
                        df = pd.DataFrame(result['records'],dtype=str)
                        while result['done'] == False:
                            result = sf_connection.query_more(result['nextRecordsUrl'], True)
                            df = pd.concat([df, pd.DataFrame(result['records'],dtype=str)], ignore_index=True)

                    return df
                
                except Exception as e:
                    logging.error('Erro ao transformar os registros para o dataframe.')
                    raise Exception(e)
            else:
                return None
            
    
    class ToBigquery(GetSalesforceData):
        def __init__(self, get_salesforce_data):
            self.projectId = request.json["projectId"]
            self.tableBQ = request.json["tableBQ"]
            self.partition = str(request.json['partition']).lower()
            self.fieldDelta = get_salesforce_data.fieldDelta
            self.dtini = get_salesforce_data.dtini
            self.dtfim = get_salesforce_data.dtfim

        def adjust_df(self, df):
            df.columns = map(str.lower, df.columns)
            if 'attributes'  in df.columns:
                df.drop('attributes', axis='columns', inplace=True)
            if self.partition == 's':
                df[self.fieldDelta] = pd.to_datetime(df[self.fieldDelta])
            
            return df

    get_salesforce_data = GetSalesforceData()
    df = get_salesforce_data.extract(get_salesforce_data.connect())

    print(ToBigquery(get_salesforce_data).adjust_df(df))
    return "Success"