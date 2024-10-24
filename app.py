import boto3
from botocore.exceptions import ClientError
import os
from io import StringIO
from io import BytesIO
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
from fn import convert
import json
from pip._vendor import requests
from datetime import datetime,timedelta
import csv
import asyncio



#################    INPUT  #########################################
PERIOD = os.environ["PERIOD"]
EXPORT_START_DATE = os.environ["EXPORT_START_DATE"]
EXPORT_END_DATE = os.environ["EXPORT_END_DATE"]
COUNTRY_CODE= os.environ['COUNTRY_CODE']
DURATION = os.environ["EXPORT_DURATION"]
MARGIN_RATE = int(os.environ["MARGIN_RATE"])

#CALLBACK_URL = os.environ['LAMBDA_URL']
WAREHOUSE_URL = os.environ['WAREHOUSE_URL']
WAREHOUSE_ID = os.environ['WAREHOUSE_ID']
WAREHOUSE_TOKEN = os.environ['WAREHOUSE_TOKEN']
EXPORT_API_RESULT_FILE_NAME =  'chargeguru_chargelog.csv'

######################################################################

# # lambda entry point
def lambda_handler(event, context):
    print('event', event)

    COUNTRY_CODE = None
    EXPORT_START_DATE = None
    EXPORT_END_DATE = None
    PERIOD= None
    MARGIN_RATE = None
    config_json_str = set_config_variables(event)
    config_json = json.loads(config_json_str)
    if config_json:
        print('config_json_str')
        print(config_json_str)
        COUNTRY_CODE= config_json["COUNTRY_CODE"]
        EXPORT_START_DATE = config_json["EXPORT_START_DATE"]
        EXPORT_END_DATE = config_json["EXPORT_END_DATE"]
        PERIOD= config_json["PERIOD"]
        MARGIN_RATE = int(config_json["MARGIN_RATE"])

        # Print the results
        print('COUNTRY_CODE :', COUNTRY_CODE)
        print('EXPORT_START_DATE :', EXPORT_START_DATE)
        print('EXPORT_END_DATE:', EXPORT_END_DATE)
        print('PERIOD :', PERIOD)
        print('MARGIN_RATE :', MARGIN_RATE)
    
    statement_id_response= get_data_from_databricks(WAREHOUSE_URL, WAREHOUSE_ID, WAREHOUSE_TOKEN,config_json)
    if statement_id_response.get('statusCode', 0) == 200:
        if 'body' in statement_id_response and 'statement_id' in statement_id_response['body'] and 'config_variables' in statement_id_response['body']:
            body_json = json.loads(statement_id_response['body'])
            statement_id = body_json['statement_id']
            config_variables = body_json['config_variables']
            print('statement_id:', statement_id)
            if statement_id and config_variables:  
                download_url_response=  get_download_url(WAREHOUSE_URL, WAREHOUSE_TOKEN,statement_id,config_variables)    
                if  download_url_response.get('statusCode', 0) == 200:
                    if 'body' in download_url_response and 'download_url' in download_url_response['body'] and 'config_variables' in download_url_response['body']:
                        body_json = json.loads(download_url_response['body'])
                        download_url = body_json['download_url']
                        config_variables = body_json ['config_variables']
                        print('download_url: ',download_url)   
                        download_export_databricks(download_url,  config_variables)
                        process_cpo_internal(config_variables)       
                else:
                    print('download_url not found: ',download_url_response['statusCode'])   
    else:
        print('statement_id not found: ',statement_id_response['statusCode'])  


def lambda_handler_test(event, context):
    print('event', event)

    COUNTRY_CODE = None
    EXPORT_START_DATE = None
    EXPORT_END_DATE = None
    PERIOD= None
    MARGIN_RATE = None
    config_json_str = set_config_variables(event)
    config_json = json.loads(config_json_str)
    if config_json:
        print('config_json_str')
        print(config_json_str)
        COUNTRY_CODE= config_json["COUNTRY_CODE"]
        EXPORT_START_DATE = config_json["EXPORT_START_DATE"]
        EXPORT_END_DATE = config_json["EXPORT_END_DATE"]
        PERIOD= config_json["PERIOD"]
        MARGIN_RATE = int(config_json["MARGIN_RATE"])

        # Print the results
        print('COUNTRY_CODE :', COUNTRY_CODE)
        print('EXPORT_START_DATE :', EXPORT_START_DATE)
        print('EXPORT_END_DATE:', EXPORT_END_DATE)
        print('PERIOD :', PERIOD)
        print('MARGIN_RATE :', MARGIN_RATE)
    
    get_data_by_middleware(url,  config_variables)
    process_cpo_internal(config_variables)  


def get_data_by_middleware(config_variables):
    url = 'https://partner.chargeguru.com/proxy?block=undefined&queryId=973446c8-33e8-4369-9bd5-fd48efcf8f5f&where[cpo_environment]=ampeco_fr&where[export_start_date]=2024-09-01&where[export_end_date]=2024-09-30'
    print('get_data_by_middleware started')
    
    if config_variables:
        COUNTRY_CODE = config_variables["COUNTRY_CODE"]  
        PERIOD = config_variables["PERIOD"]
        directory = COUNTRY_CODE + '/' + PERIOD + '/'
    
    headers = {
        'content-type': 'application/x-www-form-urlencoded'
    }
    
    try:
        response = requests.get(url, headers=headers).json()

        # Check if the request was successful (status code 200 and "SUCCEEDED" state)
        if response["status"]["state"] == "SUCCEEDED":
            data_array = response["result"]["data_array"]
            columns = [col["name"] for col in response["manifest"]["schema"]["columns"]]

            # Convert the response data to a DataFrame
            df = pd.DataFrame(data_array, columns=columns)

            # Ensure proper data types are applied
            df = df.apply(pd.to_numeric, errors='ignore')  # Converts columns with numeric values
           
            # Push the DataFrame to S3 as a CSV
            csv_data = df.to_csv(sep=';', index=False, header=True)
            push_csv_to_s3(csv_data, directory + 'chargeguru_chargelog.csv')
        
        else:
            return api_response(500, 'Error: Response state not succeeded')
        
    except Exception as e:
        return api_response(500, 'Error during download: ' + str(e))

     
# Function to set configuration variables
def set_config_variables(event):
    print('in set_config_variables event is')
    print(event)
    output_result = event.copy()
    config_variables = {}

    if 'COUNTRY_CODE' in event and output_result: 
        COUNTRY_CODE= output_result.get("COUNTRY_CODE")
        EXPORT_START_DATE = output_result.get("EXPORT_START_DATE")
        EXPORT_END_DATE = output_result.get("EXPORT_END_DATE")
        PERIOD = output_result.get("PERIOD")
        MARGIN_RATE = int(output_result.get("MARGIN_RATE"))

        if COUNTRY_CODE is None or COUNTRY_CODE == 'Scenario failed to complete.' or EXPORT_START_DATE == 'Scenario failed to complete.':
            print('using Lambda env variables')
            EXPORT_START_DATE = os.environ.get('EXPORT_START_DATE', '')
            EXPORT_END_DATE = os.environ.get('EXPORT_END_DATE', '')
            PERIOD = os.environ.get('PERIOD', '')
            COUNTRY_CODE = os.environ["COUNTRY_CODE"]
            MARGIN_RATE = int(os.environ["MARGIN_RATE"])
        else:
            print('using Make variable:')

    else:
        print('using Lambda env variables')
        EXPORT_START_DATE = os.environ.get('EXPORT_START_DATE', '')
        EXPORT_END_DATE = os.environ.get('EXPORT_END_DATE', '')
        PERIOD = os.environ.get('PERIOD', '')
        COUNTRY_CODE = os.environ["COUNTRY_CODE"]
        MARGIN_RATE = int(os.environ["MARGIN_RATE"])
    
    config_variables['COUNTRY_CODE'] = COUNTRY_CODE
    config_variables['EXPORT_START_DATE'] = EXPORT_START_DATE
    config_variables['EXPORT_END_DATE'] = EXPORT_END_DATE
    config_variables['PERIOD'] = PERIOD
    config_variables['MARGIN_RATE'] = MARGIN_RATE

    print('config_variables:', config_variables)
    return json.dumps(config_variables)

#def writeEmployeeOutput(config_variables, companyName, employeeName, detail, customerNumber, cardNumber):
def writeEmployeeOutput(config_variables, employeeName, detail, customerNumber, cardNumber):
    if config_variables:
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]
        PERIOD= config_variables["PERIOD"]
        directory = COUNTRY_CODE +'/'+ PERIOD+'/'  

    #ac3
    # sessions_number = len(detail[column_language_dict.get(COUNTRY_CODE)[0]])
    # total_revenues = detail[column_language_dict.get(COUNTRY_CODE)[1]].sum()
    # employeeSummary = pd.DataFrame(columns=['columns1', 'columns2'], data = 
    # [[column_language_dict.get(COUNTRY_CODE)[2], employeeName ],[column_language_dict.get(COUNTRY_CODE)[3],companyName], [column_language_dict.get(COUNTRY_CODE)[4],PERIOD],
    #                                                                 [column_language_dict.get(COUNTRY_CODE)[5],sessions_number],[column_language_dict.get(COUNTRY_CODE)[6],total_revenues],[column_language_dict.get(COUNTRY_CODE)[7], customerNumber]])
    
    
    sessions_number = len(detail[column_dict.get(COUNTRY_CODE)["Station Owner"]])
    total_revenues = detail[column_dict.get(COUNTRY_CODE)["Session Amount (€)"]].sum()
    employeeSummary = pd.DataFrame(
    columns=['columns1', 'columns2'], 
    #ac2
    # data=[
    #     [column_dict.get(COUNTRY_CODE)["Name and surname of the user"], employeeName],
    #     [column_dict.get(COUNTRY_CODE)["Company Name"], companyName],
    #     [column_dict.get(COUNTRY_CODE)["Period"], PERIOD],
    #     [column_dict.get(COUNTRY_CODE)["Number of charging sessions"], sessions_number],
    #     [column_dict.get(COUNTRY_CODE)["Total to reimburse (€)"], total_revenues],
    #     [column_dict.get(COUNTRY_CODE)["Customer Number"], customerNumber]
    # ]

     data=[
        [column_dict.get(COUNTRY_CODE)["Name and surname of the user"], employeeName],
        [column_dict.get(COUNTRY_CODE)["Period"], PERIOD],
        [column_dict.get(COUNTRY_CODE)["Number of charging sessions"], sessions_number],
        [column_dict.get(COUNTRY_CODE)["Total to reimburse (€)"], total_revenues],
        [column_dict.get(COUNTRY_CODE)["Customer Number"], customerNumber]
    ]
    )


    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    
    employeeSummary.to_excel(writer, sheet_name='Global',index = False, header=False)
    for column in employeeSummary:
        column_width = max(employeeSummary[column].astype(str).map(len).max(), len(column))
        col_idx = employeeSummary.columns.get_loc(column)
        writer.sheets['Global'].set_column(col_idx, col_idx, column_width)

    detail.to_excel(writer, sheet_name='Detail',index = False)

    for column in detail:
        column_width = max(detail[column].astype(str).map(len).max(), len(column))
        col_idx = detail.columns.get_loc(column)
        writer.sheets['Detail'].set_column(col_idx, col_idx, column_width)

    writer.close()

    xlsx_data = output.getvalue()
    s3_path = directory+' FABS Client Expense Report - ' + PERIOD+' - '+cardNumber+' - '+employeeName+'.xlsx'
    push_file_to_s3(xlsx_data, s3_path)



# def writeCompanyOutput(config_variables, companyName, employeesDetails, customerNumber, CG_summary_companies):
#     if config_variables:
#         COUNTRY_CODE= config_variables["COUNTRY_CODE"]
#         PERIOD= config_variables["PERIOD"]
#         directory = COUNTRY_CODE +'/'+ PERIOD+'/'     
#     numberOfEmployees = len(employeesDetails[column_language_dict.get(COUNTRY_CODE)[10]].unique())
#     numberOfSessions = employeesDetails[column_language_dict.get(COUNTRY_CODE)[5]].sum()
#     totalToReimburse = employeesDetails[column_language_dict.get(COUNTRY_CODE)[11]].sum()
#     companySummary = pd.DataFrame(columns=['columns1', 'columns2'], data = [[column_language_dict.get(COUNTRY_CODE)[3], companyName ],[column_language_dict.get(COUNTRY_CODE)[7], customerNumber], [column_language_dict.get(COUNTRY_CODE)[4],PERIOD],[column_language_dict.get(COUNTRY_CODE)[12],numberOfEmployees],[column_language_dict.get(COUNTRY_CODE)[5],numberOfSessions],
#                                     [column_language_dict.get(COUNTRY_CODE)[13],totalToReimburse]])


#     output = BytesIO()
#     writer = pd.ExcelWriter(output, engine='xlsxwriter')
#     companySummary.to_excel(writer, sheet_name='Global',index = False, header=False)
    
#     for column in companySummary:
#         column_width = max(companySummary[column].astype(str).map(len).max(), len(column))
#         col_idx = companySummary.columns.get_loc(column)
#         writer.sheets['Global'].set_column(col_idx, col_idx, column_width)

#     employeesDetails.to_excel(writer, sheet_name='Detail',index = False)

#     for column in employeesDetails:
#         column_width = max(employeesDetails[column].astype(str).map(len).max(), len(column))
#         col_idx = employeesDetails.columns.get_loc(column)
#         writer.sheets['Detail'].set_column(col_idx, col_idx, column_width)

#     CG_summary_companies.append({'Company Name':companyName,
#                                                     'Company customer id':customerNumber,
#                                                     'Number of employees':numberOfEmployees,
#                                                     'Total number of charging sessions':numberOfSessions,
#                                                     'Total to reimburse to company (€)':totalToReimburse
#                                                 })
#     writer.close()

#     xlsx_data = output.getvalue()    
#     print('directory : '+directory)
#     print('companyName : '+str(companyName))
#     print('PERIOD : '+PERIOD)

#     s3_path = directory+'ChargeGuru - Avoir - ' + PERIOD+' - '+str(companyName)+'.xlsx'
#     push_file_to_s3(xlsx_data, s3_path)

#     return(CG_summary_companies)


################
# SF Methods
# ##############      

# returns sf access token
def get_sf_authorization():
    secret_name = "sf_raiden_prod"
    region_name = "eu-west-3"

    sf_secret = json.loads(get_aws_secret(secret_name, region_name))
    
    authorizeResponse = requests.post('https://raiden.my.salesforce.com/services/oauth2/token', data= {
        'grant_type': 'password',
        'client_id': sf_secret['SF_CLIENT_ID'],
        'client_secret': sf_secret['SF_CLIENT_SECRET'],
        'username': sf_secret['SF_USERNAME'],
        'password': sf_secret['SF_PASSWORD']+sf_secret['SF_SECURITY_TOKEN']
    })
    
    return authorizeResponse.json()

# to query salesforce data
def sf_query(auth_data, query):
    access_token = auth_data['access_token']
    instance_url = auth_data['instance_url']
    
    # get accounts from salesforce
    endpoint = instance_url+"/services/data/v57.0/query/?q="
    response = requests.get(endpoint+query, data=None, headers= {
        'Authorization': 'Bearer '+access_token
    })
    return response.json()


#################
# AWS Secret Manager Methods
#################
# get the secret from secret manager amazon
def get_aws_secret(secret_name, region_name):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.  
    return secret
    
# return api response for the lambda
def api_response(status_code, body_str): 
    return {
        "statusCode": status_code,
        'headers': {
            'Content-Type': 'application/json',
        },
        "body": body_str
    }

# get the secret from secret manager amazon
def get_secret():

    secret_name = "sf_raiden_prod"
    region_name = "eu-west-3"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.  
    return secret


# returns sf access token
def get_sf_authorization():
    sf_secret = json.loads(get_secret())
    
    authorizeResponse = requests.post('https://raiden.my.salesforce.com/services/oauth2/token', data= {
        'grant_type': 'password',
        'client_id': sf_secret['SF_CLIENT_ID'],
        'client_secret': sf_secret['SF_CLIENT_SECRET'],
        'username': sf_secret['SF_USERNAME'],
        'password': sf_secret['SF_PASSWORD']+sf_secret['SF_SECURITY_TOKEN']
    })
    
    return authorizeResponse.json()

# GET call to retrieve salesforce account by name
# def get_sf_account(auth_data, account_id):
#     access_token = auth_data['access_token']
#     instance_url = auth_data['instance_url']
    
#     # get accounts from salesforce
#     safe_account_id = account_id.replace("'", "\\'")
#     endpoint = instance_url+"/services/data/v57.0/query/?q="
#     query = "SELECT Id FROM Account WHERE Id='"+safe_account_id+"'"
#     account_response = requests.get(endpoint+query, data=None, headers= {
#         'Authorization': 'Bearer '+access_token
#     })
#     return account_response.json()


# PATCH call to update salesforce account benergised name
# not used for now
# def update_sf_accounts(auth_data, rows):

#     access_token = auth_data['access_token']
#     instance_url = auth_data['instance_url']
    
#     rows['CustomerNumber'].fillna('')
#     rows['LastName'].fillna('')
    
#     records = []
#     for index,row in rows.iterrows():
#         if(str(row['CustomerNumber']) != '' and str(row['LastName']) != ''):
#             records.append({
#                 "attributes" : {"type" : "Account"},
#                 "id": str(row['CustomerNumber']),
#                 "BeEnergised_Name__c": str(row['LastName'])
#             })

#     payload = json.dumps({"allOrNone": False, "records" : records})
#     endpoint = instance_url+"/services/data/v57.0/composite/sobjects/"
    
#     # check if account_id and beenergised_name are not null
#     update_response = requests.patch(endpoint, data=payload, headers={
#         "Authorization":'Bearer '+access_token,
#         "Content-Type":"application/json; charset=UTF-8",
#         "Accept": "application/json"
#     })
    
#     return 'update call done'

# push csv to s3
def push_csv_to_s3(csv, file_name):
    print('pushing csv to s3')
    print('file_name : '+file_name)
    encoded_csv_sig = csv.encode('utf-8-sig')
    bucket_name = os.environ['S3_BUCKET'] # work bucket
    print('bucket_name : ')
    print(bucket_name)
    s3_path = 'ampeco/'+file_name
    print('s3_path : ')
    print(s3_path)
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_csv_sig)

# push file to s3
def push_file_to_s3(file_content, s3_path):
    bucket_name = os.environ['S3_BUCKET'] # work bucket
    s3 = boto3.resource("s3")
    final_path = 'ampeco/'+s3_path
    obj = s3.Object(bucket_name, final_path)
    obj.put(Body=file_content)

def retrieve_csv_from_s3(object_key):
    bucket_name = os.environ['S3_BUCKET'] # work bucket
    client = boto3.client('s3')
    final_key = 'ampeco/'+object_key
    csv_obj = client.get_object(Bucket=bucket_name, Key=final_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    return csv_string


def get_duration_days_ago_date(end_date, duration):
    date_format = '%Y-%m-%d'
    end_date = datetime.strptime(end_date, date_format)
    result_date = end_date - timedelta(days=int(duration))
    return result_date.date().strftime(date_format)


def process_cpo_internal_old(config_variables):
    print('process_cpo_internal started')
   
    if config_variables:
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]
        EXPORT_START_DATE = config_variables["EXPORT_START_DATE"]
        EXPORT_END_DATE = config_variables["EXPORT_END_DATE"]
        PERIOD= config_variables["PERIOD"]
        MARGIN_RATE= int(config_variables["MARGIN_RATE"])
        directory = COUNTRY_CODE +'/'+ PERIOD+'/'
    guru_company = get_guru_company(COUNTRY_CODE)
   
    # query active mandates
    mandate_query = f"SELECT Id, Account_holder__c, Account_holder__r.Name, Badge_Number__c, Account_holder__r.Account_PersonAccount__c, Account_holder__r.Account_PersonAccount__r.Name, CreatedDate FROM SEPA_Mandate__c WHERE Status__c = 'Active' AND Guru_Company__r.Name = '{guru_company}'"
     
    # get active mandates
    mandate_response = sf_query(get_sf_authorization(), mandate_query)
    
    # check if the response has records, @todo => raise exception when not
    if('records' in mandate_response and len(mandate_response['records'])>0): 
        mandates = mandate_response['records'] #todo => manage the case when doubles customers are found
        queried_df = pd.DataFrame(mandates)
       
        SFData = pd.json_normalize(queried_df.to_dict(orient = 'records'))      

        del SFData['attributes.type']
        del SFData['attributes.url']
        del SFData['Account_holder__r.attributes.type']
        del SFData['Account_holder__r.attributes.url']
        del SFData['Account_holder__r.Account_PersonAccount__r.attributes.type']
        del SFData['Account_holder__r.Account_PersonAccount__r.attributes.url']

        SFData.rename(columns = {
            "Account_holder__c": "EmployeeSFID",
            "Badge_Number__c": "CardNumber",
            "Account_holder__r.Name": "EmployeeName",
            "Account_holder__r.Account_PersonAccount__c": "CompanySFID",
            "Account_holder__r.Account_PersonAccount__r.Name": "CompanyName",
            "CreatedDate" : "MandateCreationDate"
        },
        inplace = True)

        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        SFData.to_excel(writer,index = False, header=True)
        writer.close()
        xlsx_data = output.getvalue()
       
        #push_file_to_s3(xlsx_data, directory +'Paid_Badge_list_Salesforce' + datetime.today().strftime('%Y-%m-%d')+'.xlsx')
      
        
        companies = SFData[['CompanyName','CompanySFID']].copy()
        employees = SFData[['EmployeeName','EmployeeSFID','CompanyName','CompanySFID','CardNumber']].copy()

        #ac3
        # employees[column_language_dict.get(COUNTRY_CODE)[21]] =  0
        # employees[column_language_dict.get(COUNTRY_CODE)[20]] =  0
        # employees['number of sessions'] =  0

        employees['Total to bill (€)'] =  0
        employees['Total consumption (kWh)'] =  0
        employees['number of sessions'] =  0

        companiesList = companies.CompanyName.unique()
        EmployeesList = employees.EmployeeName.unique()
        
        ## TAA change
        chargelog_csv = retrieve_csv_from_s3(directory+'chargeguru_chargelog.csv')
       
        main_instance_data=pd.read_csv(StringIO(chargelog_csv), sep=',')
       
        
        export_end_date = convert_date(EXPORT_END_DATE)
        
        main_instance_data=main_instance_data.reset_index(drop=True)

       
        main_instance_data = main_instance_data.fillna('')

       
        main_instance_data['PurchaseTotalCosts'] = [round(main_instance_data['PurchaseTotalCosts'][i], 2) for i in range(len(main_instance_data['PurchaseTotalCosts']))] 
        main_instance_data['MeterTotal'] = [round(main_instance_data['MeterTotal'][i], 2) for i in range(len(main_instance_data['MeterTotal']))]
        main_instance_data['TotalCosts'] = [round(main_instance_data['TotalCosts'][i], 2) for i in range(len(main_instance_data['TotalCosts']))]

        del main_instance_data['SessionId']

       #ac3 
       # ORDER_Employee_Sheet = [column_language_dict.get(COUNTRY_CODE)[0],column_language_dict.get(COUNTRY_CODE)[14],column_language_dict.get(COUNTRY_CODE)[15],column_language_dict.get(COUNTRY_CODE)[16],column_language_dict.get(COUNTRY_CODE)[17],column_language_dict.get(COUNTRY_CODE)[18],column_language_dict.get(COUNTRY_CODE)[19]]
       
        ORDER_Employee_Sheet = [
        column_dict.get(COUNTRY_CODE)["Station Owner"],
        column_dict.get(COUNTRY_CODE)["Date and time of charging sessions"],
        column_dict.get(COUNTRY_CODE)["Charging point ID"],
        column_dict.get(COUNTRY_CODE)["Badge ID"],
        column_dict.get(COUNTRY_CODE)["Charging Duration"],
        column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"],
        column_dict.get(COUNTRY_CODE)["Session Amount (€)"]
    ]


        #to be deleted??
        #ORDER_Company_Sheet = [column_language_dict.get(COUNTRY_CODE)[10],column_language_dict.get(COUNTRY_CODE)[16],column_language_dict.get(COUNTRY_CODE)[5],column_language_dict.get(COUNTRY_CODE)[20],column_language_dict.get(COUNTRY_CODE)[11]]

       
        CG_summary_employees = []
        CG_summary_companies = []

        for i in range(len(companiesList)):
            currentCO = companiesList[i]
          
            currentCOfiledir = directory
           
            currentCOEmployees = employees[employees['CompanyName'] == currentCO].EmployeeName.unique()
            for j in range(len(currentCOEmployees)):
                currentEmployee = currentCOEmployees[j]
                currentEmployeeCard = employees['CardNumber'][employees.index[employees['EmployeeName'] == currentEmployee].tolist()[0]]
                currentEmployeeData = main_instance_data [main_instance_data['CardNumber'] == currentEmployeeCard].copy()
                currentEmployeeData= currentEmployeeData.reset_index(drop=True)
                #ac3

                currentEmployeeData.rename(columns = {
                "StationOwnerName": column_dict.get(COUNTRY_CODE)["Station Owner"],
                "End": column_dict.get(COUNTRY_CODE)["Date and time of charging sessions"],
                "EVSEId": column_dict.get(COUNTRY_CODE)["Charging point ID"],
                "Duration": column_dict.get(COUNTRY_CODE)["Charging Duration"], #tocheck (in file we have DurationTotal)
                "MeterTotal": column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"],
                "TotalCosts": column_dict.get(COUNTRY_CODE)["Session Amount (€)"],
                "CardNumber" : column_dict.get(COUNTRY_CODE)["Badge ID"]
                },
                inplace = True)
                del currentEmployeeData['PurchaseTotalCosts']
                currentEmployeeData = currentEmployeeData[ORDER_Employee_Sheet]
                #ac3
                # amountForEmployee = currentEmployeeData[column_language_dict.get(COUNTRY_CODE)[1]].sum()
                # employees.loc[employees.CardNumber == currentEmployeeCard, column_language_dict.get(COUNTRY_CODE)[21]] = amountForEmployee
                # employees.loc[employees.CardNumber == currentEmployeeCard, column_language_dict.get(COUNTRY_CODE)[20]] = currentEmployeeData[column_language_dict.get(COUNTRY_CODE)[18]].sum()
                # employees.loc[employees.CardNumber == currentEmployeeCard, 'number of sessions'] = len(currentEmployeeData[column_language_dict.get(COUNTRY_CODE)[18]])
               

                amountForEmployee = currentEmployeeData[column_dict.get(COUNTRY_CODE)["Session Amount (€)"]].sum()
                employees.loc[employees.CardNumber == currentEmployeeCard, 'Total to bill (€)'] = amountForEmployee
                employees.loc[employees.CardNumber == currentEmployeeCard, 'Total consumption (kWh)'] = currentEmployeeData[column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"]].sum()
                employees.loc[employees.CardNumber == currentEmployeeCard, 'number of sessions'] = len(currentEmployeeData[column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"]])
                

                currentEmployeeNumber = employees[employees['CardNumber'] == currentEmployeeCard]['EmployeeSFID'].tolist()[0]
                


                writeEmployeeOutput(config_variables, currentCO, currentEmployee, currentEmployeeData, currentEmployeeNumber, currentEmployeeCard)
               

        for i in range(len(companiesList)):
            currentCO = companiesList[i]
            currentCONumber = ''
            if(len(companies[companies['CompanyName'] == currentCO]['CompanySFID'].tolist()) > 0):
                currentCONumber = companies[companies['CompanyName'] == currentCO]['CompanySFID'].tolist()[0]
           
            currentCOfiledir = directory
           
            currentCOEmployees = employees[employees['CompanyName'] == currentCO].copy()
            currentCOEmployees=currentCOEmployees.reset_index(drop=True)
            for j in range(len(currentCOEmployees['EmployeeName'])):
                #ac2 'Company Name':currentCO,'Company customer id':currentCONumber,
                CG_summary_employees.append({
                                                                    
                                                                    'Name':currentCOEmployees['EmployeeName'][j],
                                                                    'Customer id':currentCOEmployees['EmployeeSFID'][j],
                                                                    'Card number':currentCOEmployees['CardNumber'][j],
                                                                    'Number of sessions':currentCOEmployees['number of sessions'][j],
                                                                    #ac3
                                                                    #'Total to invoice (€)':currentCOEmployees[column_language_dict.get(COUNTRY_CODE)[21]][j]
                                                                    'Total to bill (€)':currentCOEmployees['Total to bill (€)'][j]
                                                                    })
            del currentCOEmployees['EmployeeSFID']
            del currentCOEmployees['CompanyName']
            del currentCOEmployees['CompanySFID']
            #to be deleted start
            # currentCOEmployees.rename(columns = {
            #     "EmployeeName": column_language_dict.get(COUNTRY_CODE)[10],
            #     "CardNumber": column_language_dict.get(COUNTRY_CODE)[16],
            #     "Total to invoice (€)": column_language_dict.get(COUNTRY_CODE)[21],
            #     "Total consumption (kWh)": column_language_dict.get(COUNTRY_CODE)[20],
            #     "number of sessions": column_language_dict.get(COUNTRY_CODE)[5],
            #     },
            #     inplace = True)
   
          
            # currentCOEmployees[column_language_dict.get(COUNTRY_CODE)[11]] = [round(currentCOEmployees[column_language_dict.get(COUNTRY_CODE)[21]][j]/( 1 + (int(float(MARGIN_RATE)) / 100) ), 2) for j in range(len(currentCOEmployees[column_language_dict.get(COUNTRY_CODE)[21]]))]
            # del currentCOEmployees[column_language_dict.get(COUNTRY_CODE)[21]]
            # currentCOEmployees = currentCOEmployees[ORDER_Company_Sheet]
          #to be deleted end
            #CG_summary_companies = writeCompanyOutput(config_variables,currentCO, currentCOEmployees, currentCONumber, CG_summary_companies)
       
            
        # convert to dataframe
        CG_summary_employees = pd.DataFrame(CG_summary_employees)
        #CG_summary_companies = pd.DataFrame(CG_summary_companies)
        print(CG_summary_employees)
       # print(CG_summary_companies)
        
        # People_not_to_invoice = CG_summary_employees[CG_summary_employees["Total to invoice (€)"] == 0]
        # People_not_to_invoice = People_not_to_invoice.reset_index(drop=True)
        

        # output = BytesIO()
        # writer = pd.ExcelWriter(output, engine='xlsxwriter')
        # People_not_to_invoice.to_excel(writer,index = False, header=True)
        # writer.close()
        # xlsx_data = output.getvalue()
       
        # push_file_to_s3(xlsx_data, directory+'People_not_to_invoice_' + datetime.today().strftime('%Y-%m-%d')+'.xlsx')

        
        CG_summary_employees = CG_summary_employees[CG_summary_employees['Total to bill (€)']>0]
        CG_summary_employees = CG_summary_employees.reset_index(drop=True)
        
        # convert to dataframe
       # CG_summary_companies = CG_summary_companies[CG_summary_companies['Total to reimburse to company (€)']>0]
        #CG_summary_companies = CG_summary_companies.reset_index(drop=True)

        #summary_companies_csv = CG_summary_companies.to_csv(sep=';', index = False, header=True)
        summary_employees_csv = CG_summary_employees.to_csv(sep=';', index = False, header=True)
       
        #push_csv_to_s3(summary_companies_csv, directory+PERIOD+' - Full Summary_companies.csv')
        push_csv_to_s3(summary_employees_csv, directory+PERIOD+' - Full Summary_employees.csv')
      

def process_cpo_internal_1(config_variables):

    if config_variables:
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]
        EXPORT_START_DATE = config_variables["EXPORT_START_DATE"]
        EXPORT_END_DATE = config_variables["EXPORT_END_DATE"]
        PERIOD= config_variables["PERIOD"]
        MARGIN_RATE= int(config_variables["MARGIN_RATE"])
        directory = COUNTRY_CODE + '/' + PERIOD + '/'
   
    
    # Load CSV data ## TO BE CHANGED following 2 lines!!!
    chargelog_csv = retrieve_csv_from_s3(directory + 'chargeguru_chargelog_test.csv')
    main_instance_data = pd.read_csv(StringIO(chargelog_csv), sep=';')

  

    # Fill missing values with empty strings
    main_instance_data = main_instance_data.fillna('')


    # Drop unnecessary columns
    del main_instance_data['SessionId']

    # Create DataFrames for employees
    employees = main_instance_data[['SFAccountId', 'SepaMandateCardNumber','ClientName']].drop_duplicates()
    employees['Total to bill (€)'] = 0
    employees['Total consumption (kWh)'] = 0
    employees['number of sessions'] = 0

    ORDER_Employee_Sheet = [
        column_dict.get(COUNTRY_CODE)["Station Owner"],
        column_dict.get(COUNTRY_CODE)["Date and time of charging sessions"],
        column_dict.get(COUNTRY_CODE)["Charging point ID"],
        column_dict.get(COUNTRY_CODE)["Badge ID"],
        column_dict.get(COUNTRY_CODE)["Charging Duration"],
        column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"],
        column_dict.get(COUNTRY_CODE)["Session Amount (€)"]
    ]

    # Process each employee
    for index, row in employees.iterrows():
        currentEmployeeCard = row['SepaMandateCardNumber']
        ## to be corrected??
        currentEmployeeData = main_instance_data[main_instance_data['CardNumber'] == currentEmployeeCard].copy()
        currentEmployeeData = currentEmployeeData.reset_index(drop=True)

        currentEmployeeData.rename(columns = {
                "StationOwnerName": column_dict.get(COUNTRY_CODE)["Station Owner"],
                "End": column_dict.get(COUNTRY_CODE)["Date and time of charging sessions"],
                "EVSEId": column_dict.get(COUNTRY_CODE)["Charging point ID"],
                "Duration": column_dict.get(COUNTRY_CODE)["Charging Duration"], #tocheck (in file we have DurationTotal)
                "MeterTotal": column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"],
                "TotalCosts": column_dict.get(COUNTRY_CODE)["Session Amount (€)"],
                "CardNumber" : column_dict.get(COUNTRY_CODE)["Badge ID"]
                },
                inplace = True)
        del currentEmployeeData['PurchaseTotalCosts']
        currentEmployeeData = currentEmployeeData[ORDER_Employee_Sheet]

        amountForEmployee = currentEmployeeData[column_dict.get(COUNTRY_CODE)["Session Amount (€)"]].sum()
        employees.loc[employees['SepaMandateCardNumber'] == currentEmployeeCard, 'Total to bill (€)']  = amountForEmployee
        employees.loc[employees['SepaMandateCardNumber'] == currentEmployeeCard, 'Total consumption (kWh)']  = currentEmployeeData[column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"]].sum()
        employees.loc[employees['SepaMandateCardNumber'] == currentEmployeeCard, 'number of sessions'] = len(currentEmployeeData[column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"]])
                

        # Export employee data
        writeEmployeeOutput(config_variables, row['ClientName'], currentEmployeeData, row['SFAccountId'], currentEmployeeCard)

    # Filter out employees with no charges
    CG_summary_employees = employees[employees['Total to bill (€)'] > 0].reset_index(drop=True)

    # Convert to CSV
    summary_employees_csv = CG_summary_employees.to_csv(sep=';', index=False, header=True)

    # Push summary CSV to S3
    push_csv_to_s3(summary_employees_csv, directory + PERIOD + ' - Full Summary_employees.csv')



def process_cpo_internal(config_variables):
    print('process_cpo_internal started')
   
    if config_variables:
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]
        EXPORT_START_DATE = config_variables["EXPORT_START_DATE"]
        EXPORT_END_DATE = config_variables["EXPORT_END_DATE"]
        PERIOD= config_variables["PERIOD"]
        MARGIN_RATE= int(config_variables["MARGIN_RATE"])
        directory = COUNTRY_CODE +'/'+ PERIOD+'/'
    

    chargelog_csv = retrieve_csv_from_s3(directory+'chargeguru_chargelog_test.csv')    
    main_instance_data=pd.read_csv(StringIO(chargelog_csv), sep=';')
    main_instance_data=main_instance_data.reset_index(drop=True)
    main_instance_data = main_instance_data.fillna('')
    
    employees =  main_instance_data[['SFAccountId', 'SepaMandateCardNumber','ClientName']].copy()
    employees = employees.drop_duplicates()
    employees['Total to bill (€)'] =  0
    employees['Total consumption (kWh)'] =  0
    employees['number of sessions'] =  0

    del main_instance_data['SessionId']

      
    ORDER_Employee_Sheet = [
        column_dict.get(COUNTRY_CODE)["Station Owner"],
        column_dict.get(COUNTRY_CODE)["Date and time of charging sessions"],
        column_dict.get(COUNTRY_CODE)["Charging point ID"],
        column_dict.get(COUNTRY_CODE)["Badge ID"],
        column_dict.get(COUNTRY_CODE)["Charging Duration"],
        column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"],
        column_dict.get(COUNTRY_CODE)["Session Amount (€)"]
    ]


    CG_summary_employees = []
      
    for index, row in employees.iterrows():
        currentEmployeeCard = row['SepaMandateCardNumber']
        currentEmployee = row['ClientName']
        currentEmployeeData = main_instance_data[main_instance_data['SepaMandateCardNumber'] == currentEmployeeCard].copy()
        currentEmployeeData= currentEmployeeData.reset_index(drop=True)

        currentEmployeeData.rename(columns = {
            "StationOwnerName": column_dict.get(COUNTRY_CODE)["Station Owner"],
            "End": column_dict.get(COUNTRY_CODE)["Date and time of charging sessions"],
            "EVSEId": column_dict.get(COUNTRY_CODE)["Charging point ID"],
            "Duration": column_dict.get(COUNTRY_CODE)["Charging Duration"], #tocheck (in file we have DurationTotal)
            "MeterTotal": column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"],
            "TotalCosts": column_dict.get(COUNTRY_CODE)["Session Amount (€)"],
            #"CardNumber" : column_dict.get(COUNTRY_CODE)["Badge ID"]
            "SepaMandateCardNumber" : column_dict.get(COUNTRY_CODE)["Badge ID"]
            },
            inplace = True)
        del currentEmployeeData['PurchaseTotalCosts']
        currentEmployeeData = currentEmployeeData[ORDER_Employee_Sheet]


        amountForEmployee = currentEmployeeData[column_dict.get(COUNTRY_CODE)["Session Amount (€)"]].sum()
        employees.loc[employees.SepaMandateCardNumber == currentEmployeeCard, 'Total to bill (€)']  = amountForEmployee
        employees.loc[employees.SepaMandateCardNumber == currentEmployeeCard, 'Total consumption (kWh)']  = currentEmployeeData[column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"]].sum()
        employees.loc[employees.SepaMandateCardNumber == currentEmployeeCard, 'number of sessions'] = len(currentEmployeeData[column_dict.get(COUNTRY_CODE)["Energy consumption (kWh)"]])
           
        currentEmployeeNumber = row['SFAccountId']
                
        writeEmployeeOutput(config_variables, currentEmployee, currentEmployeeData, currentEmployeeNumber, currentEmployeeCard)

   
   

    for index, row in employees.iterrows():
            # Append each row to the list
            CG_summary_employees.append({
                'Name': row['ClientName'],
                'Customer id': row['SFAccountId'],
                'Card number': row['SepaMandateCardNumber'],
                'Number of sessions': row['number of sessions'],
                'Total to bill (€)': round(row['Total to bill (€)'], 2) 
            })
           
    del employees['SFAccountId']

    # convert to dataframe
    CG_summary_employees = pd.DataFrame(CG_summary_employees)
    print(CG_summary_employees)
      
    CG_summary_employees = CG_summary_employees[CG_summary_employees['Total to bill (€)']>0]
    CG_summary_employees = CG_summary_employees.reset_index(drop=True)

    summary_employees_csv = CG_summary_employees.to_csv(sep=';', index = False, header=True)
    push_csv_to_s3(summary_employees_csv, directory+PERIOD+' - Full Summary_employees.csv')

def convert_date(original_date_str): 
    
    # Convert the date string to a datetime object
    original_date = datetime.strptime(original_date_str, '%Y-%m-%d')

    # Set the time to the end of the day (23:59:59.999)
    end_of_day = original_date.replace(hour=23, minute=59, second=59, microsecond=999)

    # Convert the datetime object back to a string with the desired format
    end_of_day_str = end_of_day.isoformat()

    return end_of_day_str
    #print(end_of_day_str)  # Output: 2024-02-29T23:59:59.999

def get_from_clause(country_code):
    return f"cpo.be_energised_{country_code}_main.wizard__chargelog AS wizl " \
           f"LEFT JOIN cpo.be_energised_{country_code}_main.cpo__charge_log AS cpol ON wizl.UUID = cpol.id"


def get_payload(warehouse_id, country_code,export_end_date):
    export_start_date = get_duration_days_ago_date(export_end_date, DURATION)
    export_end_date = convert_date(export_end_date)
    payload = {}
    country_code = country_code.lower()
    #from_clause = get_from_clause(country_code)
    cpo_environment = get_cpo_environment(country_code)
    

    payload = {
        "warehouse_id": warehouse_id,
        "statement": f"""SELECT
                    se.session_id AS SessionId,
                    so.station_owner_name AS StationOwnerName,
                    cc.connector_code AS EVSEId,
                    REPLACE(am.medium_code, 'O', '0') AS CardNumber,
                    --se.session_start_date AS Start,
                    se.session_end_date AS End,
                    CONCAT(
                        LPAD(FLOOR(DATEDIFF(SECOND, se.session_start_date, se.session_end_date) / (60*60)), 2, '0'), ':', 
                        LPAD(FLOOR(MOD(DATEDIFF(SECOND, se.session_start_date, se.session_end_date) / 60, 60)), 2, '0'), ':', 
                        LPAD(FLOOR(MOD(DATEDIFF(SECOND, se.session_start_date, se.session_end_date), 60)), 2, '0')
                    ) AS `Duration`,
                    ROUND(se.session_energy_Wh / 1000, 2) AS MeterTotal,
                    ROUND(COALESCE(se.cost_amount, 0), 2) AS TotalCosts,
                    ROUND(COALESCE(se.purchase_cost_amount_eur, 0), 2) AS PurchaseTotalCosts
                    --se.reimbursement_use_case AS reimbursement_use_case
                    FROM cpo.centralised_data.sessions AS se
                    LEFT JOIN cpo.centralised_data.connectors AS cc ON cc.connector_id = se.connector_id
                    LEFT JOIN cpo.centralised_data.station_owners AS so ON so.station_owner_id = cc.station_owner_id
                    LEFT JOIN cpo.centralised_data.authentication_media AS am ON am.medium_id = se.medium_id
                    WHERE se.cpo_environment IN ('{cpo_environment}')
                    AND COALESCE(se.record_end_date, se.session_end_date) >= '{export_start_date}'  AND COALESCE(se.record_end_date, se.session_end_date) <= '{export_end_date}'
                    AND se.cost_amount > 0
                    AND se.session_energy_Wh > 0
                    --AND am.medium_code LIKE '%CG%'
                    AND se.reimbursement_use_case IN ('Internal')
                    AND COALESCE(se.anomaly_detected, '') NOT IN ('high-energy', 'provider-warning')
                    ORDER BY COALESCE(se.record_end_date, se.session_end_date) ASC""",
        "wait_timeout": "0s",
        "disposition": "EXTERNAL_LINKS",
        "format": "CSV"
    }

    return json.dumps(payload)
    
def get_data_from_databricks(warehouse_url, warehouse_id, warehouse_token,config_variables):
    if config_variables:
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]
        EXPORT_END_DATE = config_variables["EXPORT_END_DATE"]
  
    api_url = f"{warehouse_url}/api/2.0/sql/statements/"
    
    payload = get_payload(warehouse_id,COUNTRY_CODE,EXPORT_END_DATE)

    headers = {
    'Content-Type': 'application/json',
    'Authorization': warehouse_token
    }
    
    try:
        # Make the API request using POST
        response = requests.request("POST", api_url, headers=headers, data=payload,)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the response JSON
            response_json = response.json()

            if 'statement_id' in response_json  and response_json ['statement_id']:
                print('statement_id is in body')
                statement_id =  response_json ['statement_id']
                print(response_json )
                data = {
                    'statement_id': statement_id,
                    'config_variables': config_variables     
                }

                return api_response(200, json.dumps(data))
            else:
                return api_response(500, 'get_data_from_databricks statement_id not found in the response.')
        else:
            return api_response(response.status_code, response.text)

    except Exception as e:
        print(f"Error in get_data_from_databricks: {str(e)}")
        return api_response(500, f'Error: {str(e)}')
    

# Function to download the file generated by Databricks

def download_export_databricks(download_url,config_variables):

    print('Download export started')
    print(download_url)
    if config_variables:
      
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]  
        PERIOD= config_variables["PERIOD"]
        directory = COUNTRY_CODE +'/'+ PERIOD+'/'

    headers = {
        'content-type': 'application/x-www-form-urlencoded'
    }
    try:
        # Make a GET request to the download URL
        download_response = requests.get(download_url, headers=headers)

        # Check if the request was successful (status code 200)
        if download_response.status_code == 200:

            print('download_response.text')
            print(download_url)
            push_csv_to_s3(download_response.text, directory + 'chargeguru_chargelog.csv')
            return api_response(200, 'Processing export ' + str(download_url))
        else:
            return api_response(download_response.status_code, 'Failed to download. Status Code: ' + str(download_response.status_code))
    except Exception as e:
        return api_response(500, 'Error during download: ' + str(e))


def get_download_url(warehouse_url, warehouse_token, statement_id, config_variables):
    # Databricks API endpoint for getting statement details
    api_url = f"{warehouse_url}/api/2.0/sql/statements/{statement_id}"

    # Headers with authorization token
    headers = {
        'Content-Type': 'application/json',
        'Authorization': warehouse_token
    }
    is_succeeded = False
    try:
      
        while not is_succeeded:          
             # Make the API request using POST
            response = requests.get(api_url, headers=headers)
            is_succeeded = response.json()["status"]["state"] == "SUCCEEDED"
            
        if response.status_code == 200:
            # Parse the response JSON
            response_json = response.json()

            # Check if 'external_links' is present in the response
            if 'result' in response_json and response_json['result'] and 'external_links' in response_json['result'] and response_json['result']['external_links'] and response_json['result']['external_links'][0] and 'external_link' in response_json['result']['external_links'][0]:
                # Extract and return the 'external_link'
                external_link = response_json['result']['external_links'][0]['external_link']
                print(external_link)
                data =  {
                    'download_url': external_link,
                    'config_variables': config_variables      
                }

                return api_response(200, json.dumps(data))
            else:
                return api_response(500, 'get_download_url External link not found in the response.')
        else:
            print(f"Error during get_download_url")
            print(response.text)
            return api_response(response.status_code, response.text)

    except Exception as e:
        print(f"Error during get_download_url: {str(e)}")
        return api_response(500, f'Error during get_download_url: {str(e)}')

  

def get_guru_company(country_code):
   
    mapping = {
        'FR': 'RAIDEN',
        'ES': 'RAIDEN IBERICA',
        'PT': 'LUSO RAIDEN',
        'IT': 'RAIDEN ITALY',
        'DE': 'RAIDEN DEUTSCHLAND',
        'UK': 'RAIDEN CHARGING UK LTD',
        'IR': 'RAIDEN CHARGING IRELAND LIMITED',
        'BE': 'RAIDEN BELGIUM'
    }
    return mapping.get(country_code.upper(), 'Unknown')

#get cpo_environment
def get_cpo_environment(country_code):
     return 'ampeco_' + country_code.lower()

    

######## GLOBAL VARS #########

column_language_dict = {
    "FR": ["Propriétaire de la borne","Montant de la recharge (€)","Montant de la recharge (€)","Nom de l'entreprise", "Période","Nombre de sessions de recharge", "Total à régler (€)","Numéro de client","ChargeGuru - Facture - ","ChargeGuru - Avoir - ","Nom et prénom de l'utilisateur","Revenu total de la recharge (€)","Nombre de salariés","Revenu total des recharges (€)","Date et heure des sessions de recharge","ID du points de charge", "Numéro du badge","Durée de la recharge","Consommation d'énergie (kWh)","Montant de la recharge (€)","Consommation totale (kWh)","Total facturé à l'usagé (€)"],
    "ES": ["Propietario del punto de recarga","Coste de la sesión de carga (€)","Nombre de usuario", "Nombre de empresa", "Periodo", "Número de sesiones de carga","Total a pagar (€)", "Número de cliente" , "ChargeGuru - Billing - ", "ChargeGuru - Credit Note - ","Nombre y apellidos del usuario","Ingresos totales por recarga (€)","Número de empleados","Ingresos totales por recargas (euros)", "Fecha y hora de las sesiones de carga", "Identificación del punto de carga","ID Tarjeta RFID", "Duración de la recarga", "Energía consumida (kWh)" , "Coste de la sesión de carga (€)", "Consumo total (kWh)", "Total facturado al usuario (€)"],
    "PT": ["Proprietário do ponto de carregamento","Custo da sessão de carregamento (€)","Nome do utilizador","Nome de empresa", "Período","Número de sessões de carregamento","Total a pagar (€)","Número de cliente","ChargeGuru - Billing - ","ChargeGuru - Credit Note - ","Nome e apelido do utilizador","Total das receitas de recarga (€)","Número de empregados","Receitas totais das recargas (€)","Data e hora das sessões de carregamento","Identificação do ponto de carregamento","Cartão de identificação RFID","Duração do carregamento","Energia consumida (kWh)","Custo da sessão de carregamento (€)","Consumo total (kWh)","Total facturado ao utilizador (€)"],
    "IT": ["Proprietario della stazione di ricarica","Importo della sessione di ricarica (€)","Nome dell'utente","Nome dell'azienda", "Periodo","Numero di sessioni di ricarica","Totale da pagare (€)","Numero cliente","ChargeGuru - Billing - ","ChargeGuru - Credit Note - ","Nome e cognome dell'utente","Totale ricavi da ricarica (€)","Numero di dipendenti","Ricavo totale delle ricariche (€)","Data e ora delle sessioni di ricarica","ID del punto di ricarica","ID tessera RFiD","Durata della sessione di ricarica","Energia consumata (kWh)","Importo della sessione di ricarica (€)","Consumo totale (kWh)","Totale fatturato all'utente (€)"],
    "UK": ["Owner of the charging station","Charging session amount (€)","User name","Company name", "Period","Number of charging sessions","Total to be paid (€)","Customer number","ChargeGuru - Billing - ","ChargeGuru - Credit Note - ","Name and surname of the user","Total income of the charging (€)","Number of employees","Total income of the sessions (€)","Date and time of charging sessions","Charging point ID","Badge ID","Duration of charging sessions","Energy consumption (kWh)","Charging session amount (€)","Total Consumption (kWh)","Total to invoice (€)"],
    "IR": ["Owner of the charging station","Charging session amount (€)","User name","Company name", "Period","Number of charging sessions","Total to be paid (€)","Customer number","ChargeGuru - Billing - ","ChargeGuru - Credit Note - ","Name and surname of the user","Total income of the charging (€)","Number of employees","Total income of the sessions (€)","Date and time of charging sessions","Charging point ID","Badge ID","Duration of charging sessions","Energy consumption (kWh)","Charging session amount (€)","Total Consumption (kWh)","Total to invoice (€)"],
}


""" "BE": ["","","","", "","","","","","","","","","","","","","","","",""],
    "DE": ["","","","", "","","","","","","","","","","","","","","","",""] """


column_dict = {
    "FR": {
        "Station Owner": "Propriétaire de la borne",
        "Session Amount (€)": "Montant de la recharge (€)",
        "Company Name": "Nom de l'entreprise",
        "Period": "Période",
        "Number of charging sessions": "Nombre de sessions de recharge",
        "Total to reimburse (€)": "Total à régler (€)",
        "Customer Number": "Numéro de client",
        "ChargeGuru Invoice": "ChargeGuru - Facture -",
        "ChargeGuru Credit Note": "ChargeGuru - Avoir -",
        "Name and surname of the user": "Nom et prénom de l'utilisateur",
        "Total income of the charging (€)": "Revenu total de la recharge (€)",
        "Number of employees": "Nombre de salariés",
        "Date and time of charging sessions": "Date et heure des sessions de recharge",
        "Charging point ID": "ID du points de charge",
        "Badge ID": "Numéro du badge",
        "Charging Duration": "Durée de la recharge",
        "Energy consumption (kWh)": "Consommation d'énergie (kWh)",
        "Total Consumption (kWh)": "Consommation totale (kWh)",
        "Total income of the sessions (€)": "Revenu total des recharges (€)",
        "Total to bill (€)": "Total facturé à l'usagé (€)"
    },
    "ES": {
        "Station Owner": "Propietario del punto de recarga",
        "Session Amount (€)": "Coste de la sesión de carga (€)",
        "Company Name": "Nombre de empresa",
        "Period": "Periodo",
        "Number of charging sessions": "Número de sesiones de carga",
        "Total to reimburse (€)": "Total a pagar (€)",
        "Customer Number": "Número de cliente",
        "ChargeGuru Invoice": "ChargeGuru - Billing -",
        "ChargeGuru Credit Note": "ChargeGuru - Credit Note -",
        "Name and surname of the user": "Nombre y apellidos del usuario",
        "Total income of the charging (€)": "Ingresos totales por recarga (€)",
        "Number of employees": "Número de empleados",
        "Date and time of charging sessions": "Fecha y hora de las sesiones de carga",
        "Charging point ID": "Identificación del punto de carga",
        "Badge ID": "ID Tarjeta RFID",
        "Charging Duration": "Duración de la recarga",
        "Energy consumption (kWh)": "Energía consumida (kWh)",
        "Total Consumption (kWh)": "Consumo total (kWh)",
        "Total income of the sessions (€)": "Ingresos totales por recargas (euros)",
        "Total to bill (€)": "Total facturado al usuario (€)"
    },
    "PT": {
        "Station Owner": "Proprietário do ponto de carregamento",
        "Session Amount (€)": "Custo da sessão de carregamento (€)",
        "Company Name": "Nome de empresa",
        "Period": "Período",
        "Number of charging sessions": "Número de sessões de carregamento",
        "Total to reimburse (€)": "Total a pagar (€)",
        "Customer Number": "Número de cliente",
        "ChargeGuru Invoice": "ChargeGuru - Billing -",
        "ChargeGuru Credit Note": "ChargeGuru - Credit Note -",
        "Name and surname of the user": "Nome e apelido do utilizador",
        "Total income of the charging (€)": "Total das receitas de recarga (€)",
        "Number of employees": "Número de empregados",
        "Date and time of charging sessions": "Data e hora das sessões de carregamento",
        "Charging point ID": "Identificação do ponto de carregamento",
        "Badge ID": "Cartão de identificação RFID",
        "Charging Duration": "Duração do carregamento",
        "Energy consumption (kWh)": "Energia consumida (kWh)",
        "Total Consumption (kWh)": "Consumo total (kWh)",
        "Total income of the sessions (€)": "Receitas totais das recargas (€)",
        "Total to bill (€)": "Total facturado ao utilizador (€)"
    },
    "IT": {
        "Station Owner": "Proprietario della stazione di ricarica",
        "Session Amount (€)": "Importo della sessione di ricarica (€)",
        "Company Name": "Nome dell'azienda",
        "Period": "Periodo",
        "Number of charging sessions": "Numero di sessioni di ricarica",
        "Total to reimburse (€)": "Totale da pagare (€)",
        "Customer Number": "Numero cliente",
        "ChargeGuru Invoice": "ChargeGuru - Billing -",
        "ChargeGuru Credit Note": "ChargeGuru - Credit Note -",
        "Name and surname of the user": "Nome e cognome dell'utente",
        "Total income of the charging (€)": "Totale ricavi da ricarica (€)",
        "Number of employees": "Numero di dipendenti",
        "Date and time of charging sessions": "Data e ora delle sessioni di ricarica",
        "Charging point ID": "ID del punto di ricarica",
        "Badge ID": "ID tessera RFiD",
        "Charging Duration": "Durata della sessione di ricarica",
        "Energy consumption (kWh)": "Energia consumata (kWh)",
        "Total Consumption (kWh)": "Consumo totale (kWh)",
        "Total income of the sessions (€)": "Ricavo totale delle ricariche (€)",
        "Total to bill (€)": "Totale fatturato all'utente (€)"
    },
    "UK": {
        "Station Owner": "Owner of the charging station",
        "Session Amount (€)": "Charging session amount (€)",
        "Company Name": "Company name",
        "Period": "Period",
        "Number of charging sessions": "Number of charging sessions",
        "Total to reimburse (€)": "Total to be paid (€)",
        "Customer Number": "Customer number",
        "ChargeGuru Invoice": "ChargeGuru - Billing -",
        "ChargeGuru Credit Note": "ChargeGuru - Credit Note -",
        "Name and surname of the user": "Name and surname of the user",
        "Total income of the charging (€)": "Total income of the charging (€)",
        "Number of employees": "Number of employees",
        "Date and time of charging sessions": "Date and time of charging sessions",
        "Charging point ID": "Charging point ID",
        "Badge ID": "Badge ID",
        "Charging Duration": "Duration of charging sessions",
        "Energy consumption (kWh)": "Energy consumption (kWh)",
        "Total Consumption (kWh)": "Total Consumption (kWh)",
        "Total income of the sessions (€)": "Total income of the sessions (€)",
        "Total to bill (€)": "Total to bill (€)"
    },
    "IR": {
        "Station Owner": "Owner of the charging station",
        "Session Amount (€)": "Charging session amount (€)",
        "Company Name": "Company name",
        "Period": "Period",
        "Number of charging sessions": "Number of charging sessions",
        "Total to reimburse (€)": "Total to be paid (€)",
        "Customer Number": "Customer number",
        "ChargeGuru Invoice": "ChargeGuru - Billing -",
        "ChargeGuru Credit Note": "ChargeGuru - Credit Note -",
        "Name and surname of the user": "Name and surname of the user",
        "Total income of the charging (€)": "Total income of the charging (€)",
        "Number of employees": "Number of employees",
        "Date and time of charging sessions": "Date and time of charging sessions",
        "Charging point ID": "Charging point ID",
        "Badge ID": "Badge ID",
        "Charging Duration": "Duration of charging sessions",
        "Energy consumption (kWh)": "Energy consumption (kWh)",
        "Total Consumption (kWh)": "Total Consumption (kWh)",
        "Total income of the sessions (€)": "Total income of the sessions (€)",
        "Total to bill (€)": "Total to bill (€)"
    }
}
