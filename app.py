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
from typing import Tuple



#################    INPUT  #########################################
PERIOD = os.environ["PERIOD"]
EXPORT_START_DATE = os.environ["EXPORT_START_DATE"]
EXPORT_END_DATE = os.environ["EXPORT_END_DATE"]
COUNTRY_CODE= os.environ['COUNTRY_CODE']
DURATION = os.environ["EXPORT_DURATION"]
MARGIN_RATE = int(os.environ["MARGIN_RATE"])

#CALLBACK_URL = os.environ['LAMBDA_URL']
BASE_URL = os.environ['BASE_URL']
QUERY_ID = os.environ['QUERY_ID']
WAREHOUSE_URL = os.environ['WAREHOUSE_URL']
WAREHOUSE_ID = os.environ['WAREHOUSE_ID']
WAREHOUSE_TOKEN = os.environ['WAREHOUSE_TOKEN']
EXPORT_API_RESULT_FILE_NAME =  'chargeguru_chargelog.csv'
USE_CASE = 'FABS'
######################################################################

# # lambda entry point
def lambda_handler(event,context):
    
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
    
    get_data_by_middleware(BASE_URL, QUERY_ID, USE_CASE, config_json)
    process_cpo_data(config_json)  


def get_data_by_middleware(base_url,query_id,use_case,config_variables):
   
   #url = 'https://partner.chargeguru.com/proxy?block=undefined&queryId=5d4b8dff-4202-44d7-979d-c8ee713d15d6?o=649459631911710&p_cpo_environment=ampeco_es&p_end_date=2024-08-30&p_start_date=2024-08-01 '
    #base_url = "https://partner.chargeguru.com/proxy"

    #query_id = "5d4b8dff-4202-44d7-979d-c8ee713d15d6"

    #query_id = "a309f299-520d-4f04-8b84-cb2c85392358"

    # cpo_environment = "ampeco_es"
    # start_date = "2024-08-01"
    # end_date = "2024-08-30"

    if config_variables:
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]
        EXPORT_START_DATE = config_variables["EXPORT_START_DATE"]
        EXPORT_END_DATE = config_variables["EXPORT_END_DATE"]
        start_date = get_next_day(EXPORT_START_DATE)
        end_date = get_next_day(EXPORT_END_DATE)
        cpo_environment = get_cpo_environment(COUNTRY_CODE, use_case)
        PERIOD = config_variables["PERIOD"]
        directory = COUNTRY_CODE + '/' + PERIOD + '/'

        print(f"query_id {query_id}")

        url = f"{base_url}?block=undefined&queryId={query_id}&where[cpo_environment]={cpo_environment}&where[start_date]={start_date}&where[end_date]={end_date}"
        print(f"url {url}")
        print('!!!get_data_by_middleware started, cpo_environment')
        print(cpo_environment)
            
        headers = {
            'content-type': 'application/x-www-form-urlencoded'
        }
        try:
            response = requests.get(url, headers=headers).json()

            # Print total_chunk_count and total_row_count
            total_chunk_count = response.get("manifest", {}).get("total_chunk_count", 0)
            total_row_count = int(response.get("manifest", {}).get("total_row_count", 0))
            columns = [col["name"] for col in response.get("manifest", {}).get("schema", {}).get("columns", [])]

            print(f"total_chunk_count: {total_chunk_count}")
            print(f"total_row_count: {total_row_count}")

            if total_row_count== 0:  # Check if total_row_count is zero
                print(f"!!! outside total_row_count == 0")
                df = pd.DataFrame(columns=columns)  # Create an empty DataFrame with headers

                 # Push the DataFrame to S3 as a CSV
                csv_data = df.to_csv(sep=',', index=False, header=True)
                push_csv_to_s3(csv_data, directory + 'chargeguru_chargelog.csv')
            elif response["status"]["state"] == "SUCCEEDED":
                data_array = response["result"]["data_array"]
                    
                # Convert the response data to a DataFrame
                df = pd.DataFrame(data_array, columns=columns)

                # Ensure proper data types are applied
                df = df.apply(pd.to_numeric, errors='ignore')  # Converts columns with numeric values
                
                # Push the DataFrame to S3 as a CSV
                csv_data = df.to_csv(sep=',', index=False, header=True)
                push_csv_to_s3(csv_data, directory + 'chargeguru_chargelog.csv')
            else:
                return api_response(500, 'Error: Response state not succeeded')
            
        except Exception as e:
            return api_response(500, 'Error during download: ' + str(e))
        


# function that increments the given date by one day.
# Parameters: - date_str: A date in 'YYYY-MM-DD' format. Returns: - A new date string in 'YYYY-MM-DD' format, incremented by one day.
def get_next_day(date_str: str) -> str:
    date = datetime.strptime(date_str, '%Y-%m-%d')
    next_day = date + timedelta(days=1)
    return next_day.strftime('%Y-%m-%d')


     
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
    
    
    sessions_number = len(detail[column_dict.get(COUNTRY_CODE)["Name and surname of the user"]])
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
    
    employeeSummary.to_excel(writer, sheet_name='Summary',index = False, header=False)
    for column in employeeSummary:
        column_width = max(employeeSummary[column].astype(str).map(len).max(), len(column))
        col_idx = employeeSummary.columns.get_loc(column)
        writer.sheets['Summary'].set_column(col_idx, col_idx, column_width)

    detail.to_excel(writer, sheet_name='Detail',index = False)

    for column in detail:
        column_width = max(detail[column].astype(str).map(len).max(), len(column))
        col_idx = detail.columns.get_loc(column)
        writer.sheets['Detail'].set_column(col_idx, col_idx, column_width)

    writer.close()

    xlsx_data = output.getvalue()
    s3_path = directory+' FABS Client Expense Report - ' + PERIOD+' - '+cardNumber+' - '+employeeName+'.xlsx'
    push_file_to_s3(xlsx_data, s3_path)


# return api response for the lambda
def api_response(status_code, body_str): 
    return {
        "statusCode": status_code,
        'headers': {
            'Content-Type': 'application/json',
        },
        "body": body_str
    }


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



def process_cpo_data(config_variables):
    
    if config_variables:
        COUNTRY_CODE= config_variables["COUNTRY_CODE"]
       # EXPORT_START_DATE = config_variables["EXPORT_START_DATE"]
        #EXPORT_END_DATE = config_variables["EXPORT_END_DATE"]
        PERIOD= config_variables["PERIOD"]
       # MARGIN_RATE= int(config_variables["MARGIN_RATE"])
        directory = COUNTRY_CODE +'/'+ PERIOD+'/'
    

    # chargelog_csv = retrieve_csv_from_s3(directory+'chargeguru_chargelog_test.csv')    
    # main_instance_data=pd.read_csv(StringIO(chargelog_csv), sep=';')

    chargelog_csv = retrieve_csv_from_s3(directory+'chargeguru_chargelog.csv') 
    #chargelog_csv = retrieve_csv_from_s3('ES/Q3_2024_test/chargeguru_chargelog_martin_test.csv')    
    main_instance_data=pd.read_csv(StringIO(chargelog_csv), sep=',')   
   

    if len(main_instance_data.columns) > 0 and main_instance_data.shape[0] > 0:

        main_instance_data=main_instance_data.reset_index(drop=True)
        main_instance_data = main_instance_data.fillna('')
        
        employees =  main_instance_data[['SFAccountId', 'SepaMandateCardNumber','ClientName']].copy()
        employees = employees.drop_duplicates()
        employees['Total to bill (€)'] =  0
        employees['Total consumption (kWh)'] =  0
        employees['number of sessions'] =  0

        del main_instance_data['SessionId']

        
        ORDER_Employee_Sheet = [
            column_dict.get(COUNTRY_CODE)["Name and surname of the user"],
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
                #"StationOwnerName": column_dict.get(COUNTRY_CODE)["Station Owner"],
                #"StationOwnerName": column_dict.get(COUNTRY_CODE)["Name and surname of the user"],
                "ClientName": column_dict.get(COUNTRY_CODE)["Name and surname of the user"],
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


# def get_cpo_environment(country_code: str, use_case: str) -> Tuple[str, str]:
#     country_code = country_code.lower()
#     if use_case == 'FABS':
#         environments = (f'ampeco_{country_code}', '')
#     elif use_case == 'HOME':
#         environments = (f'be_energised_{country_code}_home', f'ampeco_{country_code}')
#     elif use_case in ['INTERNAL', 'EXTERNAL']:
#         environments = (f'be_energised_{country_code}_main', f'ampeco_{country_code}')
#     else:
#         raise ValueError("Invalid use case. Use 'HOME', 'INTERNAL', 'EXTERNAL', or 'FABS'.")

#     return environments

def get_cpo_environment(country_code: str, use_case: str) -> str:
    country_code = country_code.lower()
    
    if use_case == 'FABS':
        environments = f'ampeco_{country_code}'
    elif use_case == 'HOME':
        environments = f'be_energised_{country_code}_home,ampeco_{country_code}'
    elif use_case in ['INTERNAL', 'EXTERNAL']:
        environments = f'be_energised_{country_code}_main,ampeco_{country_code}'
    else:
        raise ValueError("Invalid use case. Use 'HOME', 'INTERNAL', 'EXTERNAL', or 'FABS'.")

    return environments

    

######## GLOBAL VARS #########

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
    },
     "DE": {
        "Station Owner": "Besitzer der Ladestation",
        "Session Amount (€)": "Ladevorgang Betrag (€)",
        "Company Name": "Firma",
        "Period": "Zeitraum",
        "Number of charging sessions": "Anzahl der Ladevorgänge",
        "Total to reimburse (€)": "Gesamtbetrag zur Rückerstattung an Mitarbeiter (€)",
        "Customer Number": "Kundennummer",
        "ChargeGuru Invoice": "ChargeGuru - Rechnung -",
        "ChargeGuru Credit Note": "ChargeGuru - Credit Note -",
        "Name and surname of the user": "Name und Nachname des Benutzers",
        "Total income of the charging (€)": "Gesamteinnahmen des Ladevorgangs (€)",
        "Number of employees": "Anzahl der Mitarbeiter",
        "Date and time of charging sessions": "Datum und Uhrzeit der Ladevorgänge",
        "Charging point ID": "Ladepunkt-ID",
        "Badge ID": "Ausweisnummer",
        "Charging Duration": "Ladedauer",
        "Energy consumption (kWh)": "Energieverbrauch (kWh)",
        "Total Consumption (kWh)": "Gesamtverbrauch (kWh)",
        "Total income of the sessions (€)": "Gesamteinnahmen der Ladevorgänge (€)",
        "Total to bill (€)": "Gesamtbetrag, der dem Benutzer in Rechnung gestellt wurde (€)"
    }
}