from pprint import pprint
import pandas as pd
import os
import numpy as np
import math
import shelve
from datetime import datetime
import warnings
import sys
import requests
import json
import time
from datetime import datetime
import base64

warnings.filterwarnings("ignore")
os.system('cls')

def Data_Storage(f, key, datain):
    try: 
        shfile = shelve.open("data_selve")  
        dataout = 0
        if int(f) == 1:  # f = 1: Date Store, 0:Data Fetch
            shfile[key] = datain
        else:
            dataout = shfile[key]
        shfile.close()
    except Exception as e:
        print(f"Error in Data_Storage: {e}")
    return dataout

def getReset(df):
    df.reset_index(inplace=True)
    try:
        df = df.drop(columns=['index'])
    except:
        pass
    try:
        df = df.drop(columns=['level_0'])
    except:
        pass
    try:
        df = df.drop(columns=['Unnamed: 0'])
    except:
        pass       
    return df

def convert_date_format(date_str):
    """Force EVERY input into 'DD-MMM-YYYY' format (even invalid ones become today's date)"""
    date_obj = pd.to_datetime(date_str, errors='coerce') or pd.Timestamp.now()
    return date_obj.strftime('%d-%b-%Y').upper()

def pushDataToL2API(payload, request_id):
    # Basic Authentication credentials
    username = "*****"
    password = "****"
    
    # API URL
    url = "API_URL"
    
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=120)
        
        if response.status_code == 200:
            try:
                response_data = response.json()
                status = "Success" if response_data.get("response_message_text", "").lower() == "success" else response_data.get("response_message_text", "Unknown Error")
                return True, status, response_data
            except ValueError:
                return False, "Invalid JSON response", None
        else:
            return False, f"HTTP Error {response.status_code}: {response.text}", None
    except requests.exceptions.RequestException as e:
        return False, f"Request failed: {e}", None

# Main processing
def main():
    excel_files = [f for f in os.listdir() if f.endswith('.xlsx') or f.endswith('.xls')]
    
    if not excel_files:
        print("No Excel files found in the directory.")
        return
    
    excel_file = excel_files[0]
    print(f"Loading data from: {excel_file}")
    
    try:
        df = pd.read_excel(excel_file)
        print("Excel data loaded successfully.")
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return
    
    # Define required columns and mapping
    required_columns = {
        'survey_timings': 'Replacement_Date',
        'Application_ID': 'Application_ID',
        'sdocode': 'Billing_Unit',
        'NC STATUS AS PER UID': 'current_Workflow_Status',
        'consumername': 'Consumer_Name',
        'kno': 'Consumer_Number'
    }
    
    missing_columns = [col for col in required_columns.keys() if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing required columns in Excel: {', '.join(missing_columns)}")
        return
    
    print("Processing the data for creating JSON payload...")
    
    df1 = pd.DataFrame()
    total_records = len(df)
    
    for ii in range(total_records):

        os.system('cls')
        print(f"Processing Data: {ii+1} of {total_records} || {round(100 * (ii+1) / total_records, 1)}% complete")

        for excel_col, api_col in required_columns.items():
            if excel_col == 'sdocode':
                sdocode = str(df.loc[ii, 'sdocode'])
                if len(sdocode) == 4:
                    df1.loc[ii, 'Billing_Unit'] = sdocode
                elif len(sdocode) == 3:
                    df1.loc[ii, 'Billing_Unit'] = '0' + sdocode
                elif len(sdocode) == 2:
                    df1.loc[ii, 'Billing_Unit'] = '00' + sdocode
                else:
                    df1.loc[ii, 'Billing_Unit'] = sdocode.zfill(4)
            elif excel_col == 'survey_timings':
                df1.loc[ii, 'Replacement_Date'] = convert_date_format(df.loc[ii, 'survey_timings'])
            else:
                df1.loc[ii, api_col] = str(df.loc[ii, excel_col])
        
        df1.loc[ii, 'AMISP_SMART_METER_APPLICATION_FEED_BY_MSEDCL_USER_FLAG_YN'] = 'N'
        df1.loc[ii, 'AMISP_SMART_METER_FLAG_YN'] = 'Y'
        df1.loc[ii, 'service_Type_ID'] = np.int64(9)
        df1.loc[ii, 'Current_Workflow_Status_ID'] = np.int64(33)
        df1.loc[ii, 'Remark'] = 'Manual L2, Excel Based'

    Data_Storage(1, 'L2_API_Data', df1)
    
    print("\nProcessing successfully completed. Press Enter to push data to API...")
    input()
    
    df1 = Data_Storage(0, 'L2_API_Data', "")
    total_records = len(df1)
    lot_size = 100
    num_lots = math.ceil(total_records / lot_size)
    
    df['API_Status'] = ""
    
    overall_status = pd.DataFrame(columns=['Total', 'Success', 'Fail'])
    
    for lot in range(num_lots):
        start_idx = lot * lot_size
        end_idx = min((lot + 1) * lot_size, total_records)
        current_lot_size = end_idx - start_idx
        
        print(f"\nProcessing lot {lot+1} of {num_lots} (records {start_idx+1} to {end_idx})")
        
        # Prepare payload
        request_id = int(time.time())  # Unique request ID using timestamp
        payload = {
            "RequestId": request_id,
            "ReplacementRecords": current_lot_size,
            "ApplicationList": df1.iloc[start_idx:end_idx].to_dict('records')
        }
        
        payload_filename = f"Output_{request_id}.json"
        with open(payload_filename, 'w') as f:
            json.dump(payload, f, indent=2)
        
        success, status, response_data = pushDataToL2API(payload, request_id)
        
        # # Save response JSON
        # if response_data:
        #     response_filename = f"L2_Response_{request_id}.json"
        #     with open(response_filename, 'w') as f:
        #         json.dump(response_data, f, indent=2)
        
        if success:
            success_count = current_lot_size
            fail_count = 0
        else:
            success_count = 0
            fail_count = current_lot_size
        
        overall_status.loc[lot] = [current_lot_size, success_count, fail_count]
        
        for idx in range(start_idx, end_idx):
            original_idx = df.index[idx]
            df.loc[original_idx, 'API_Status'] = "Success" if success else "Failed"
            # df.loc[original_idx, 'API_Response'] = status
        
        print("\nCurrent Status:")
        print(overall_status)
        
        if lot < num_lots - 1:
            time.sleep(20)
    
    timestamp = datetime.now().strftime("%d%m%Y")
    output_excel = f"L2_API_Results_{timestamp}.xlsx"
    df.to_excel(output_excel, index=False)
    
    print("\nProcess completed.")
    print(f"\nFinal results saved to: {output_excel}")
    print("\nSummary:")
    print(f"Total Records Processed: {total_records}")
    print(f"Success: {overall_status['Success'].sum()}")
    print(f"Failed: {overall_status['Fail'].sum()}")

if __name__ == "__main__":
    main()
    input("\nPress Enter to exit...")