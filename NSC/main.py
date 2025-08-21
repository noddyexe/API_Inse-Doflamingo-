import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import json
import requests
from datetime import datetime
import shelve
import os
import math
import time
from pprint import pprint

class NSCDataPushTool:
    def __init__(self, root):
        self.root = root
        self.root.title("Doflamingo")
        self.root.geometry("900x700")

        self.logged_in = False
        self.token = ""
        

        self.df = None
        self.process_status = None
        self.failed_records = None
        

        self.create_login_frame()
        
    def create_login_frame(self):
        """Create the login interface matching the provided design"""
        self.clear_frame()
        

        self.login_frame = tk.Frame(self.root, bg="#0078D7")
        self.login_frame.pack(fill=tk.BOTH, expand=True)
        

        header_frame = tk.Frame(self.login_frame, bg="#0078D7")
        header_frame.pack(pady=(50, 20))
        
        tk.Label(header_frame, 
                text="NSC Data Upload", 
                font=('Arial', 16, 'bold'), 
                fg="white", 
                bg="#0078D7").pack()
        

        login_box = tk.Frame(self.login_frame, bg="white", padx=20, pady=20)
        login_box.pack(pady=20, ipadx=20, ipady=20)
        

        tk.Label(login_box, 
                text="LOGIN", 
                font=('Arial', 14, 'bold'), 
                bg="white").pack(pady=(0, 20))
        

        maintained_frame = tk.Frame(login_box, bg="white")
        maintained_frame.pack(fill=tk.X, pady=5)
        
        

        entry_frame = tk.Frame(login_box, bg="white")
        entry_frame.pack(fill=tk.X, pady=10)
        

        tk.Label(entry_frame, 
                text="Username:", 
                font=('Arial', 10), 
                bg="white").pack(anchor=tk.W)
        
        self.username_entry = tk.Entry(entry_frame, 
                                     font=('Arial', 12), 
                                     relief=tk.GROOVE, 
                                     bd=2)
        self.username_entry.pack(fill=tk.X, pady=5)
        
        tk.Label(entry_frame, 
                text="Password:", 
                font=('Arial', 10), 
                bg="white").pack(anchor=tk.W)
        
        self.password_entry = tk.Entry(entry_frame, 
                                     show="*", 
                                     font=('Arial', 12), 
                                     relief=tk.GROOVE, 
                                     bd=2)
        self.password_entry.pack(fill=tk.X, pady=5)

        login_btn = tk.Button(login_box, 
                             text="LOGIN", 
                             command=self.authenticate, 
                             font=('Arial', 12, 'bold'), 
                             bg="#0078D7", 
                             fg="white",
                             relief=tk.GROOVE,
                             bd=0,
                             padx=20,
                             pady=5)
        login_btn.pack(pady=(20, 10))
        

        footer_frame = tk.Frame(login_box, bg="white")
        footer_frame.pack()
        

        version_frame = tk.Frame(self.login_frame, bg="#0078D7")
        version_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=10)
        
        tk.Label(version_frame, 
                text="2025 Doflamingo @V-1.0.0.0", 
                font=('Arial', 8), 
                fg="white", 
                bg="#0078D7").pack()
        
    def create_main_frame(self):
        """Create the main application interface"""
        self.clear_frame()
        

        self.main_frame = tk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        

        token_frame = tk.LabelFrame(self.main_frame, text="Token Management", padx=10, pady=10)
        token_frame.pack(fill=tk.X, pady=10)
        
        self.token_status = tk.Label(token_frame, text="Token: Not Generated", fg="red", font=('Arial', 10))
        self.token_status.pack(side=tk.LEFT, padx=10)
        
        token_btn = tk.Button(token_frame, text="Generate Token", command=self.generate_token,
                             font=('Arial', 10), bg='#2196F3', fg='white')
        token_btn.pack(side=tk.RIGHT, padx=10)

        file_frame = tk.LabelFrame(self.main_frame, text="Excel File Selection", padx=10, pady=10)
        file_frame.pack(fill=tk.X, pady=10)
        
        self.file_path = tk.StringVar()
        file_entry = tk.Entry(file_frame, textvariable=self.file_path, width=50, font=('Arial', 10))
        file_entry.pack(side=tk.LEFT, padx=10, fill=tk.X, expand=True)
        
        browse_btn = tk.Button(file_frame, text="Browse", command=self.browse_file,
                              font=('Arial', 10), bg='#607D8B', fg='white')
        browse_btn.pack(side=tk.RIGHT, padx=10)
        

        control_frame = tk.Frame(self.main_frame)
        control_frame.pack(fill=tk.X, pady=10)
        
        process_btn = tk.Button(control_frame, text="Process Data", command=self.process_data,
                               font=('Arial', 12), bg='#FF9800', fg='white')
        process_btn.pack(side=tk.LEFT, padx=10)
        
        push_btn = tk.Button(control_frame, text="Push to API", command=self.push_to_api,
                            font=('Arial', 12), bg='#009688', fg='white')
        push_btn.pack(side=tk.LEFT, padx=10)
        
        export_btn = tk.Button(control_frame, text="Export Results", command=self.export_results,
                              font=('Arial', 12), bg='#673AB7', fg='white')
        export_btn.pack(side=tk.RIGHT, padx=10)
        

        log_frame = tk.LabelFrame(self.main_frame, text="Process Logs", padx=10, pady=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = tk.Text(log_frame, height=15, wrap=tk.WORD, font=('Consolas', 10))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        scrollbar = tk.Scrollbar(self.log_text)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.log_text.yview)
        

        self.status_var = tk.StringVar()
        self.status_var.set("2025 Doflamingo @V-1.0.0.0")
        status_bar = tk.Label(self.main_frame, textvariable=self.status_var, bd=1, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(fill=tk.X, pady=5)
        
    def clear_frame(self):
        """Clear all widgets from root"""
        for widget in self.root.winfo_children():
            widget.destroy()
    
    def authenticate(self):
        """Authenticate user"""
        username = self.username_entry.get()
        password = self.password_entry.get()
        
        if username == "mcl" and password == "mcl@123":
            self.logged_in = True
            self.create_main_frame()
        else:
            messagebox.showerror("Login Failed", "Invalid username or password")
            
            
    
    def generate_token(self):
        """Generate API token"""
        url = "API_URL"
        payload = {
            "username": "****",
            "password": "****"
        }
        
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                response_data = response.json()
                

                print("API Response:", response_data)
                

                if isinstance(response_data, list):
                    if response_data and isinstance(response_data[0], dict):
                        self.token = response_data[0].get("token", "")
                    else:
                        self.token = response_data[0] if response_data else ""
                elif isinstance(response_data, dict):
                    self.token = response_data.get("token", "")
                else:
                    self.token = str(response_data)
                
                if self.token:
                    self.token_status.config(text=f"Token: Generated (Expires in 1 hour)", fg="green")
                    self.log("Token generated successfully")
                else:
                    self.token_status.config(text="Token: Invalid Format", fg="red")
                    self.log("Token generation failed: Unexpected response format")
            else:
                self.token_status.config(text="Token: Generation Failed", fg="red")
                self.log(f"Token generation failed: {response.text}")
        except Exception as e:
            self.token_status.config(text="Token: Connection Error", fg="red")
            self.log(f"Token generation error: {str(e)}")
    
    
    
    def browse_file(self):
        """Browse for Excel file"""
        filepath = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx *.xls")])
        if filepath:
            self.file_path.set(filepath)
            self.log(f"Selected file: {filepath}")
    
    def process_data(self):
        """Process the Excel file"""
        if not self.file_path.get():
            messagebox.showerror("Error", "Please select an Excel file first")
            return
            
        try:
            self.log("Starting data processing...")
            self.df = pd.read_excel(self.file_path.get())
            
            # Create DataFrame with required columns
            clmn = ['applicationId','applicationDate','serviceTypeId','assignedConsumerNumber','billingUnit',
                   'sectionId','consumerCategoryId','supplyTypeId','subCategoryId','requestedLoad',
                   'requestedLoadUnit','contractDemand','contractDemandUnit','firstName','middleName',
                   'lastName','flatSurveyHouseBldg','landmark','addressLine1','villageCity','pincode',
                   'mobile','email','latitude','longitude','currentWorkflowStatus','currentWorkflowStatusId',
                   'billingDay','pc','substationCode','feederCode','dtcCode','tariffCode','meterMakeCode',
                   'meterSerialNumber','meterTypeId','meterSubType','sfKwh','meterDigit','ctRationMainMeter',
                   'ptRationMainMeter','amperage','installationDate','supplyDate','meterReadingDate',
                   'headerReadingKwh','amispAgencyId','amispAgencyName','meterPhaseId']
            
            df1 = pd.DataFrame(columns=clmn)
            
            for ii in range(len(self.df)):
                if ii % 10 == 0:  # Update progress every 10 records
                    self.status_var.set(f"Processing record {ii+1} of {len(self.df)}")
                    self.root.update()
                
                if len(str(self.df.loc[ii,'BU'])) == 4:
                    df1.loc[ii,'billingUnit'] = str(self.df.loc[ii,'BU'])
                elif len(str(self.df.loc[ii,'BU'])) == 3:
                    df1.loc[ii,'billingUnit'] = '0' + str(self.df.loc[ii,'BU'])
                elif len(str(self.df.loc[ii,'BU'])) == 2:
                    df1.loc[ii,'billingUnit'] = '00' + str(self.df.loc[ii,'BU'])
                
                df1.loc[ii,'sectionId'] = str(self.df.loc[ii,'SECTION_CODE'])
                df1.loc[ii,'dtcCode'] = str(self.df.loc[ii,'DTC_CODE'])
                df1.loc[ii,'pc'] = str(self.df.loc[ii,'PC'])
                df1.loc[ii,'supplyTypeId'] = str(self.df.loc[ii,'CONS_TYPE'])
                df1.loc[ii,'assignedConsumerNumber'] = str(self.df.loc[ii,'CONSUMER_NUMBER'])
                df1.loc[ii,'applicationId'] = str(self.df.loc[ii,'APPLICATION_ID'])
                df1.loc[ii,'firstName'] = str(self.df.loc[ii,'CONSUMER_NAME'])
                df1.loc[ii,'addressLine1'] = str(self.df.loc[ii,'ADDRESS'])
                df1.loc[ii,'requestedLoad'] = str(self.df.loc[ii,'SANCIONED_LD_KW'])
                df1.loc[ii,'contractDemand'] = str(self.df.loc[ii,'CONTRACT_DEMAND_IN_KVA'])
                df1.loc[ii, 'applicationDate'] = self.convert_date_format(self.df.loc[ii, 'APPLICATION_DT'])
                df1.loc[ii, 'installationDate'] = self.convert_date_format(self.df.loc[ii, 'INSTALLATION_DT'])
                df1.loc[ii, 'supplyDate'] = self.convert_date_format(self.df.loc[ii, 'SUPPLY_DT'])

                if len(str(self.df.loc[ii,'METER_MAKE'])) == 3:
                    df1.loc[ii,'meterMakeCode'] = str(self.df.loc[ii,'METER_MAKE'])
                elif len(str(self.df.loc[ii,'METER_MAKE'])) == 2:
                    df1.loc[ii,'meterMakeCode'] = '0' + str(self.df.loc[ii,'METER_MAKE'])
                elif len(str(self.df.loc[ii,'METER_MAKE'])) == 1:
                    df1.loc[ii,'meterMakeCode'] = '00' + str(self.df.loc[ii,'METER_MAKE'])

                df1.loc[ii,'meterSerialNumber'] = str(self.df.loc[ii,'METER_NUMBER'])
                df1.loc[ii,'mobile'] = str(self.df.loc[ii,'MOBILE'])
                df1.loc[ii,'email'] = str(self.df.loc[ii,'EMAIL'])
                df1.loc[ii,'amispAgencyId'] = str(self.df.loc[ii,'AMISP_AGENCY_ID'])
                df1.loc[ii,'amispAgencyName'] = str(self.df.loc[ii,'AMISP_AGENCY_NAME'])
                df1.loc[ii,'currentWorkflowStatus'] = str(self.df.loc[ii,'CURRENT_WORKFLOW_STATUS'])
                df1.loc[ii,'currentWorkflowStatusId'] = str(self.df.loc[ii,'CURRENT_WF_STATUS_ID'])
                df1.loc[ii,'meterPhaseId'] = str(self.df.loc[ii,'NEW_METER_TYPE'])
                df1.loc[ii,'tariffCode'] = str(self.df.loc[ii,'TARIFF_CODE'])
                df1.loc[ii,'billingDay'] = str(self.df.loc[ii,'BILLING_DAY'])
                df1.loc[ii,'meterReadingDate'] = str(self.df.loc[ii,'METER_READING_DT'])
                df1.loc[ii,'headerReadingKwh'] = str(self.df.loc[ii,'HEADER_KWH_N'])
                df1.loc[ii,'latitude'] = str(0)
                df1.loc[ii,'longitude'] = str(0)
                df1.loc[ii,'requestedLoadUnit'] = str("KW")
                df1.loc[ii,'contractDemandUnit'] = str("KVA")
                df1.loc[ii,'serviceTypeId'] = ""
                df1.loc[ii,'consumerCategoryId'] = ""
                df1.loc[ii,'subCategoryId'] = ""
                df1.loc[ii,'middleName'] = ""
                df1.loc[ii,'lastName'] = ""
                df1.loc[ii,'flatSurveyHouseBldg'] = ""
                df1.loc[ii,'landmark'] = ""
                df1.loc[ii,'villageCity'] = ""
                df1.loc[ii,'pincode'] = ""
                df1.loc[ii,'substationCode'] = ""
                df1.loc[ii,'feederCode'] = ""
                df1.loc[ii,'meterTypeId'] = ""
                df1.loc[ii,'meterSubType'] = ""
                df1.loc[ii,'sfKwh'] = ""
                df1.loc[ii,'meterDigit'] = ""
                df1.loc[ii,'ctRationMainMeter'] = ""
                df1.loc[ii,'ptRationMainMeter'] = ""
                df1.loc[ii,'amperage'] = ""


            with shelve.open("Data_Selve") as shfile:
                shfile['NSC_API_Data'] = df1
            
            self.log(f"Data processing completed. Total records processed: {len(df1)}")
            self.status_var.set("Data processing completed")
            
        except Exception as e:
            self.log(f"Error processing data: {str(e)}")
            messagebox.showerror("Processing Error", f"An error occurred: {str(e)}")
    
    def push_to_api(self):
        """Push data to NSC API"""
        if not self.token:
            messagebox.showerror("Error", "Please generate a token first")
            return
            
        try:

            with shelve.open("Data_Selve") as shfile:
                df1 = shfile['NSC_API_Data']
            
            if df1 is None or len(df1) == 0:
                messagebox.showerror("Error", "No data to push. Please process data first")
                return
                
            lot_size = 10
            currDate = datetime.today().strftime("%d%m%y")
            total_records = len(df1)
            num_lots = math.ceil(total_records / lot_size)
            
            self.process_status = pd.DataFrame(columns=['Lot','Tot','Pass','Fail'])
            self.failed_records = pd.DataFrame(columns=['applicationId','assignedConsumerNumber','status','description'])
            
            self.log("\nStarting API push process...")
            self.log(f"Total records: {total_records}, Lots: {num_lots}, Lot size: {lot_size}")
            
            for lot in range(num_lots):
                start_idx = lot * lot_size
                end_idx = min((lot + 1) * lot_size, total_records)
                
                self.log(f"\nProcessing lot {lot+1} of {num_lots} (records {start_idx+1} to {end_idx})")
                self.status_var.set(f"Processing lot {lot+1} of {num_lots}")
                self.root.update()
                
                df2 = df1.iloc[start_idx:end_idx].copy()
                df2.reset_index(drop=True, inplace=True)
                FName = f'output_{currDate}_{lot+1}.json'
                

                records = df2.to_dict(orient='records')
                json_data = json.dumps(records)
                

                with open(FName, 'w') as f:
                    f.write(json_data)
                

                self.push_lot_to_api(json_data, lot)
                
                time.sleep(5)  # Add delay between lots
            
            # Display final results
            total = len(self.df)  # Total records attempted
            success = self.process_status['Pass'].sum()
            failed = self.process_status['Fail'].sum()
            
            self.log("\nAPI Push Summary:")
            self.log(f"Total records attempted: {total}")
            self.log(f"Successfully pushed: {success}")
            self.log(f"Failed: {failed}")
            
            if not self.failed_records.empty:
                self.log("\nFailed records details:")
                for _, row in self.failed_records.iterrows():
                    self.log(f"App ID: {row['applicationId']}, Status: {row['status']}, Reason: {row['description']}")
            
            self.status_var.set(f"API push completed. Success: {success}, Failed: {failed}")
            
        except Exception as e:
            self.log(f"Error in API push: {str(e)}")
            messagebox.showerror("API Push Error", f"An error occurred: {str(e)}")
    
    
    
    def push_lot_to_api(self, json_data, lot_number):
        """Push a single lot to API"""
        url = "API_URL"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, data=json_data, headers=headers, timeout=120)
            
            if response.status_code == 200:
                data = response.json()
                

                print("API Response:", data)
                

                total = 0
                success = 0
                failure = 0
                
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('Status', '').lower() == 'success':
                                success += 1
                            else:
                                failure += 1
                            total += 1
                            
                            # Record failed data if any
                            if item.get('Status', '').lower() != 'success':
                                failed_record = {
                                    'applicationId': item.get('ApplicationId', ''),
                                    'assignedConsumerNumber': '',  # You may need to map this
                                    'status': item.get('Status', ''),
                                    'description': item.get('DESCRIPTION', '')
                                }
                                self.failed_records = pd.concat([
                                    self.failed_records, 
                                    pd.DataFrame([failed_record])
                                ], ignore_index=True)
                elif isinstance(data, dict):
                    if data.get('Status', '').lower() == 'success':
                        success += 1
                    else:
                        failure += 1
                    total += 1
                    
                    if data.get('Status', '').lower() != 'success':
                        failed_record = {
                            'applicationId': data.get('ApplicationId', ''),
                            'assignedConsumerNumber': '',  
                            'status': data.get('Status', ''),
                            'description': data.get('DESCRIPTION', '')
                        }
                        self.failed_records = pd.concat([
                            self.failed_records, 
                            pd.DataFrame([failed_record])
                        ], ignore_index=True)
                
                self.process_status.loc[lot_number] = [
                    lot_number+1,
                    total,
                    success,
                    failure
                ]
                
                self.log(f"Lot {lot_number+1} processed - Total: {total}, Success: {success}, Failed: {failure}")
                
            else:
                self.log(f"Error in lot {lot_number+1}: {response.status_code} - {response.text}")
                self.process_status.loc[lot_number] = [lot_number+1, 0, 0, 0]
                
        except requests.exceptions.RequestException as e:
            self.log(f"Request failed for lot {lot_number+1}: {str(e)}")
            self.process_status.loc[lot_number] = [lot_number+1, 0, 0, 0]
        except Exception as e:
            self.log(f"Unexpected error for lot {lot_number+1}: {str(e)}")
            self.process_status.loc[lot_number] = [lot_number+1, 0, 0, 0]
    
    
    def export_results(self):
        """Export process results to Excel"""   
        if self.process_status is None:
            messagebox.showerror("Error", "No results to export. Please process and push data first")
            return
            
        try:
            with shelve.open("Data_Selve") as shfile:
                processed_data = shfile['NSC_API_Data']
            
            result_df = pd.DataFrame({
                'CONSUMER_NUMBER': processed_data['assignedConsumerNumber'],
                'METER_NUMBER': processed_data['meterSerialNumber'],
                'APPLICATION_ID': processed_data['applicationId'],
                'status': 'Success' 
            })
            
            if not self.failed_records.empty:
                for _, row in self.failed_records.iterrows():
                    mask = result_df['APPLICATION_ID'] == row['applicationId']
                    result_df.loc[mask, 'status'] = row['status']
            
            filename = f"NSC_Push_Results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            result_df.to_excel(filename, index=False)
            
            self.log(f"\nResults exported to {filename}")
            messagebox.showinfo("Export Successful", f"Results exported to {filename}")
            
        except Exception as e:
            self.log(f"Error exporting results: {str(e)}")
            messagebox.showerror("Export Error", f"An error occurred: {str(e)}")
    
    def convert_date_format(self, date_str):
        """Convert date to DD-MMM-YY format"""
        try:
            date_obj = pd.to_datetime(date_str, errors='coerce') or pd.Timestamp.now()
            return date_obj.strftime('%d-%b-%y').upper()
        except:
            return pd.Timestamp.now().strftime('%d-%b-%y').upper()
    
    def log(self, message):
        """Add message to log"""
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.root.update()


if __name__ == "__main__":
    root = tk.Tk()
    app = NSCDataPushTool(root)
    root.mainloop()