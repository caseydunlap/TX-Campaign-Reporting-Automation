import pandas as pd
import numpy as np
import pytz
import urllib
import math
import json
from io import BytesIO
import snowflake.connector
from sqlalchemy.sql import text
from sqlalchemy.types import VARCHAR
import os
import logging
from datetime import datetime,timedelta,timezone,time,date
import requests
from sqlalchemy import create_engine
from decimal import Decimal

#Config logging
script_dir = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(
    filename=os.path.join(script_dir,'logs.log'),
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

try:
    #Load Secrets
    evv_taxonomies_str = os.getenv('evv_taxonomies', '')
    taxonomy_evv_scope = [item.strip().strip("'\"") for item in evv_taxonomies_str.split(',') if item.strip()]
    mask_list_1_str = os.getenv('mask_list_1', '')
    mask_list_2_str = os.getenv('mask_list_1_2', '')
    mask_list_1 = [item.strip().strip("'\"") for item in mask_list_1_str.split(',') if item.strip()]
    mask_list_2 = [item.strip().strip("'\"") for item in mask_list_2_str.split(',') if item.strip()]
    manual_adjust_list = mask_list_1 + mask_list_2
    mask_provider_list_str = os.getenv('mask_list_2', '')
    masked_provider_list = [item.strip().strip("'\"") for item in mask_provider_list_str.split(',') if item.strip()]
    secret = os.getenv('graph_secret')
    client_id = os.getenv('graph_client')
    tenant_id = os.getenv('graph_tenant')
    sharepoint_url_base = os.getenv('sharepoint_url_base')
    sharepoint_url_end = os.getenv('sharepoint_url_end')
    snowflake_user = os.getenv('snowflake_user')
    snowflake_pass = os.getenv('snowflake_password')
    snowflake_wh = os.getenv('snowflake_fivetran_wh')
    snowflake_role = os.getenv('snowflake_role')
    snowflake_schema = os.getenv('snowflake_schema')
    snowflake_account = os.getenv('snowflake_account')
    snowflake_fivetran_db = os.getenv('snowflake_fivetran_db')

    #Init some date parameters for dyanmic Web Validation lookup
    now = datetime.now()
    current_day = f"{now.day:02d}"
    current_year = now.year
    current_month = f"{now.month:02d}"

    #Use the Microsfot Graph API to get the Mask NPI file contents

    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': secret,
        'scope':  'https://graph.microsoft.com/.default'}
    response = requests.post(url, data=data)
    response_json = response.json()
    access_token = response_json.get('access_token')

    url = f'https://graph.microsoft.com/v1.0/sites/{sharepoint_url_base}:/personal/{sharepoint_url_end}'
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    site_data = response.json()
    site_id = site_data.get("id")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    response = requests.get(f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives', headers=headers)

    drive_id = None
    drives = response.json().get('value', [])
    for drive in drives:
        if drive['name']== 'OneDrive':
            drive_id = drive['id']
            break

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    items = response.json()
    for item in items['value']:
        if item['name']== 'Cognito Forms':
            item_id = item['id']
            break

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    children = response.json().get('value', [])

    for child in children:
        if child['name'] == 'Mask_NPIs.xlsx':
            child_item_id = child['id']
            break

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{child_item_id}/content'

    headers = {'Authorization': f'Bearer {access_token}'}

    # Make the request to download the file
    response = requests.get(url, headers=headers)

    mask_npi = pd.read_excel(BytesIO(response.content))

    mask_npi['NPI'] = mask_npi['NPI'].astype(str)

    #Refresh Auth token
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': secret,
        'scope':  'https://graph.microsoft.com/.default'}
    response = requests.post(url, data=data)
    response_json = response.json()
    access_token = response_json.get('access_token')

    #Refresh Site ID
    url = f'https://graph.microsoft.com/v1.0/sites/{sharepoint_url_base}:/personal/{sharepoint_url_end}'
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    site_data = response.json()
    site_id = site_data.get("id")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    #Refresh OneDrive Drive ID
    response = requests.get(f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives', headers=headers)

    drive_id = None  # Initialize the variable to None
    drives = response.json().get('value', [])
    for drive in drives:
        if drive['name']== 'OneDrive':
            drive_id = drive['id']
            break  # Exit the loop as we found the drive ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    items = response.json()

    for item in items['value']:
        if item['name']== 'Cognito Forms':
            item_id = item['id']
            break  # Exit the loop as we found the drive ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    children = response.json().get('value', [])

    child_item_ids = {}  # Dictionary to store found child item IDs

    try:
        for child in children:
            for i in range(1, 4):
                expected_name = f"{current_year}-{current_month}-{current_day}-{i}.xlsx"
                if child['name'] == expected_name:
                    child_item_ids[f"child_item_id_{i}"] = child['id']
                    break

        if not child_item_ids:
            raise ValueError("No expected files found.")

    except Exception as e:
        print(f"An error occurred: {e}")

    webservices_dfs = {}  # Dictionary to store each downloaded file as a DataFrame

    for key, child_item_id in child_item_ids.items():
        try:
            # Construct the download URL for the current child item
            url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{child_item_id}/content'
            
            # Make the request to download the file
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Load the file content into a DataFrame
                df = pd.read_excel(BytesIO(response.content))
                
                # Store the DataFrame in the dictionary
                webservices_dfs[key] = df
            else:
                raise ValueError("Failed to download key.")
        except Exception as e:
            print(f"An error occurred: {e}")

    master_sheets = {}

    # Loop through a range that covers the expected number of child items
    for i in range(1, 4):  # Adjust the range based on the expected number of child items
        try:
            # Attempt to assign the DataFrame from 'webservices_dfs' to 'master_sheet_{i}'
            master_sheets[f'master_sheet_{i}'] = webservices_dfs[f'child_item_id_{i}']
        except Exception:
            pass

    #Function to clean up the web validation data
    def data_cleanup(df):
        df.columns = df.iloc[2]
        df = df.drop(df.index[0:3])
        df = df.loc[:,df.columns.notnull()]
        return df

    #Clean up Web Validation Data
    try:
        master_sheet_1 = data_cleanup(master_sheets['master_sheet_1'])
        master_sheet_2 = data_cleanup(master_sheets['master_sheet_2'])
        master_sheet_3 = data_cleanup(master_sheets['master_sheet_3'])
    except Exception:
        pass

    #Store n web validation reports in single dataframe
    dfs_to_merge = []

    if 'master_sheet_1' in locals() or 'master_sheet_1' in globals():
        dfs_to_merge.append(master_sheet_1)
    if 'master_sheet_2' in locals() or 'master_sheet_2' in globals():
        dfs_to_merge.append(master_sheet_2)
    if 'master_sheet_3' in locals() or 'master_sheet_3' in globals():
        dfs_to_merge.append(master_sheet_3)

    if dfs_to_merge:
        merged_success_df = pd.concat(dfs_to_merge, ignore_index=True)
    else:
        merged_success_df = pd.DataFrame()

    #Only include taxonomies that are considered "in scope"
    filtered_success = merged_success_df[~merged_success_df['Taxonomy'].isin(taxonomy_evv_scope)]

    #Init Snowflake session
    ctx = snowflake.connector.connect(
        user = snowflake_user,
        role = snowflake_role,
        warehouse = snowflake_wh,
        password = snowflake_pass,
        schema = snowflake_schema,
        account= snowflake_account)
    cs = ctx.cursor()
    script = """
    select * from "PC_FIVETRAN_DB"."CAMPAIGN_REPORTING"."TEXAS"
    """
    payload = cs.execute(script)
    provider_jumpoff = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': secret,
        'scope':  'https://graph.microsoft.com/.default'}
    response = requests.post(url, data=data)
    response_json = response.json()
    access_token = response_json.get('access_token')

    access_token = response_json.get('access_token')

    url = f"https://graph.microsoft.com/v1.0/sites/hhaexchange-my.sharepoint.com:/personal/mdunlap_hhaexchange_com"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    site_data = response.json()
    site_id = site_data.get("id")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    # Make a GET request to list all drives (document libraries)
    response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)

    drive_id = None  # Initialize the variable to None
    # Check if the request was successful
    if response.status_code == 200:
        drives = response.json().get('value', [])
        for drive in drives:
            if drive['name']== 'OneDrive':
                drive_id = drive['id']
                break  #Exit the loop as we found the ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    items = response.json()

    for item in items['value']:
        if item['name'] == 'Desktop':
            item_id = item['id']
            break  #Exit the loop as we found the ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    children = response.json().get('value', [])

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    children = response.json().get('value', [])

    for child in children:
        if child['name'] == 'HG Files':
            child_item_id = child['id']
            break #Exit the loop as we found the ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{child_item_id}/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    nested_children = response.json().get('value', [])

    for child in nested_children:
        if child['name'] == 'Snowflake Excel Files':
            nested_child_item_id = child['id']
            break #Exit the loop as we found the ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{nested_child_item_id}/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    nested_children_final = response.json().get('value', [])

    for child in nested_children_final:
        if child['name'] == 'Texas_PSO_Providers.csv':
            final_nested_child_item_id = child['id']
            break #Exit the loop as we found the ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{final_nested_child_item_id}/content'

    headers = {'Authorization': f'Bearer {access_token}'}

    # Make the request to download the file
    response = requests.get(url, headers=headers)

    pso_list = pd.read_csv(BytesIO(response.content))


    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{nested_child_item_id}/children'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    nested_children_final = response.json().get('value', [])

    for child in nested_children_final:
        if child['name'] == 'Texas Onboarding Form_Stream.xlsx':
            final_nested_child_item_id = child['id']
            break #Exit the loop as we found the ID

    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{final_nested_child_item_id}/content'

    headers = {'Authorization': f'Bearer {access_token}'}

    # Make the request to download the file
    response = requests.get(url, headers=headers)

    cognito_form = pd.read_excel(BytesIO(response.content))

    #Do some data cleanup for matching later
    cognito_form['TaxID'] = pd.to_numeric(cognito_form['TaxID'], errors='coerce').fillna(0).astype(int)
    cognito_form['NPI'] = pd.to_numeric(cognito_form['NPI'], errors='coerce').fillna(0).astype(int)

    cognito_form['TaxID'] = cognito_form['TaxID'].astype(str)
    cognito_form['NPI'] = cognito_form['NPI'].astype(str)

    #Create a dataframe from existing onboarded providers table to attempt to append missing TaxIDs
    provider_jumpoff_nulls_only = provider_jumpoff[provider_jumpoff['Tax ID TIN'].isna()]

    provider_jumpoff_nulls_only['Tax ID TIN'] = provider_jumpoff_nulls_only['Tax ID TIN'].astype(str)
    provider_jumpoff_nulls_only['NPIorAPI'] = provider_jumpoff_nulls_only['NPIorAPI'].astype(str)

    #In the event there is more than one row per NPI in the Cognito form, only keep the first instance for lookup purposes
    cognito_form_unique = cognito_form.drop_duplicates(subset='NPI', keep='first')

    merged_jumpoff_table = pd.merge(provider_jumpoff_nulls_only, cognito_form_unique, left_on='NPIorAPI',right_on='NPI',how='left')

    #Determine potential matches for missing TaxIDs, do some data cleanup
    merged_jumpoff_table['cognito_match'] = merged_jumpoff_table['NPIorAPI'].isin(cognito_form['NPI'])

    merged_jumpoff_table = merged_jumpoff_table[merged_jumpoff_table['cognito_match'] == True]

    merged_jumpoff_table['Tax ID TIN'] = merged_jumpoff_table['TaxID']

    merged_jumpoff_table['TAX_NPI'] = merged_jumpoff_table['Tax ID TIN'].astype(str) + merged_jumpoff_table['NPIorAPI'].astype(str)

    update_tax_ids = merged_jumpoff_table[['NPIorAPI','Tax ID TIN','TAX_NPI']]

    #If the update_tax_ids dataframe has at least one row, iterate through the dataframe and append new TaxID and TAX_NPI in Snowflake table
    if len(update_tax_ids) > 0:
        engine = create_engine(f'snowflake://{snowflake_user}:{snowflake_pass}@{snowflake_account}/{snowflake_fivetran_db}/CAMPAIGN_REPORTING?warehouse={snowflake_wh}&role={snowflake_role}')

        table_name = '"PC_FIVETRAN_DB"."CAMPAIGN_REPORTING"."TEXAS"'


        for _, single_row in update_tax_ids.iterrows():

            update_parts = [f'"{col}" = \'{single_row[col]}\' ' for col in single_row.index if col != 'NPIorAPI']
            
            set_clause = ", ".join(update_parts)

            update_sql = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE "NPIorAPI" = '{single_row["NPIorAPI"]}' and "Tax ID TIN" is null
            """
            engine.execute(update_sql)

    #Reload onboarded providers with updated TaxIDs, Tax_NPIs
    cs = ctx.cursor()
    script = """
    select * from "PC_FIVETRAN_DB"."CAMPAIGN_REPORTING"."TEXAS"
    """
    payload = cs.execute(script)
    provider_jumpoff = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

    #Save cognito form with transformations up to this point for LMS status update
    cognito_form_adjusted = cognito_form

    #Remove masked NPIs from cognito form, check PSO, Web Validation
    cognito_form = cognito_form[~cognito_form['NPI'].isin(manual_adjust_list)]
    cognito_form = cognito_form[~cognito_form['NPI'].isin(mask_npi['NPI'])]
    cognito_form = cognito_form[~cognito_form['NPI'].isin(masked_provider_list)]

    cognito_form['TaxNPI'] = cognito_form['TaxID'].astype(str) + cognito_form['NPI'].astype(str)

    cognito_filtered = cognito_form[~cognito_form['TaxNPI'].isin(provider_jumpoff['TAX_NPI'])]

    cognito_filtered['PSO'] = cognito_filtered['NPI'].isin(pso_list['NPI'])

    cognito_filtered_no_pso = cognito_filtered[cognito_filtered['PSO'] == False]

    cognito_filtered_no_pso['Success Checker'] = cognito_filtered_no_pso['NPI'].isin(merged_success_df['Provider NPI'])

    cognito_filtered_success_only = cognito_filtered_no_pso[cognito_filtered_no_pso['Success Checker'] == True]

    cognito_filtered_success_only['LMSKey'] = cognito_form['SubmitterSignatureAuthorityEmail'].astype(str) + cognito_form['TaxID'].astype(str)

    #Fetch latest LMS course data from Snowflake, store in dataframe
    cs = ctx.cursor()
    script = """
    select * from "PC_FIVETRAN_DB"."DOCEBO"."CUSTOM_COURSES"
    where course_name in ('Texas LMS Quiz','Texas FMSA LMS Quiz')
    """
    payload = cs.execute(script)
    docebo_df = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

    #Do some data cleanup, perform LMS credentialing test
    docebo_df['LMSKey'] = docebo_df['EMAIL'].astype(str) + docebo_df['AGENCY_TAX_ID'].astype(str)

    cognito_filtered_success_only['LMSCredsSent'] = cognito_filtered_success_only['LMSKey'].isin(docebo_df['LMSKey'])

    cognito_filtered_success_only = cognito_filtered_success_only.drop_duplicates(subset='TaxNPI', keep='last')

    final_list = cognito_filtered_success_only[cognito_filtered_success_only['LMSCredsSent'] == True]

    final_list_unique = final_list.drop_duplicates(subset='TaxNPI', keep='last')

    final_list_unique_cleaned = final_list_unique.dropna(subset=['NPI'])

    import_list = final_list_unique_cleaned[['LegalEntityName','TaxID','NPI','TaxNPI','PSO','LMSCredsSent']]

    import_list['PSO'] = 'NOT_A_PSO'

    #Create final list for import of newly added Providers into Snowflake
    import_list = import_list.rename(columns={'LegalEntityName' : 'LegalName','TaxID' : 'Tax ID TIN','NPI' : 'NPIorAPI','LMSCredsSent': 'LMS Status','TaxNPI': 'TAX_NPI'})

    engine = create_engine(f'snowflake://{snowflake_user}:{snowflake_pass}@{snowflake_account}/{snowflake_fivetran_db}/CAMPAIGN_REPORTING?warehouse={snowflake_wh}&role={snowflake_role}')

    if len(import_list) > 0:
        chunk_size = 10000
        chunks = [x for x in range(0, len(import_list), chunk_size)] + [len(import_list)]
        table_name = 'TEXAS' 

        numeric_column_names = ['NPIorAPI']
        import_list[numeric_column_names] = import_list[numeric_column_names].astype(str)

        for i in range(len(chunks) - 1):
            import_list[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False,dtype={'NPIorAPI': VARCHAR})

    #Reload providers with latest updates
    cs = ctx.cursor()
    script = """
    select * from "PC_FIVETRAN_DB"."CAMPAIGN_REPORTING"."TEXAS"
    """
    payload = cs.execute(script)
    provider_jumpoff = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

    #Split the docebo dataframe for status updates, conduct necessary tests and transformations
    update_df = docebo_df[['AGENCY_TAX_ID','COURSE_ENROLLMENT_STATUS']]

    filtered_jump_off = provider_jumpoff[provider_jumpoff['LMS Status'].str.strip() != 'Completed']
    filtered_jump_off = filtered_jump_off.dropna(subset=['Tax ID TIN'])

    filtered_jump_off['Tax ID TIN'] = filtered_jump_off['Tax ID TIN'].apply(lambda x: str(int(pd.to_numeric(x, errors='coerce'))) if not pd.isnull(pd.to_numeric(x, errors='coerce')) else x)
    update_df['AGENCY_TAX_ID'] = update_df['AGENCY_TAX_ID'].apply(lambda x: str(int(float(x))) if pd.notna(x) and isinstance(x, str) and x.replace('.', '', 1).replace('-', '', 1).isdigit() else x)
    merged_df = pd.merge(filtered_jump_off, update_df, left_on='Tax ID TIN',right_on='AGENCY_TAX_ID',how='left')

    cognito_form_adjusted['TaxID'] = pd.to_numeric(cognito_form_adjusted['TaxID'], errors='coerce').fillna(0).astype(int)
    cognito_form_adjusted['NPI'] = pd.to_numeric(cognito_form_adjusted['NPI'], errors='coerce').fillna(0).astype(int)

    cognito_form_adjusted['TaxID'] = cognito_form_adjusted['TaxID'].astype(str)
    cognito_form_adjusted['NPI'] = cognito_form_adjusted['NPI'].astype(str)

    cognito_form_adjusted['TaxNPI'] = cognito_form['TaxID'].astype(str) + cognito_form['NPI'].astype(str)

    merged_df['cognito_match'] = merged_df['TAX_NPI'].isin(cognito_form_adjusted['TaxNPI'])

    merged_df['updated_lms_status'] = None
    merged_df['updated_lms_status'] = merged_df['updated_lms_status'].replace('', np.nan)
    merged_df['updated_lms_status'] = merged_df['updated_lms_status'].fillna(merged_df['COURSE_ENROLLMENT_STATUS'])

    merged_df['updated_lms_status'] = np.where((merged_df['cognito_match'] == False) & (merged_df['COURSE_ENROLLMENT_STATUS'].isna()) | merged_df['Tax ID TIN'].isna(),'Onboarding Form Needed',merged_df['updated_lms_status'])  # Keep the existing value if conditions are not met

    merged_df['updated_lms_status'] = np.where((merged_df['cognito_match'] == True) & (merged_df['updated_lms_status'].isna()) ,'LMS Credentials Needed',merged_df['updated_lms_status'])

    merged_df['updated_lms_status'] = merged_df['updated_lms_status'].replace('Enrolled', 'Not Started')

    merged_df_final = merged_df[['TAX_NPI','updated_lms_status']]
    merged_df_final = merged_df_final.rename(columns={'updated_lms_status': 'LMS Status'})

    table_name = '"PC_FIVETRAN_DB"."CAMPAIGN_REPORTING"."TEXAS"'

    #Load latest LMS statuses for each provider
    for _, single_row in merged_df_final.iterrows():

        update_parts = [f'"{col}" = \'{single_row[col]}\' ' for col in single_row.index if col != 'TAX_NPI']
        
        set_clause = ", ".join(update_parts)

        update_sql = f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE "TAX_NPI" = '{single_row["TAX_NPI"]}' and "LMS Status" != 'Completed'
        """
        engine.execute(update_sql)
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Success')
except Exception as e:
    logging.exception('Operation failed due to an error')
logging.getLogger().setLevel(logging.ERROR)
