import logging
import pandas as pd
import re
import pyodbc
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of prefixes to look for in "Plan Système"
# Customize the list below to include the prefixes you need
prefixes = [
    "CAP", "CCO", "CEC" # Add more prefixes as needed
]

# Regex pattern to extract codes from the "Plan Système" column
# Customize the pattern if your data requires a different format
pattern = re.compile(r'\bCP[A-Z0-9]+\b')

# Function to extract codes from the "Plan Système" column
def extract_codes(plan_systeme):
    if pd.isna(plan_systeme):
        return []
    return [code.strip() for code in pattern.findall(plan_systeme)]

# Channel to database mapping
# Modify the mapping below to reflect your available channels and databases
channel_mapping = {
    "TEST": "ESBO_01",  # Example, modify as per actual channels
     # Add more mappings as necessary
}

# Function to get the corresponding database name based on the channel
def get_database_name(channel):
    return channel_mapping.get(channel.upper(), None)

# Function to fetch existing codes from the database in batches
def fetch_existing_codes(database_name, codes, server):
    batch_size = 500  # Adjust batch size as needed
    existing_codes = set()

    # Process the codes in batches
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        codes_str = "','".join(batch)
        check_query = f"SELECT code FROM articles WHERE code IN ('{codes_str}') AND active = 1"
        
        try:
            # Adjust the connection string to match your database configuration
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database_name};Trusted_Connection=yes;'
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(check_query)
                existing_codes.update(row[0] for row in cursor.fetchall())
        except pyodbc.Error as e:
            logging.error(f"Database query error for {database_name}: {e}")
    
    return existing_codes

# Load the Excel file
# Adjust the file path below to point to the correct location of your Excel file
file_path = Path("# Adjust this path to your Excel file location")
if not file_path.exists():
    logging.error(f"Excel file not found at {file_path}")
    raise FileNotFoundError(f"File not found: {file_path}")

logging.info(f"Loading Excel file from {file_path}")
df = pd.read_excel(file_path)

# List of banners to check
# Modify this list to include the banners relevant to your task
required_banners = ["TEST"]  # Add more banners as needed

# Filter data to include only the specified banners
filtered_df = df[df['Banner'].isin(required_banners)]
logging.info(f"Filtered data to include only the specified banners: {required_banners}")

# Extract codes from the "Plan Système" and "CodeX3" columns and group by banner
codes_by_banner = {}
for _, row in filtered_df.iterrows():
    banner = row['Banner']
    plan_systeme = row['Plan Système']
    codes = extract_codes(plan_systeme)
    
    # Add extracted codes from "Plan Système"
    if banner in codes_by_banner:
        codes_by_banner[banner].update(codes)
    else:
        codes_by_banner[banner] = set(codes)
    
    # Add "CodeX3" if it's not empty
    code_x3 = row['CodeX3']
    if pd.notna(code_x3):
        cleaned_code_x3 = code_x3.strip()  # Clean up any extra spaces
        codes_by_banner[banner].add(cleaned_code_x3)

# Fetch existing codes from the database
# Modify the server name to match your environment's server address
server = "# Adjust to your database server address"
results = []
not_found_codes = []

for banner, codes in codes_by_banner.items():
    db_name = get_database_name(banner)
    if db_name:
        logging.info(f"Fetching existing codes for banner: {banner}")
        existing_codes = fetch_existing_codes(db_name, list(codes), server)
        
        for code in codes:
            if code in existing_codes:
                results.append({'Banner': banner, 'Code_Article': code})
            else:
                not_found_codes.append({'Banner': banner, 'Code_Article': code})

# Merge results and remove duplicates
final_df = pd.DataFrame(results).drop_duplicates()
logging.info("Merged the codes from both columns and removed duplicates.")

# Save the results to an Excel file
# Adjust the output file path to the location where you want to save the file
output_file_path = Path("# Modify this path to your desired output location")
try:
    with pd.ExcelWriter(output_file_path, mode='w', engine='openpyxl') as writer:
        # Save "1" and "2" codes in "ALL" sheet
        ag_all_df = final_df[final_df['Banner'].isin(["1", "2"])]  # Modify banner names if necessary
        ag_all_df.to_excel(writer, sheet_name="ALL", index=False)
        
        # Save not found codes
        not_found_df = pd.DataFrame(not_found_codes)
        if not not_found_df.empty:
            not_found_df.to_excel(writer, sheet_name="Codes_Not_Found", index=False)
            logging.info("Saved 'Codes_Not_Found' sheet with missing codes.")
        else:
            logging.info("All codes were found in the database.")
        
        # Save each banner's codes in a separate sheet
        for banner, group_df in final_df.groupby('Banner'):
            if banner not in ["1", "2"]:  # Skip "1" and "2" as they were merged earlier
                sheet_name = f"{banner}"[:31]  # Sheet name length limit is 31 characters
                group_df.to_excel(writer, sheet_name=sheet_name, index=False)
                logging.info(f"Saved sheet for banner: {sheet_name}")
    
    logging.info(f"File successfully saved at {output_file_path}")

except Exception as e:
    logging.error(f"Error while saving the file: {e}")
