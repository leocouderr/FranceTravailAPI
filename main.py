from offres_emploi import Api
from offres_emploi.utils import dt_to_str_iso
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os


# Initialize the API client
client = Api(client_id="PAR_alfredrestauration_487210a282c499a6c091cb90c60c863126cf9ad19ee41d15991f6855364f66f2", 
             client_secret="96a3abc286012daf4b5ce3c7ff44fcec45d1eff0e135df1b2260d89f08c3ae57")

# Define the date range
start_dt = datetime.datetime(2024, 1, 1, 12, 30)
end_dt = datetime.datetime.today()

# Base parameters for the API request
params_template = {
    "motsCles": "restauration",
    'minCreationDate': dt_to_str_iso(start_dt),
    'maxCreationDate': dt_to_str_iso(end_dt),
}

# Define the ranges for pagination
ranges = [
    (0, 149), (150, 299), (300, 449), (450, 599),
    (600, 749), (750, 899), (900, 1049), (1050, 1149)
]

# Initialize a list to store dataframes
dataframes = []

for start, end in ranges:
    # Update the range parameter for the current batch
    params = params_template.copy()
    params['range'] = f"{start}-{end}"
    print(f"Fetching job listings from {start} to {end}...")

    # Perform the API request
    search_on_big_data = client.search(params=params)
    df = pd.json_normalize(search_on_big_data["resultats"])

    # Append the results to the list
    dataframes.append(df)

# Combine all dataframes into a single dataframe
combined_df = pd.concat(dataframes, ignore_index=True)

# Debug: Print the total number of rows
print(f"Total job listings fetched: {combined_df.shape[0]}")

# Google Sheets API setup
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials_info = json.loads(os.environ.get("GOOGLE_CREDENTIALS"))
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_info, scope)
client = gspread.authorize(credentials)

# Open the Google Sheet
spreadsheet = client.open('FranceTravailListings')  # Use your sheet's name
worksheet = spreadsheet.sheet1

# Read existing data from Google Sheets into a DataFrame
existing_data = pd.DataFrame(worksheet.get_all_records())

# Convert scraped results into a DataFrame
new_data = combined_df

# Combine and remove duplicates
if not existing_data.empty:
    combined_data = pd.concat([existing_data, new_data], ignore_index=True).drop_duplicates(
        subset=['id']
    )
else:
    combined_data = new_data

# Debug: Print the number of rows to append
rows_to_append = combined_data.shape[0]
print(f"Rows to append: {rows_to_append}")

# Handle NaN, infinity values before sending to Google Sheets
# Replace NaN values with 0 or another placeholder (you can customize this)
combined_data = combined_data.fillna(0)

# Replace infinite values with 0 or another placeholder
combined_data.replace([float('inf'), float('-inf')], 0, inplace=True)

# Optional: Ensure all float types are valid (e.g., replace any invalid float with 0)
combined_data = combined_data.applymap(lambda x: 0 if isinstance(x, float) and (x == float('inf') or x == float('-inf') or x != x) else x)

# Optional: Ensuring no invalid values (like lists or dicts) in any column
def clean_value(value):
    if isinstance(value, (list, dict)):
        return str(value)  # Convert lists or dicts to string
    return value

combined_data = combined_data.applymap(clean_value)

#Remove rows with Mesure POEI and Consultant as Intitulé
combined_data = combined_data[
    ~combined_data["intitule"].str.contains("Mesure POEI|Consultant Freelance Expert en Hôtellerie et Restauration", na=False)
]


# Update Google Sheets with the combined data
worksheet.clear()  # Clear existing content
worksheet.update([combined_data.columns.tolist()] + combined_data.values.tolist())

print("New rows successfully appended to Google Sheets without duplicates!")
