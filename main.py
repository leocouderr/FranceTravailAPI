from offres_emploi import Api
from offres_emploi.utils import dt_to_str_iso
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os
import numpy as np
import unicodedata
import re
import requests
import asyncio
import httpx
import nest_asyncio  # Allows running async code in Jupyter
from gspread.exceptions import APIError
import time




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
    "sort": 1
}

# Define the ranges for pagination
ranges = [(0, 149)]
#, (150, 299), (300, 449), (450, 599),
 #  (600, 749), (750, 899), (900, 1049), (1050, 1149)
#]

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

#5 listings les plus récents
print(end_dt)
print(combined_df.dateCreation.head())
print(combined_df.id.head())

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

# Debug: Print the number of rows from get_all_records in existing google sheets
existing_rows_in_google_sheets = existing_data.shape[0]
print(f"Existing rows in google sheets: {existing_rows_in_google_sheets}")

# Convert scraped results into a DataFrame
new_data = combined_df
print(f"Head nouvelles annonces: {new_data.head()}")
#check urls partenaire
#print(f"check urls partenaire: {combined_df['origineOffre.urlOrigine'].head(15)}")

#make dateCreation a date dtype
new_data["dateCreation"] = pd.to_datetime(new_data["dateCreation"], format="%Y-%m-%dT%H:%M:%S.%fZ", errors="ignore").dt.strftime("%Y-%m-%d")
print(f"Check Rows date after modifiying date column: {new_data.dateCreation.head()}")

# Ensure 'lieuTravail.codePostal' is a string
new_data["lieuTravail.codePostal"] = new_data["lieuTravail.codePostal"].astype(str)

# Add leading zeros if the postal code has less than 5 characters
new_data["lieuTravail.codePostal"] = new_data["lieuTravail.codePostal"].str.zfill(5)

# Remove numbers and hyphens from 'lieuTravail.libelle'
new_data["Cleaned_Libelle"] = new_data["lieuTravail.libelle"].str.replace(r"^\d+\s*-\s*", "", regex=True).str.strip()

# Create 'Localisation' column
new_data["Localisation"] = (new_data["lieuTravail.codePostal"] + ", " + new_data["Cleaned_Libelle"] + ", France").str.upper()

# Drop the intermediate column if not needed
new_data.drop(columns=["Cleaned_Libelle"], inplace=True)

# Apply nest_asyncio to fix event loop issue in Jupyter
#nest_asyncio.apply()

# Data Gouv API URL
API_URL = "https://api-adresse.data.gouv.fr/search"

# Function to call API asynchronously with retries
async def get_geodata(client, address, retries=3):
    params = {"q": address, "limit": 1}

    for attempt in range(retries):
        try:
            response = await client.get(API_URL, params=params, timeout=5)

            if response.status_code == 503:  # Server overloaded
                print(f"503 Error - Retrying {address} (Attempt {attempt+1})...")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                continue

            response.raise_for_status()  # Raise error if response is bad
            data = response.json()

            if data["features"]:
                props = data["features"][0]["properties"]
                geo = data["features"][0]["geometry"]["coordinates"]

                ville = props.get("city", "")
                code_postal = props.get("postcode", "")
                longitude = geo[0] if geo else None
                latitude = geo[1] if geo else None
                contexte = props.get("context", "")

                # Extract region name (after second comma)
                region = contexte.split(", ")[-1] if contexte.count(",") >= 2 else ""

                return ville, code_postal, longitude, latitude, region
        
        except Exception as e:
            print(f"Error fetching data for {address} (Attempt {attempt+1}): {e}")
        
        await asyncio.sleep(2 ** attempt)  # Exponential backoff for retries

    return None, None, None, None, None  # Return empty values if all retries fail

# Async function to process all addresses with rate limiting
async def process_addresses(address_list, delay_between_requests=0.017):  # 1/60 = ~0.017s
    results = []
    async with httpx.AsyncClient() as client:
        for i, address in enumerate(address_list):
            result = await get_geodata(client, address)
            results.append(result)
            
            #print(f"Processed {i + 1} / {len(address_list)}")

            # Respect 60 requests per second limit
            await asyncio.sleep(delay_between_requests)  

    return results

# Run API calls asynchronously
addresses = new_data["Localisation"].tolist()
geodata_results = asyncio.run(process_addresses(addresses))

# Assign the results to the DataFrame
new_data[["Ville", "Code Postal", "Longitude", "Latitude", "Region"]] = pd.DataFrame(geodata_results)

# Add "France Travail" column
new_data["Source"] = "France Travail"

# combine new and existing data, reorder columns in new data if necessary, add new columns if necessary
if not existing_data.empty:
    # Get the column order from existing_data
    existing_cols = list(existing_data.columns)
    # Find extra columns in new_data that aren't in existing_data
    extra_cols = [col for col in new_data.columns if col not in existing_cols]
    # The final union of columns: existing order followed by extra columns
    union_cols = existing_cols + extra_cols

    # Reindex both DataFrames using the union of columns
    existing_aligned = existing_data.reindex(columns=union_cols)
    new_aligned = new_data.reindex(columns=union_cols)

    # Concatenate the aligned DataFrames and remove duplicates based on 'id'
    combined_data = pd.concat([new_aligned, existing_aligned], ignore_index=True).drop_duplicates(subset=['origineOffre.urlOrigine'], keep='first')
else:
    combined_data = new_data.copy()


# Debug: Print the number of rows to append
rows_to_append = combined_data.shape[0]
print(f"Rows to append before filtering: {rows_to_append}")
print(f"Check date after column mapping: {combined_data.dateCreation.head()}")
print(f"Check date after column mapping: {combined_data.id.head()}")
#check urls partenaire
#print(f"check urls partenaire: {combined_data[\'origineOffre.urlOrigine\'].head(15)}")


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
    ~combined_data["intitule"].str.contains("Mesure POEI|Consultant Freelance Expert en Hôtellerie et Restauration", case=False, na=False)
]

#check after Mesure POEI
print(f"Check date after column mapping: {combined_data.dateCreation.head()}")
print(f"Check date after column mapping: {combined_data.id.head()}")

#check urls partenaire
#print(f"check urls partenaire: {combined_data[\'origineOffre.urlOrigine\'].head(15)}")

# Replace NaN and infinite values with None (which converts to null in JSON)
combined_data = combined_data.replace([np.nan, np.inf, -np.inf], None)

#check before titre sans accent
print(f"Check date after column mapping: {combined_data.dateCreation.head()}")
print(f"Check date after column mapping: {combined_data.id.head()}")

#add column titre de annonce sans accents ni special characters
def remove_accents_and_special(text):
    # Normalize the text to separate characters from their accents.
    normalized = unicodedata.normalize('NFD', text)
    # Remove the combining diacritical marks.
    without_accents = ''.join(c for c in normalized if not unicodedata.combining(c))
    # Replace special characters (-, ') with a space.
    cleaned = re.sub(r"[-']", " ", without_accents)
    # Remove other special characters (retain letters, digits, and whitespace).
    cleaned = re.sub(r"[^A-Za-z0-9\s]", "", cleaned)
    return cleaned

# Create the new column "Titre annonce sans accent" by applying the function on "intitule".
combined_data["TitreAnnonceSansAccents"] = combined_data["intitule"].apply(
    lambda x: remove_accents_and_special(x) if isinstance(x, str) else x
)

#check after titre sans accent
print(f"Check date after column mapping: {combined_data.dateCreation.head()}")
print(f"Check date after column mapping: {combined_data.id.head()}")

#check urls partenaire
#print(f"check urls partenaire: {combined_data[\'origineOffre.urlOrigine\'].head(15)}")

#filter out listings from indeed to avoid duplicates and only keep the url in the json value
def process_partenaire(cell):
    # If the cell is already a valid URL string, don't change it.
    if isinstance(cell, str) and cell.startswith("https"):
        return cell
    # If the cell is a JSON string, try to parse it.
    if isinstance(cell, str):
        try:
            cell = json.loads(cell)
        except Exception:
            return None
    # If cell is not a list or is empty, return None.
    if not cell or not isinstance(cell, list):
        return None
    
    # Check each partner in the list; if any partner has 'nom' equal to 'INDEED', return None.
    for partner in cell:
        nom = partner.get('nom', '').strip().upper()
        if nom == 'INDEED':
            return None
    
    # Otherwise, return the URL of the first partner.
    return cell[0].get('url', '')

# Apply the function to the 'origineOffre.partenaires' column.
combined_data["origineOffre.partenaires"] = combined_data["origineOffre.partenaires"].apply(process_partenaire)

#check urls partenaire
#print(f"check urls partenaire: {combined_data[\'origineOffre.urlOrigine\'].head(15)}")

#check after indeed filtering
print(f"Check date after column mapping: {combined_data.dateCreation.head()}")
print(f"Check date after column mapping: {combined_data.id.head()}")

# For rows where the processed value is NaN, None, or an empty string, substitute the fallback from 'origineOffre.urlOrigine'.
combined_data["origineOffre.partenaires"] = combined_data.apply(
    lambda row: row["origineOffre.partenaires"]
    if pd.notna(row["origineOffre.partenaires"]) and row["origineOffre.partenaires"] not in [None, '',0,"O"]
    else row["origineOffre.urlOrigine"],
    axis=1
)

#check urls partenaire
#print(f"check urls partenaire: {combined_data[\'origineOffre.urlOrigine\'].head(15)}")

#last check for out of range json float and convert to json compliant None
def safe_json_value(x):
    if isinstance(x, float):
        # Check for NaN or infinite values
        if np.isnan(x) or np.isinf(x):
            return None
    return x

combined_data = combined_data.applymap(safe_json_value)

#last check to remove duplicates based on ID
# Normalize the 'id' column: convert everything to string and remove surrounding whitespace
#combined_data['id'] = combined_data['id'].astype(str).str.strip()
#combined_data = combined_data.drop_duplicates(subset=['origineOffre.urlOrigine'], keep='first')

# Debug: Print the number of rows to append after filtering
rows_to_append_after_filtering = combined_data.shape[0]
print(f"Rows to append after filtering: {rows_to_append_after_filtering}")
print(f"Check date after column mapping: {combined_data.dateCreation.head()}")
print(f"Check date after column mapping: {combined_data.id.head()}")

MAX_RETRIES = 5  # Number of times to retry
RETRY_DELAY = 30  # Seconds to wait before retrying

def safe_update(worksheet, data):
    for attempt in range(MAX_RETRIES):
        try:
            worksheet.update(data)
            print("Google Sheets update successful!")
            return
        except APIError as e:
            print(f"Error: {e}. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    print("Max retries reached. Update failed.")

# Convert your DataFrame to a list and update the sheet safely
data_to_upload = [combined_data.columns.tolist()] + combined_data.values.tolist()
safe_update(worksheet, data_to_upload)

# Update Google Sheets with the combined data
#worksheet.clear()  # Clear existing content
#worksheet.update([combined_data.columns.tolist()] + combined_data.values.tolist())

print("New rows successfully appended to Google Sheets without duplicates!")
