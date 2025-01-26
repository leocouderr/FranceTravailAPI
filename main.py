from offres_emploi import Api
from offres_emploi.utils import dt_to_str_iso
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

client = Api(client_id="PAR_alfredrestauration_487210a282c499a6c091cb90c60c863126cf9ad19ee41d15991f6855364f66f2", 
             client_secret="96a3abc286012daf4b5ce3c7ff44fcec45d1eff0e135df1b2260d89f08c3ae57")


start_dt = datetime.datetime(2024, 1, 1, 12, 30)
end_dt = datetime.datetime.today()
#end_dt = datetime.datetime(2024, 1, 2, 12, 30)

params = {
    "motsCles": "restauration",
    'minCreationDate': dt_to_str_iso(start_dt),
    'maxCreationDate': dt_to_str_iso(end_dt)

}
search_on_big_data = client.search(params=params)

df = pd.json_normalize(search_on_big_data["resultats"])

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
new_data = df

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

# Update Google Sheets with the combined data
worksheet.clear()  # Clear existing content
worksheet.update([combined_data.columns.tolist()] + combined_data.values.tolist())

print("New rows successfully appended to Google Sheets without duplicates!")
