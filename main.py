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
import requests
from datetime import datetime, timedelta
from openai import OpenAI
import instructor
from enum import Enum
from pydantic import BaseModel, Field, field_validator, constr, ValidationError, model_validator
from typing import List, Optional, Any

CLIENT_ID = 'PAR_alfredrestauration_487210a282c499a6c091cb90c60c863126cf9ad19ee41d15991f6855364f66f2'
CLIENT_SECRET = '96a3abc286012daf4b5ce3c7ff44fcec45d1eff0e135df1b2260d89f08c3ae57'
TOKEN_URL = 'https://entreprise.pole-emploi.fr/connexion/oauth2/access_token?realm=%2Fpartenaire'
API_URL = 'https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search'

def get_access_token():
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'api_offresdemploiv2 o2dsoffre'
    }
    response = requests.post(TOKEN_URL, data=payload)
    response.raise_for_status()
    return response.json()['access_token']

def dt_to_str_iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch_all_offers(token, base_search_params=None):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Ranges: 8 pages of 150 items each = 1200 max
    ranges = [
        (0, 149) 
        #,(150, 299), (300, 449),
        #(450, 599), (600, 749), (750, 899),
       # (900, 1049), (1050, 1149)
    ]

    all_offers = []
    seen_ids = set()

    for start, end in ranges:
        params = dict(base_search_params)  # copy base params
        params['range'] = f"{start}-{end}"

        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        offers = data.get('resultats', [])
        new_offers = [o for o in offers if o['id'] not in seen_ids]

        for o in new_offers:
            seen_ids.add(o['id'])

        all_offers.extend(new_offers)
        print(f"Fetched {len(new_offers)} new offers from range {start}-{end}")

        if len(offers) < (end - start + 1):  # Stop early if fewer results
            break

    print(f"✅ Total unique offers fetched: {len(all_offers)}")
    return all_offers

# === Main ===
if __name__ == "__main__":
    token = get_access_token()

    today = datetime.utcnow()
    one_month_ago = today - timedelta(days=30)

    base_search_params = {
        'motsCles': 'restauration',
        'minCreationDate': dt_to_str_iso(one_month_ago),
        'maxCreationDate': dt_to_str_iso(today),
        'sort': 1
    }

    offers = fetch_all_offers(token, base_search_params)
    
combined_df = pd.json_normalize(offers)
combined_df = combined_df.reset_index(drop=True)

# Combine all dataframes into a single dataframe
#combined_df = pd.concat(dataframes, ignore_index=True)

# Debug: Print the total number of rows
print(f"Total job listings fetched: {combined_df.shape[0]}")
print(f"Total columns fetched: {combined_df.shape[1]}")
print(combined_df.head(5))


#------------------------CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------

from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account


# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

# Query existing URLs from your BigQuery table
query = """
    SELECT origineOffre_urlOrigine
    FROM `databasealfred.jobListings.franceTravail`
    WHERE origineOffre_urlOrigine IS NOT NULL
"""
query_job = client.query(query)

# Convert results to a set for fast lookup
existing_urls = {row.origineOffre_urlOrigine for row in query_job}

print(f"Loaded {len(existing_urls)} URLs from BigQuery")

before_count = len(combined_df)

combined_df = combined_df[
    ~combined_df["origineOffre.urlOrigine"].isin(existing_urls)
].copy()

after_count = len(combined_df)

print(f"🧹 Removed {before_count - after_count} duplicate URLs")
print(f"✅ Remaining new job listings: {after_count}")

#------------------------ FIN CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------

# Convert scraped results into a DataFrame
new_data = combined_df

#make dateCreation a date dtype
new_data["dateCreation"] = pd.to_datetime(new_data["dateCreation"], format="%Y-%m-%dT%H:%M:%S.%fZ", errors="ignore").dt.strftime("%Y-%m-%d")

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

# Remove rows that already exist in existing_data to save computational time
#new_data = new_data[~new_data["origineOffre.urlOrigine"].isin(existing_data["origineOffre.urlOrigine"])]

# -------- DEBUT CHATGPT DATA ENRICHMENT --------------------------------------------------------------------------------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client_ai = instructor.patch(OpenAI(api_key=OPENAI_API_KEY))

class Loge(str, Enum):
    LOGE = "Logé"
    NON_LOGE = "Non Logé"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"

class TypeContrat(str, Enum):
    CDD = "CDD"
    CDI = "CDI"
    STAGE = "Stage"
    APPRENTISSAGE = "Apprentissage"
    INTERIM = "Interim"
    EXTRA = "Extra"
    SAISONNIER = "Saisonnier"
    ALTERNANCE = "Alternance"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class CoupleAccepte(str, Enum):
    ACCEPTE = "Couple accepté"
    NON_ACCEPTE = "Couple non accepté"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class CategorieEtablissement(str, Enum):
    GASTRONOMIQUE = "Gastronomique"
    BRASSERIE = "Brasserie"
    BAR = "Bar"
    RAPIDE = "Restauration rapide"
    COLLECTIVE = "Restauration collective"
    RESTAURANT = "Restaurant"
    HOTEL_LUXE = "Hôtel luxe"
    HOTEL = "Hôtel"
    CAMPING = "Camping"
    CAFE = "Café/Salon de thé"
    BOULANGERIE = "Boulangerie/Patisserie"
    ETOILE = "Etoile Michelin"
    PALACE = "Palace"
    TRAITEUR = "Traiteur/Événementiel/Banquet"
    SPA = "Spa"
    LABORATOIRE = "Laboratoire"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class CategorieJob1(str, Enum):
    RESTAURATION = "Restauration"
    HOTELLERIE = "Hôtellerie"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"
    GARBAGE = "Support & Back-office"


class CategorieJob2(str, Enum):
    SALLE = "Salle & Service"
    DIRECTION = "Direction & Management"
    SUPPORT = "Support & Back-office"
    CUISINE = "Cuisine"
    SPA = "Spa & Bien-être"
    ETAGES = "Étages & Housekeeping"
    BAR = "Bar & Sommellerie"
    RECEPTION = "Réception & Hébergement"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class CategorieJob3(str, Enum):
    CHEF_EXECUTIF = "Chef exécutif"
    CHEF_CUISINE = "Chef de cuisine"
    SOUS_CHEF = "Sous-chef"
    CHEF_PARTIE = "Chef de partie"
    COMMIS_CUISINE = "Commis de cuisine"
    PATISSIER = "Pâtissier"
    BOULANGER = "Boulanger"
    PIZZAIOLO = "Pizzaiolo"
    TRAITEUR = "Traiteur"
    MANAGER = "Manager / Responsable"
    EMPLOYE = "Employé polyvalent"
    PLONGEUR = "Plongeur"
    STEWARD = "Steward"
    DIRECTEUR = "Directeur"
    RESPONSABLE_SALLE = "Responsable de salle"
    MAITRE_HOTEL = "Maître d’hôtel"
    CHEF_RANG = "Chef de rang"
    COMMIS_SALLE = "Commis de salle / Runner"
    SERVEUR = "Serveur"
    SOMMELIER = "Sommelier"
    BARMAN = "Barman"
    BARISTA = "Barista"
    RECEPTIONNISTE = "Réceptionniste / Hôte d’accueil"
    CONCIERGE = "Concierge"
    BAGAGISTE = "Bagagiste / Voiturier"
    VALET = "Valet / Femme de chambre"
    MARKETING = "Marketing / Communication"
    AGENT_RESERVATIONS = "Agent de réservations"
    REVENUE_MANAGER = "Revenue manager"
    GOUVERNANT = "Gouvernant(e)"
    SPA_PRATICIEN = "Spa praticien(ne) / Ésthéticien(ne)"
    COACH = "Coach sportif"
    MAITRE_NAGEUR = "Maître-nageur"
    ANIMATION = "Animation / Événementiel"
    COMMERCIAL = "Commercial"
    RH = "RH / Paie"
    COMPTABILITE = "Comptabilité / Contrôle de gestion"
    TECHNICIEN = "Technicien / Maintenance"
    IT = "IT / Data"
    HACCP = "HACCP manager"
    CUISINIER = "Cuisinier"
    LIMONADIER = "Limonadier"
    ALLOTISSEUR = "Allotisseur"
    APPROVISIONNEUR = "Approvisionneur / Économe"
    AGENT_SECURITE = "Agent de sécurité"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class Urgent(str, Enum):
    URGENT = "Urgent"
    NON_URGENT = "Non Urgent"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"

class Environnement(str, Enum):
    CENTRE_VILLE = "Centre ville"
    BORD_MER = "Bord de mer"
    MONTAGNE = "Montagne"
    BANLIEUE = "Banlieue"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class ChaineIndependant(str, Enum):
    CHAINE = "Chaine"
    INDEPENDANT = "Indépendant"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class TempsTravail(str, Enum):
    PLEIN_TEMPS = "Plein temps"
    TEMPS_PARTIEL = "Temps partiel"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class HorairesTravail(str, Enum):
    JOUR = "Jour"
    NUIT = "Nuit"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class Experience(str, Enum):
    DEBUTANT = "Débutant"
    CONFIRME = "Confirmé"
    VIDE = ""
    NON_SPECIFIE = "Non spécifié"


class DureeModel(BaseModel):
    value: str


class HeuresParSemaineModel(BaseModel):
    heures: Optional[int] = None

    # v2 field validator
    @field_validator("heures", mode="before")
    def parse_heures(cls, v):
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            match = re.search(r"\d+", v)
            if match:
                return int(match.group())
        return None

class DateDebutModel(BaseModel):
    value: str

class SalaireModel(BaseModel):
    value: str

# --- Base model that ties everything together ---
class JobClassification(BaseModel):
    IA_Logé: Loge
    IA_Type_de_contrat: TypeContrat
    IA_Salaire: SalaireModel
    IA_Couple_accepté: CoupleAccepte
    IA_Catégorie_établissement: CategorieEtablissement
    IA_Catégorie_Job_1: CategorieJob1
    IA_Catégorie_Job_2: CategorieJob2
    IA_Catégorie_Job_3: CategorieJob3
    IA_Urgent: Urgent
    IA_Date_de_début: DateDebutModel
    IA_Durée: DureeModel
    IA_Type_environnement: Environnement
    IA_Chaine_Indépendant: ChaineIndependant
    IA_Temps_de_travail: TempsTravail
    IA_Horaires_de_travail: HorairesTravail
    IA_Heures_par_semaine: HeuresParSemaineModel
    IA_Éxpérience: Experience

SYSTEM_PROMPT = """You are a classifier for job listings in the hospitality industry in France. You are an expert and absolutely have to respect the 
instructions. Each category can ONLY take one the value that are specified for it.
The success of my business depends on you so double check!!
    "IA_Logé": when accomodation or help with accomodation is provided "Logé" else "Non logé",
        "IA_Type_de_contrat": it MUST BE one of ["CDD", "CDI", "Stage", "Apprentissage", "Interim", "Extra", "Saisonnier", "Alternance"],
        "IA_Salaire": the highest salary offered in format "X€/heure" or "X€/mois" or "X€/an", or "" if not specified,
        "IA_Couple_accepté": "Couple accepté" or "",
    	"IA_Catégorie_établissement": it MUST BE one of the following and CANNOT be empty ["Gastronomique","Brasserie","Bar","Restauration rapide","Restauration collective","Restaurant","Hôtel luxe","Hôtel","Camping","Café/Salon de thé”,”Boulangerie/Patisserie”,”Etoile Michelin","Palace”, “Traiteur/Événementiel/Banquet”,“Spa”, “Laboratoire”],
    	"IA_Catégorie_Job_1":  it MUST BE one of the following and it cannot be empty [“Restauration”, “Hôtellerie”],
    	“IA_Catégorie_Job_2”:  it MUST BE one of and the most relevant, it cannot be empty [“Salle & Service”, “Direction & Management”, “Support & Back-office”, “Cuisine”, “Spa & Bien-être”, “Étages & Housekeeping”, “Bar & Sommellerie”, “Réception & Hébergement”],
        “IA_Catégorie_Job_3”: it has to be one of the following and the most relevant, it cannot be empty ["Chef exécutif","Chef de cuisine","Sous-chef","Chef de partie","Commis de cuisine","Pâtissier","Boulanger","Pizzaiolo","Traiteur","Manager / Responsable","Employé polyvalent","Plongeur","Steward","Directeur","Responsable de salle","Maître d’hôtel","Chef de rang","Commis de salle / Runner","Serveur","Sommelier","Barman","Barista","Réceptionniste / Hôte d’accueil","Concierge","Bagagiste / Voiturier","Valet / Femme de chambre","Marketing / Communication","Agent de réservations","Revenue manager","Gouvernant(e)","Spa praticien(ne) / Ésthéticien(ne)","Coach sportif","Maître-nageur","Animation / Événementiel","Commercial","RH / Paie","Comptabilité / Contrôle de gestion","Technicien / Maintenance","IT / Data","HACCP manager","Cuisinier","Limonadier","Allotisseur","Approvisionneur / Économe","Agent de sécurité"],
    	"IA_Urgent": "Urgent" or "", it takes "Urgent" only when the starting date is within 2 weeks of the date_scraping or when it is explicitly mentioned in the description
        "IA_Date_de_début": starting date in format YYYY-MM-DD if present, else "",
        "IA_Durée": contract duration like "N days", "N weeks", "N months", or "Indéfini",
        "IA_Type_environnement”: one of ["Centre ville","Bord de mer","Montagne","Banlieue"],
    	“IA_Chaine_Indépendant”: when the company posting the job listing is part of a group or bigger company "Chaine", else ""
        "IA_Temps_de_travail": "Plein temps" or "Temps partiel",
        "IA_Horaires_de_travail": "Jour" or "Nuit",
        "IA_Heures_par_semaine": return a number not a string ! the number of hours worked per week if available, when the contract is less than a week just put how many hours it , else “”,
    	“IA_Éxpérience” one the following [“Débutant”, “Confirmé”]

    Strictly output without explanations."""


def classify_job_listing(ticket_text: str) -> JobClassification:
    response = client_ai.chat.completions.create(
            model="gpt-4o-mini",
            max_retries=3,
            response_model=JobClassification,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": ticket_text}
            ],
            temperature=0
        )
    return response

# Define the columns you want to include
cols_to_join = [
    "intitule",
    "description",
    "dateCreation",
    "typeContrat",
    "experienceLibelle",
    "dureeTravailLibelle",
    "dureeTravailLibelleConverti",
    "secteurActiviteLibelle",
    "salaire.libelle",
    "Ville",
    "Code Postal",
    "Longitude",
    "Latitude",
    "Region"
]

# Keep only the columns that exist in your DataFrame (to avoid KeyErrors)
cols_to_join = [col for col in cols_to_join if col in new_data.columns]

# Build the string only for those columns
new_data["row_as_string"] = new_data.apply(
    lambda row: ", ".join(
        [f'"{col}":"{row[col]}"' for col in cols_to_join if pd.notnull(row[col])]
    ),
    axis=1
)


# Apply your classify_job_listing function to each row
result = new_data["row_as_string"].apply(classify_job_listing)

# If you want, convert the results (list of dicts) into a DataFrame
classified_df = pd.DataFrame(result.tolist())

base_model_columns = list(JobClassification.model_fields.keys())

def get_value(cell, column_name=None):
    if isinstance(cell, tuple) and len(cell) == 2:
        val = cell[1]

        # Special case for IA_Heures_par_semaine
        if column_name == "IA_Heures_par_semaine" and hasattr(val, "heures"):
            return val.heures  # directly the int

        # Other enums / objects
        if hasattr(val, "value"):
            return val.value
        return str(val)
    elif hasattr(cell, "value"):
        return cell.value
    return str(cell)

classified_df = pd.DataFrame([
    [get_value(cell, col) for cell, col in zip(row, base_model_columns)]
    for row in classified_df.values
], columns=base_model_columns)

new_data = new_data.drop(columns=["row_as_string"])

# -------- FIN CHATGPT DATA ENRICHMENT ----------------------------------------------------------------------------------------------

# Merge with original sample
new_data = pd.concat([new_data.reset_index(drop=True), classified_df], axis=1)

# -------- DEBUT EMBEDING OPENAI LARGE ----------------------------------------------------------------------------------------------

from tqdm import tqdm
import time

# --- Function to build a natural-language sentence per row ---
def build_sentence(row):
    return (
        f"Le titre de ce job est {row.get('titre', '')} et est à {row.get('Ville', '')} "
        f"dans le département {row.get('Code Postal', '')} de la région {row.get('Region', '')}. "
        f"Ce poste est {row.get('IA_Logé', '')} et le contrat est en {row.get('IA_Type_de_contrat', '')}. "
        f"Le salaire est de {row.get('IA_Salaire', '')}. Particularité : {row.get('IA_Couple_accepté', '')} "
        f"et {row.get('IA_Urgent', '')}. Le job est dans un {row.get('IA_Catégorie_établissement', '')} "
        f"et dans le secteur de {row.get('IA_Catégorie_Job_1', '')}. "
        f"Détails du job : {row.get('IA_Catégorie_Job_2', '')} {row.get('IA_Catégorie_Job_3', '')}. "
        f"Il commence le {row.get('IA_Date_de_début', '')} et dure {row.get('IA_Durée', '')}. "
        f"L'établissement est situé {row.get('IA_Type_environnement', '')} et est {row.get('IA_Chaine_Indépendant', '')}. "
        f"C'est un contrat à {row.get('IA_Temps_de_travail', '')} et travail de {row.get('IA_Horaires_de_travail', '')} "
        f"pendant {row.get('IA_Heures_par_semaine', '')} heures par semaine. "
        f"Le poste est pour les {row.get('IA_Éxpérience', '')}."
    )

# --- Create the combined natural-language field ---
new_data["combined_text"] = new_data.apply(build_sentence, axis=1)

# --- Define batching function ---
def embed_in_batches(texts, batch_size=1500):
    """
    Embed a list of texts in batches to avoid API rate limits and memory issues.
    """
    embeddings = []
    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding batches"):
        batch = texts[i:i + batch_size]
        try:
            response = client_ai.embeddings.create(
                model="text-embedding-3-small",
                input=batch
            )
            batch_embeddings = [item.embedding for item in response.data]
            embeddings.extend(batch_embeddings)
        except Exception as e:
            print(f"Error at batch {i}: {e}")
            time.sleep(5)  # Wait a bit and retry
            continue
    return embeddings

# --- Generate embeddings ---
new_data["OpenAIEmbeddedSmall"] = embed_in_batches(new_data["combined_text"].tolist(), batch_size=1500)

# --- Optional: Clean up ---
new_data.drop(columns=["combined_text"], inplace=True)

# -------- FIN EMBEDING OPENAI LARGE ----------------------------------------------------------------------------------------------


# combine new and existing data, reorder columns in new data if necessary, add new columns if necessary
#if not existing_data.empty:
#    # Get the column order from existing_data
#    existing_cols = list(existing_data.columns)
#    # Find extra columns in new_data that aren't in existing_data
#    extra_cols = [col for col in new_data.columns if col not in existing_cols]
#    # The final union of columns: existing order followed by extra columns
#    union_cols = existing_cols + extra_cols

    # Reindex both DataFrames using the union of columns
#    existing_aligned = existing_data.reindex(columns=union_cols)
#    new_aligned = new_data.reindex(columns=union_cols)

    # Concatenate the aligned DataFrames and remove duplicates based on 'id'
#    combined_data = pd.concat([new_aligned, existing_aligned], ignore_index=True).drop_duplicates(subset=['origineOffre.urlOrigine'], keep='first')
#else:
#    combined_data = new_data.copy()

# -------- DEBUT DATA VALIDATION EMPTY VALUES OPENAI ----------------------------------------------------------------------------------------------

# Select columns starting with "IA_"
ia_cols = [col for col in new_data.columns if col.startswith("IA_")]

# Replace "" with "Non spécifié" in those columns only
new_data[ia_cols] = new_data[ia_cols].replace("", "Non spécifié")

new_data["IA_Catégorie_Job_1"] = new_data["IA_Catégorie_Job_1"].replace("Support & Back-office","Non spécifié")

# -------- FIN DATA VALIDATION EMPTY VALUES OPENAI ----------------------------------------------------------------------------------------------


# Debug: Print the number of rows to append
rows_to_append = new_data.shape[0]
print(f"Rows to append before filtering: {rows_to_append}")
print(f"Check date after column mapping: {new_data.dateCreation.head()}")
print(f"Check date after column mapping: {new_data.id.head()}")
#check urls partenaire
#print(f"check urls partenaire: {new_data[\'origineOffre.urlOrigine\'].head(15)}")


# Handle NaN, infinity values before sending to Google Sheets
# Replace NaN values with 0 or another placeholder (you can customize this)
new_data = new_data.fillna(0)

# Replace infinite values with 0 or another placeholder
new_data.replace([float('inf'), float('-inf')], 0, inplace=True)

# Optional: Ensure all float types are valid (e.g., replace any invalid float with 0)
new_data = new_data.applymap(lambda x: 0 if isinstance(x, float) and (x == float('inf') or x == float('-inf') or x != x) else x)

# Optional: Ensuring no invalid values (like lists or dicts) in any column
def clean_value(value):
    if isinstance(value, (list, dict)):
        return str(value)  # Convert lists or dicts to string
    return value

new_data = new_data.applymap(clean_value)

#Remove rows with Mesure POEI and Consultant as Intitulé
new_data = new_data[
    ~new_data["intitule"].str.contains("Mesure POEI|Consultant Freelance Expert en Hôtellerie et Restauration", case=False, na=False)
]

#check urls partenaire
#print(f"check urls partenaire: {new_data[\'origineOffre.urlOrigine\'].head(15)}")

# Replace NaN and infinite values with None (which converts to null in JSON)
new_data = new_data.replace([np.nan, np.inf, -np.inf], None)

#check before titre sans accent
print(f"Check date after column mapping: {new_data.dateCreation.head()}")
print(f"Check date after column mapping: {new_data.id.head()}")

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
new_data["TitreAnnonceSansAccents"] = new_data["intitule"].apply(
    lambda x: remove_accents_and_special(x) if isinstance(x, str) else x
)

#check after titre sans accent
print(f"Check date after column mapping: {new_data.dateCreation.head()}")
print(f"Check date after column mapping: {new_data.id.head()}")

#check urls partenaire
#print(f"check urls partenaire: {new_data[\'origineOffre.urlOrigine\'].head(15)}")

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
new_data["origineOffre.partenaires"] = new_data["origineOffre.partenaires"].apply(process_partenaire)

#check urls partenaire
#print(f"check urls partenaire: {new_data[\'origineOffre.urlOrigine\'].head(15)}")

#check after indeed filtering
print(f"Check date after column mapping: {new_data.dateCreation.head()}")
print(f"Check date after column mapping: {new_data.id.head()}")

# For rows where the processed value is NaN, None, or an empty string, substitute the fallback from 'origineOffre.urlOrigine'.
new_data["origineOffre.partenaires"] = new_data.apply(
    lambda row: row["origineOffre.partenaires"]
    if pd.notna(row["origineOffre.partenaires"]) and row["origineOffre.partenaires"] not in [None, '',0,"O"]
    else row["origineOffre.urlOrigine"],
    axis=1
)

#check urls partenaire
#print(f"check urls partenaire: {new_data[\'origineOffre.urlOrigine\'].head(15)}")

#last check for out of range json float and convert to json compliant None
def safe_json_value(x):
    if isinstance(x, float):
        # Check for NaN or infinite values
        if np.isnan(x) or np.isinf(x):
            return None
    return x

new_data = new_data.applymap(safe_json_value)

#last check to remove duplicates based on ID
# Normalize the 'id' column: convert everything to string and remove surrounding whitespace
#new_data['id'] = new_data['id'].astype(str).str.strip()
#new_data = new_data.drop_duplicates(subset=['origineOffre.urlOrigine'], keep='first')

# Debug: Print the number of rows to append after filtering
print(f"Total job listings fetched: {new_data.shape[0]}")
print(f"Total columns fetched: {new_data.shape[1]}")
print(new_data.head(5))

#-------------ORDER COLUMNS AND ADD IF NEEDED TO MATCH DESTINATION AND SOURCE-------------------------
# Only pour france Travail
new_data = new_data.rename(columns=lambda c: c.replace(".", "_"))

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

# --- FIX: Get schema from BigQuery metadata instead of using LIMIT 2 ---
table = client.get_table("databasealfred.jobListings.franceTravail")
reference_cols = [field.name for field in table.schema]

print(f"📌 BigQuery schema contains {len(reference_cols)} columns")

# --- 1. Drop columns not in BigQuery schema ---
new_data = new_data[[col for col in new_data.columns if col in reference_cols]]

# --- 2. Add missing columns ---
for col in reference_cols:
    if col not in new_data.columns:
        new_data[col] = None

# --- 3. Reorder to match exact BigQuery order ---
new_data = new_data[reference_cols]

print(f"🆕 new_data after alignment: {new_data.shape[1]} columns")



#---------UPLOAD TO BIGQUERY-------------------------------------------------------------------------------------------------------------

from google.cloud import bigquery
from google.oauth2 import service_account

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

table_id = "databasealfred.jobListings.hellowork"

# CONFIG WITHOUT PYARROW
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Convert DataFrame → list of dict rows (JSON compatible)
rows = new_data.to_dict(orient="records")

# Upload
job = client.load_table_from_json(
    rows,
    table_id,
    job_config=job_config
)

job.result()

print("✅ Data successfully loaded into BigQuery (JSON mode, no PyArrow needed)")
