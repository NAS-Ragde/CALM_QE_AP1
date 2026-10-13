import requests
import csv
import os
import sys
import time
import logging
import json
import gc
from urllib.parse import quote
from utils import setup_logging, load_config, setup_auth, save_csv, CsvStreamWriter

# Setup Logging
setup_logging("download.log")

# Load Configuration
config = load_config()
FHIR_BASE_URL = config["fhir"]["base_url"]
OUTPUT_DIR = config["analysis"]["output_dir"]
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Setup Authentication
AUTH, HEADERS = setup_auth(config)

def load_icd_codes(filepath, fallback):
    """Load ICD codes from a JSON file (supporting Erlangen format or flat list)."""
    if filepath and os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "codes" in data:
                return [item["code"] for item in data["codes"] if "code" in item]
            elif isinstance(data, dict):
                codes = []
                for k in ["asthma", "copd", "exacerbation"]:
                    if k in data:
                        codes.extend(data[k])
                if codes:
                    return codes
            elif isinstance(data, list):
                return data
            logging.warning(f"Could not parse structure of {filepath}. Using fallback.")
        except Exception as e:
            logging.error(f"Error reading {filepath}: {e}. Using fallback.")
    return fallback

def load_loinc_codes(filepath, fallback):
    """Load LOINC codes from a JSON file."""
    if filepath and os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "codes" in data:
                return [item["code"] for item in data["codes"] if "code" in item]
            elif isinstance(data, list):
                return data
            logging.warning(f"Could not parse structure of {filepath}. Using fallback.")
        except Exception as e:
            logging.error(f"Error reading {filepath}: {e}. Using fallback.")
    return fallback

# Load codes
asthma_copd_file = config["analysis"].get("asthma_copd_codes_file")
loinc_file = config["analysis"].get("loinc_codes_file")

condition_codes = load_icd_codes(asthma_copd_file, config["analysis"].get("condition_codes", []))
observation_codes = load_loinc_codes(loinc_file, config["analysis"].get("observation_codes", []))

# Resources to download for the analysis
RESOURCES = {
    "Condition":         condition_codes,
    "Observation":       observation_codes,
    "MedicationRequest": None,
    "Medication":        None,
}


def fetch_and_write_pages(url, writer):
    """Follow FHIR pagination and stream resources directly to the writer."""
    page = 1
    while url:
        logging.info(f"    Page {page}...")
        try:
            resp = requests.get(url, auth=AUTH, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            bundle = resp.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error: {e}")
            logging.info(f"Waiting 10 seconds and retrying page {page}...")
            time.sleep(10)
            continue

        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                writer.write_resource(resource)

        url = next((link.get("url") for link in bundle.get("link", []) if link.get("relation") == "next"), None)
        if url:
            page += 1

def fetch_all_pages(url):
    """Follow FHIR pagination and collect all resources in memory."""
    resources = []
    page = 1
    while url:
        logging.info(f"    Page {page}...")
        try:
            resp = requests.get(url, auth=AUTH, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            bundle = resp.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error: {e}")
            logging.info(f"Waiting 10 seconds and retrying page {page}...")
            time.sleep(10)
            continue

        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                resources.append(resource)

        url = next((link.get("url") for link in bundle.get("link", []) if link.get("relation") == "next"), None)
        if url:
            page += 1

    return resources

def extract_patient_ids_from_conditions():
    """Extract unique Patient IDs from condition.csv."""
    condition_csv = os.path.join(OUTPUT_DIR, "condition.csv")
    if not os.path.exists(condition_csv):
        return []

    patient_ids = set()
    with open(condition_csv, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ref = row.get("(1) subject.reference", "")
            if ref:
                pid = ref.replace("Patient/", "").strip()
                if pid:
                    patient_ids.add(pid)

    patient_ids = sorted(list(patient_ids))
    logging.info(f"Extracted {len(patient_ids)} unique cohort patient IDs from condition.csv")
    return patient_ids

def fetch_resources(resource_type, filepath=None, code_list=None, patient_ids=None):
    """Fetch resources, batching by code list and/or patient IDs."""
    writer = CsvStreamWriter(filepath) if filepath else None
    in_memory = [] if writer is None else None

    # If patient_ids are provided, filter queries by cohort patients
    if patient_ids:
        PATIENT_BATCH_SIZE = 50
        patient_chunks = [patient_ids[i:i + PATIENT_BATCH_SIZE] for i in range(0, len(patient_ids), PATIENT_BATCH_SIZE)]

        logging.info(f"Fetching {resource_type} for {len(patient_ids)} cohort patient(s) in {len(patient_chunks)} patient batch(es)...")

        for p_idx, p_chunk in enumerate(patient_chunks, 1):
            patient_str = ",".join(p_chunk)

            if code_list:
                CODE_BATCH_SIZE = 50
                code_chunks = [code_list[i:i + CODE_BATCH_SIZE] for i in range(0, len(code_list), CODE_BATCH_SIZE)]
                for c_idx, c_chunk in enumerate(code_chunks, 1):
                    code_str = ",".join(c_chunk)
                    url = f"{FHIR_BASE_URL}/{resource_type}?patient={patient_str}&code={code_str}&_count=200"
                    logging.info(f"  Patient Batch {p_idx}/{len(patient_chunks)} | Code Batch {c_idx}/{len(code_chunks)} ...")
                    if writer:
                        fetch_and_write_pages(url, writer)
                    else:
                        in_memory.extend(fetch_all_pages(url))
            else:
                url = f"{FHIR_BASE_URL}/{resource_type}?patient={patient_str}&_count=200"
                logging.info(f"  Patient Batch {p_idx}/{len(patient_chunks)} ...")
                if writer:
                    fetch_and_write_pages(url, writer)
                else:
                    in_memory.extend(fetch_all_pages(url))

            gc.collect()

        if writer:
            writer.close()
            return []
        logging.info(f"Loaded {len(in_memory)} {resource_type} entries total for cohort patients")
        return in_memory

    # Fallback if no patient_ids provided
    if not code_list:
        url = f"{FHIR_BASE_URL}/{resource_type}?_count=200"
        logging.info(f"Fetching {resource_type} (no code/patient filter) ...")
        if writer:
            fetch_and_write_pages(url, writer)
            writer.close()
            return []
        return fetch_all_pages(url)

    BATCH_SIZE = 50
    chunks = [code_list[i:i + BATCH_SIZE] for i in range(0, len(code_list), BATCH_SIZE)]

    logging.info(f"Fetching {resource_type} in {len(chunks)} batch(es) of max {BATCH_SIZE} codes...")
    for idx, chunk in enumerate(chunks, 1):
        code_str = ",".join(chunk)
        url = f"{FHIR_BASE_URL}/{resource_type}?code={code_str}&_count=200"
        logging.info(f"  Batch {idx}/{len(chunks)} ...")
        if writer:
            fetch_and_write_pages(url, writer)
        else:
            in_memory.extend(fetch_all_pages(url))

    if writer:
        writer.close()
        return []
    logging.info(f"Loaded {len(in_memory)} {resource_type} entries total")
    return in_memory

def fetch_encounters_from_condition_csv():
    """Extract Encounter IDs or Identifiers from condition.csv and stream them to CSV."""
    encounter_csv = os.path.join(OUTPUT_DIR, "encounter.csv")
    condition_csv = os.path.join(OUTPUT_DIR, "condition.csv")

    if os.path.exists(encounter_csv):
        logging.info(f"Skipping Encounter (File already exists: {encounter_csv})")
        return

    if not os.path.exists(condition_csv):
        logging.warning("condition.csv not found – Encounter download skipped.")
        return

    logging.info("Loading Encounters (via condition.csv)...")

    encounter_ids = set()
    encounter_identifiers = set()
    with open(condition_csv, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ref = row.get("(1) encounter.reference", "")
            if ref and ref.startswith("Encounter/"):
                encounter_ids.add(ref.replace("Encounter/", "").strip())
            identifier = row.get("(1) encounter.identifier.value", "")
            if identifier:
                encounter_identifiers.add(identifier.strip())

    encounter_ids = list(encounter_ids)
    encounter_identifiers = list(encounter_identifiers)

    if encounter_ids:
        logging.info(f"Downloading encounters via {len(encounter_ids)} Encounter IDs in batches...")
        BATCH_SIZE = 100
        batches = [encounter_ids[i:i + BATCH_SIZE] for i in range(0, len(encounter_ids), BATCH_SIZE)]
        with CsvStreamWriter(encounter_csv) as writer:
            for idx, batch in enumerate(batches, 1):
                url = f"{FHIR_BASE_URL}/Encounter?_id={','.join(batch)}&_count={BATCH_SIZE}"
                logging.info(f"Batch {idx}/{len(batches)}...")
                fetch_and_write_pages(url, writer)
    elif encounter_identifiers:
        logging.info(f"Encounter IDs not found. Fallback: Downloading via {len(encounter_identifiers)} Encounter Identifiers...")
        BATCH_SIZE = 100
        with CsvStreamWriter(encounter_csv) as writer:
            for idx, identifier in enumerate(encounter_identifiers, 1):
                encoded = quote(identifier, safe="")
                url = f"{FHIR_BASE_URL}/Encounter?identifier={encoded}&_count={BATCH_SIZE}"
                if idx % 50 == 0 or idx == len(encounter_identifiers):
                    logging.info(f"Identifier {idx}/{len(encounter_identifiers)}...")
                fetch_and_write_pages(url, writer)
    else:
        logging.warning("No Encounter references or identifiers found in condition.csv.")
        return

def main():
    logging.info("=" * 60)
    logging.info("FHIR Download – Distributed Analysis")
    logging.info(f"Server: {FHIR_BASE_URL}")
    logging.info(f"Target Directory: {OUTPUT_DIR}/")
    logging.info("=" * 60)

    # 1. Download Condition first to establish cohort
    condition_csv = os.path.join(OUTPUT_DIR, "condition.csv")
    if not os.path.exists(condition_csv):
        logging.info("Loading Condition (establishing cohort)...")
        fetch_resources("Condition", filepath=condition_csv, code_list=condition_codes)
    else:
        logging.info("Skipping Condition (File already exists: condition.csv)")

    # Extract Patient IDs from Condition cohort
    patient_ids = extract_patient_ids_from_conditions()

    # 2. Download remaining resources filtered by cohort patient_ids
    for resource_type in ["Observation", "MedicationRequest", "Medication"]:
        csv_name = f"{resource_type[0].lower()}{resource_type[1:]}.csv"
        filepath = os.path.join(OUTPUT_DIR, csv_name)

        if os.path.exists(filepath):
            logging.info(f"Skipping {resource_type} (File already exists: {csv_name})")
            continue

        logging.info(f"Loading {resource_type}...")
        codes = RESOURCES.get(resource_type)
        
        # Filter Observation and MedicationRequest by cohort patient_ids
        if resource_type in ["Observation", "MedicationRequest"] and patient_ids:
            fetch_resources(resource_type, filepath=filepath, code_list=codes, patient_ids=patient_ids)
        else:
            fetch_resources(resource_type, filepath=filepath, code_list=codes)

    # 3. Download Encounters
    fetch_encounters_from_condition_csv()

    logging.info("=" * 60)
    logging.info(f"Download complete! CSVs saved in: {OUTPUT_DIR}")
    logging.info("Next step: Execute analyze_fhir_distributed.py")
    logging.info("=" * 60)

if __name__ == "__main__":
    main()

