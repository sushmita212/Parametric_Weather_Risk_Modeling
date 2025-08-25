import os
import requests
from bs4 import BeautifulSoup
import re
import shutil

BASE_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

def download_storm_data(output_dir="data", limit=None):
    """
    Downloads NOAA storm event CSV.gz files, renames them by year and type,
    and organizes them into folders (details, fatalities, locations).
    """
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Scrape filenames from the NOAA directory
    soup = BeautifulSoup(requests.get(BASE_URL).text, "html.parser")
    files = [link.get("href") for link in soup.find_all("a") if link.get("href").endswith(".csv.gz")]
    if limit:
        files = files[:limit]

    for filename in files:
        file_url = BASE_URL + filename
        temp_path = os.path.join(output_dir, filename)

        # Download if not already present
        if not os.path.exists(temp_path):
            print(f"Downloading {filename}...")
            r = requests.get(file_url, stream=True)
            with open(temp_path, "wb") as f:
                for chunk in r.iter_content(8192):  # 8 KB chunks
                    f.write(chunk)

        # Step 2: Extract year and type
        match = re.search(r'd(\d{4})', filename)
        if not match:
            continue
        year = match.group(1)
        if "details" in filename:
            ftype = "details"
        elif "fatalities" in filename:
            ftype = "fatalities"
        elif "locations" in filename:
            ftype = "locations"
        else:
            continue

        # Step 3: Make type folder if needed
        folder_path = os.path.join(output_dir, ftype)
        os.makedirs(folder_path, exist_ok=True)

        # Step 4: Rename and move file
        new_name = f"{ftype}_{year}.csv.gz"
        new_path = os.path.join(folder_path, new_name)
        if not os.path.exists(new_path):
            shutil.move(temp_path, new_path)
        print(f"Saved {new_name} in {folder_path}")
    
    print("All files downloaded and organized!")
