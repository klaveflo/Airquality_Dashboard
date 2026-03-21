# Import libraries
import airbase
import os
import json
import pandas as pd

client = airbase.AirbaseClient()

# Create metadata directory
os.makedirs("metadata", exist_ok=True)

# Download metadata for all EU countries
countries = ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE']

all_metadata = []

for country in countries:
    try:
        print(f"Downloading metadata for {country}...")
        
        # Get country-specific metadata
        metadata = client.get_metadata(country=country)
        
        # Collect all metadata records
        if metadata is not None:
            all_metadata.append(metadata)
        
        print(f"OK Downloaded metadata for {country}")
    except Exception as e:
        print(f"XXX Failed to download metadata for {country}: {e}")

# Combine all metadata into a single DataFrame (if available)
if all_metadata:
    try:
        # Try to concatenate if metadata are DataFrames
        combined_metadata = pd.concat(all_metadata, ignore_index=True)
        
        # Save as CSV and JSON
        combined_metadata.to_csv("metadata/sampling_points_metadata.csv", index=False)
        print(f"OK Saved combined metadata to metadata/sampling_points_metadata.csv")
        
    except Exception as e:
        print(f"XXX Failed to combine and save metadata: {e}")
        
        # Save each country's metadata separately
        for i, country_meta in enumerate(all_metadata):
            try:
                country_meta.to_csv(f"metadata/{countries[i]}_metadata.csv", index=False)
                print(f"OK Saved {countries[i]} metadata to metadata/{countries[i]}_metadata.csv")
            except Exception as e2:
                print(f"XXX Failed to save {countries[i]} metadata: {e2}")
