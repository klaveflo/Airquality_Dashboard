# Import libraries
import asyncio
from pathlib import Path
from airbase.parquet_api import download, Dataset, AggregationType

async def download_data():
    # List of all EU countries and pollutants
    countries = ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE']
    pollutants = ['PM10', 'PM2.5']
    
    # Download verified data (2013-2023) filtered by aggregation type (daily)
    for country in countries:
        for pollutant in pollutants:
            try:
                root_path = Path(f"data/{country}/{pollutant}")
                root_path.mkdir(parents=True, exist_ok=True)
                
                # Download with aggregation type filter (daily data)
                await download(
                    dataset=Dataset.Verified,
                    root_path=root_path,
                    countries={country},
                    pollutants={pollutant},
                    frequency=AggregationType.Daily,  # Filter by aggregation type
                    quiet=False
                )
                print(f"OK Downloaded {pollutant} for {country}")
            except Exception as e:
                print(f"XXX Failed to download {pollutant} for {country}: {e}")

# Run async download
if __name__ == "__main__":
    asyncio.run(download_data())