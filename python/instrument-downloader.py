import requests
import gzip
import json
import io

# URL of the .gz file
url = 'https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz'

# Download the .gz file
response = requests.get(url)
response.raise_for_status()  # Check if the request was successful

# Decompress the .gz file
with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as gz_file:
    # Load JSON data
    data = json.load(gz_file)

# Save the JSON data to a file
with open('NSE.json', 'w', encoding='utf-8') as json_file:
    json.dump(data, json_file, indent=4)

print('File has been downloaded and saved as data.json')