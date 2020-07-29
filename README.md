# steam_web_API_price_getter.py
## A simple script to pull prices from the Steam web API

### Overview
- Steam web API allows to insert a list of commma-separated Steam App IDs into an endpoint to pull pricing (and other) information.
- This script, assuming a SQL database of Steam App IDs, pulls current price data for each app ID in the database, then updates another table with the day's price per app ID.
