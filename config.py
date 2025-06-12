# config.py
"""
Central configuration file for database connections and API endpoints.
Copy this file to config.local.py and edit values for your environment.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

# Database connection strings
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/processed/your_database.db")
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING", "sqlite:///data/processed/your_database.db")

# API endpoints
STOCK_SVC_URL = os.getenv("STOCK_SVC_URL", "https://api.example.com/stock")

# Add other configuration variables as needed 