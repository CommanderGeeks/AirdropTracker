import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
QUICKNODE_RPC_URL = os.getenv("QUICKNODE_RPC_URL")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
YAFFA_TOKEN_MINT = os.getenv("YAFFA_TOKEN_MINT")

# Database Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Crawling Configuration
MAX_CRAWL_DEPTH = 10
BATCH_SIZE = 100
RATE_LIMIT_DELAY = 0.5  # seconds between requests

# Birdeye API endpoints
BIRDEYE_BASE_URL = "https://public-api.birdeye.so"
BIRDEYE_TOKEN_TRADES_ENDPOINT = "/defi/txs/token"
BIRDEYE_PRICE_ENDPOINT = "/defi/price"

def validate_config():
    """Validate that all required configuration is present"""
    required_vars = [
        "QUICKNODE_RPC_URL",
        "BIRDEYE_API_KEY", 
        "YAFFA_TOKEN_MINT",
        "SUPABASE_URL",
        "SUPABASE_KEY"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return True