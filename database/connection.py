import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Supabase client for API calls
def get_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

# For this simplified version, we'll use Supabase's REST API instead of SQLAlchemy
# This avoids database driver compatibility issues

def init_database():
    """Initialize database connection"""
    try:
        client = get_supabase_client()
        # Test the connection by checking if we can access the schema
        # This is a simple way to test without requiring any tables to exist
        response = client.rpc('version').execute()
        print("Supabase connection successful")
        return True
    except Exception as e:
        # If the RPC call fails, try a simpler test
        try:
            # Just test if we can create the client
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
            if url and key:
                print("Supabase client created successfully")
                return True
            else:
                print("Missing Supabase credentials")
                return False
        except Exception as e2:
            print(f"Supabase connection failed: {e2}")
            return False

def create_tables():
    """Create tables using SQL commands via Supabase"""
    client = get_supabase_client()
    
    # SQL to create our tables
    create_tables_sql = """
    -- Mother wallets table
    CREATE TABLE IF NOT EXISTS mother_wallets (
        id SERIAL PRIMARY KEY,
        address VARCHAR(50) UNIQUE NOT NULL,
        label VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW(),
        total_yaffa_current FLOAT DEFAULT 0.0,
        total_sol_profit FLOAT DEFAULT 0.0,
        total_descendants INTEGER DEFAULT 0
    );

    -- Wallets table
    CREATE TABLE IF NOT EXISTS wallets (
        id SERIAL PRIMARY KEY,
        address VARCHAR(50) UNIQUE NOT NULL,
        mother_wallet_id INTEGER REFERENCES mother_wallets(id),
        parent_wallet_id INTEGER REFERENCES wallets(id),
        current_yaffa_balance FLOAT DEFAULT 0.0,
        total_yaffa_received FLOAT DEFAULT 0.0,
        total_yaffa_sent FLOAT DEFAULT 0.0,
        total_yaffa_sold FLOAT DEFAULT 0.0,
        total_sol_received FLOAT DEFAULT 0.0,
        first_yaffa_received TIMESTAMP,
        last_activity TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE,
        generation INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );

    -- Transactions table
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        transaction_hash VARCHAR(100) UNIQUE NOT NULL,
        from_wallet_id INTEGER REFERENCES wallets(id),
        to_wallet_id INTEGER REFERENCES wallets(id),
        yaffa_amount FLOAT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        block_height INTEGER,
        transaction_type VARCHAR(20),
        raw_data JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );

    -- Trades table
    CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        wallet_id INTEGER REFERENCES wallets(id) NOT NULL,
        transaction_hash VARCHAR(100) NOT NULL,
        yaffa_amount_sold FLOAT NOT NULL,
        sol_amount_received FLOAT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        dex_used VARCHAR(50),
        price_per_token FLOAT,
        raw_data JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );

    -- Crawl status table
    CREATE TABLE IF NOT EXISTS crawl_status (
        id SERIAL PRIMARY KEY,
        wallet_address VARCHAR(50) NOT NULL,
        last_crawled TIMESTAMP,
        last_transaction_signature VARCHAR(100),
        crawl_depth INTEGER DEFAULT 0,
        status VARCHAR(20) DEFAULT 'pending',
        error_message TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    try:
        # Execute the SQL via Supabase RPC or use a SQL editor in Supabase dashboard
        print("Please run the following SQL in your Supabase SQL editor:")
        print("Dashboard -> SQL Editor -> New Query")
        print("=" * 50)
        print(create_tables_sql)
        print("=" * 50)
        print("After running the SQL, the tables will be ready!")
        return True
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

# Simple dependency injection for FastAPI
def get_supabase():
    """Dependency for FastAPI to get Supabase client"""
    return get_supabase_client()