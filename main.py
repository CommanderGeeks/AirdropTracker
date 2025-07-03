from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import os
from dotenv import load_dotenv

import os
from pathlib import Path

# Import our modules
from database.connection import get_supabase, init_database, create_tables
from crawlers.wallet_crawler import WalletCrawler
from api.endpoints import router as api_router

load_dotenv()

app = FastAPI(title="Yaffa Wallet Tracker", version="1.0.0")

# Initialize database
if not init_database():
    raise Exception("Failed to initialize database connection")

# Create tables
create_tables()

# Static files and templates - make them optional
static_dir = Path("frontend/static")
templates_dir = Path("frontend/templates")

if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

if templates_dir.exists():
    templates = Jinja2Templates(directory=str(templates_dir))
else:
    templates = None

# Include API routes
app.include_router(api_router, prefix="/api")

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard"""
    if templates:
        return templates.TemplateResponse("dashboard.html", {"request": request})
    else:
        return HTMLResponse("""
        <html>
            <head><title>Yaffa Wallet Tracker</title></head>
            <body>
                <h1>Yaffa Wallet Tracker</h1>
                <p>Templates directory not found. API endpoints are available at:</p>
                <ul>
                    <li><a href="/api/summary">/api/summary</a></li>
                    <li><a href="/api/mother-wallets">/api/mother-wallets</a></li>
                    <li><a href="/docs">/docs</a> (API Documentation)</li>
                </ul>
            </body>
        </html>
        """)


@app.post("/api/initialize-mother-wallets")
async def initialize_mother_wallets(
    wallet_addresses: list[str],
    background_tasks: BackgroundTasks,
    supabase = Depends(get_supabase)
):
    """Initialize the 62 mother wallets"""
    try:
        # Add mother wallets to database
        added_count = 0
        for address in wallet_addresses:
            # Check if already exists
            existing = supabase.table('mother_wallets').select('*').eq('address', address).execute()
            if not existing.data:
                result = supabase.table('mother_wallets').insert({
                    'address': address,
                    'label': f'Mother Wallet {added_count + 1}'
                }).execute()
                added_count += 1
        
        # Start crawling in background
        background_tasks.add_task(start_full_crawl, wallet_addresses)
        
        return {
            "message": f"Added {added_count} new mother wallets. Crawling started in background.",
            "total_wallets": len(wallet_addresses)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def start_full_crawl(wallet_addresses: list[str]):
    """Background task to crawl all wallets"""
    crawler = WalletCrawler()
    try:
        await crawler.crawl_all_mother_wallets(wallet_addresses)
    except Exception as e:
        print(f"Crawling error: {e}")
    finally:
        await crawler.close()

@app.get("/api/crawl-status")
async def get_crawl_status(supabase = Depends(get_supabase)):
    """Get current crawling status"""
    try:
        status = supabase.table('crawl_status').select('*').execute()
        
        total_wallets = len(status.data)
        completed = len([s for s in status.data if s['status'] == 'completed'])
        in_progress = len([s for s in status.data if s['status'] == 'crawling'])
        errors = len([s for s in status.data if s['status'] == 'error'])
        
        return {
            "total_wallets": total_wallets,
            "completed": completed,
            "in_progress": in_progress,
            "errors": errors,
            "progress_percentage": (completed / total_wallets * 100) if total_wallets > 0 else 0
        }
    except Exception as e:
        return {
            "total_wallets": 0,
            "completed": 0,
            "in_progress": 0,
            "errors": 0,
            "progress_percentage": 0
        }

@app.get("/api/mother-wallets")
async def get_mother_wallets(supabase = Depends(get_supabase)):
    """Get all mother wallets with summary stats"""
    try:
        mother_wallets = supabase.table('mother_wallets').select('*').execute()
        
        wallet_data = []
        for mw in mother_wallets.data:
            # Calculate stats for this mother wallet
            descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mw['id']).execute()
            
            total_yaffa = sum(w.get('current_yaffa_balance', 0) for w in descendants.data)
            total_sol_profit = sum(w.get('total_sol_received', 0) for w in descendants.data)
            
            wallet_data.append({
                "id": mw['id'],
                "address": mw['address'],
                "label": mw.get('label'),
                "total_descendants": len(descendants.data),
                "total_yaffa_current": total_yaffa,
                "total_sol_profit": total_sol_profit,
                "created_at": mw.get('created_at')
            })
        
        return wallet_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/mother-wallet/{wallet_id}/tree")
async def get_wallet_tree(wallet_id: int, supabase = Depends(get_supabase)):
    """Get the complete wallet tree for a mother wallet"""
    try:
        mother_wallet = supabase.table('mother_wallets').select('*').eq('id', wallet_id).execute()
        if not mother_wallet.data:
            raise HTTPException(status_code=404, detail="Mother wallet not found")
        
        # Get all descendants
        descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', wallet_id).execute()
        
        # Build tree structure
        def build_tree_node(wallet):
            children = [w for w in descendants.data if w.get('parent_wallet_id') == wallet['id']]
            return {
                "id": wallet['id'],
                "address": wallet['address'],
                "current_yaffa_balance": wallet.get('current_yaffa_balance', 0),
                "total_sol_received": wallet.get('total_sol_received', 0),
                "generation": wallet.get('generation', 0),
                "children": [build_tree_node(child) for child in children]
            }
        
        # Start with mother wallet (generation 0)
        root_wallets = [w for w in descendants.data if w.get('generation') == 0]
        
        tree_data = {
            "mother_wallet": {
                "id": mother_wallet.data[0]['id'],
                "address": mother_wallet.data[0]['address'],
                "label": mother_wallet.data[0].get('label')
            },
            "tree": [build_tree_node(wallet) for wallet in root_wallets]
        }
        
        return tree_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Yaffa Wallet Tracker...")
    print("📊 Database tables ready!")
    print("🌐 Open your browser to: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)