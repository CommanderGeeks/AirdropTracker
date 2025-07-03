from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from database.connection import get_supabase
from crawlers.wallet_crawler import WalletCrawler
from typing import List, Dict
from datetime import datetime
import json

router = APIRouter()

@router.post("/start-crawl")
async def start_crawl(
    wallet_addresses: List[str],
    background_tasks: BackgroundTasks,
    supabase = Depends(get_supabase)
):
    """Start crawling the provided wallet addresses"""
    try:
        # Initialize mother wallets in database
        added_count = 0
        for address in wallet_addresses:
            existing = supabase.table('mother_wallets').select('*').eq('address', address).execute()
            if not existing.data:
                supabase.table('mother_wallets').insert({
                    'address': address,
                    'label': f'Mother Wallet {added_count + 1}'
                }).execute()
                added_count += 1
        
        # Start background crawling
        background_tasks.add_task(run_crawler, wallet_addresses)
        
        return {
            "message": f"Crawling started for {len(wallet_addresses)} wallets ({added_count} new)",
            "status": "started"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def run_crawler(wallet_addresses: List[str]):
    """Background task to run the crawler"""
    crawler = WalletCrawler()
    try:
        await crawler.crawl_all_mother_wallets(wallet_addresses)
    finally:
        await crawler.close()

@router.get("/summary")
async def get_summary(supabase = Depends(get_supabase)):
    """Get overall summary statistics"""
    try:
        # Mother wallet stats
        mother_wallets = supabase.table('mother_wallets').select('*').execute()
        mother_wallet_count = len(mother_wallets.data)
        
        # Total wallet stats
        all_wallets = supabase.table('wallets').select('current_yaffa_balance, total_yaffa_sold, total_sol_received').execute()
        total_wallets = len(all_wallets.data)
        active_wallets = len([w for w in all_wallets.data if w.get('current_yaffa_balance', 0) > 0])
        
        # Token stats
        total_yaffa_held = sum(w.get('current_yaffa_balance', 0) for w in all_wallets.data)
        total_yaffa_sold = sum(w.get('total_yaffa_sold', 0) for w in all_wallets.data)
        total_sol_profit = sum(w.get('total_sol_received', 0) for w in all_wallets.data)
        
        # Transaction stats
        transactions = supabase.table('transactions').select('id').execute()
        trades = supabase.table('trades').select('id').execute()
        
        return {
            "mother_wallets": mother_wallet_count,
            "total_wallets": total_wallets,
            "active_wallets": active_wallets,
            "total_yaffa_held": round(total_yaffa_held, 2),
            "total_yaffa_sold": round(total_yaffa_sold, 2),
            "total_sol_profit": round(total_sol_profit, 4),
            "total_transactions": len(transactions.data),
            "total_trades": len(trades.data)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/mother-wallets")
async def get_mother_wallets_detailed(supabase = Depends(get_supabase)):
    """Get detailed information about all mother wallets"""
    try:
        mother_wallets = supabase.table('mother_wallets').select('*').execute()
        result = []
        
        for mw in mother_wallets.data:
            # Get all descendants
            descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mw['id']).execute()
            
            # Calculate aggregated stats
            total_yaffa_current = sum(w.get('current_yaffa_balance', 0) for w in descendants.data)
            total_yaffa_sold = sum(w.get('total_yaffa_sold', 0) for w in descendants.data)
            total_sol_profit = sum(w.get('total_sol_received', 0) for w in descendants.data)
            
            # Count generations
            max_generation = max((w.get('generation', 0) for w in descendants.data), default=0)
            
            # Active descendants (with balance > 0)
            active_descendants = len([w for w in descendants.data if w.get('current_yaffa_balance', 0) > 0])
            
            result.append({
                "id": mw['id'],
                "address": mw['address'],
                "label": mw.get('label'),
                "total_descendants": len(descendants.data),
                "active_descendants": active_descendants,
                "max_generation": max_generation,
                "total_yaffa_current": round(total_yaffa_current, 2),
                "total_yaffa_sold": round(total_yaffa_sold, 2),
                "total_sol_profit": round(total_sol_profit, 4),
                "created_at": mw.get('created_at')
            })
        
        # Sort by total SOL profit descending
        result.sort(key=lambda x: x["total_sol_profit"], reverse=True)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/mother-wallet/{mother_id}/descendants")
async def get_mother_wallet_descendants(mother_id: int, supabase = Depends(get_supabase)):
    """Get all descendants of a mother wallet with detailed stats"""
    try:
        mother_wallet = supabase.table('mother_wallets').select('*').eq('id', mother_id).execute()
        if not mother_wallet.data:
            raise HTTPException(status_code=404, detail="Mother wallet not found")
        
        descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mother_id).execute()
        
        result = []
        for wallet in descendants.data:
            # Get trading history
            trades = supabase.table('trades').select('*').eq('wallet_id', wallet['id']).execute()
            
            # Get children count
            children = supabase.table('wallets').select('id').eq('parent_wallet_id', wallet['id']).execute()
            children_count = len(children.data)
            
            result.append({
                "id": wallet['id'],
                "address": wallet['address'],
                "generation": wallet.get('generation', 0),
                "current_yaffa_balance": round(wallet.get('current_yaffa_balance', 0), 2),
                "total_yaffa_received": round(wallet.get('total_yaffa_received', 0), 2),
                "total_yaffa_sent": round(wallet.get('total_yaffa_sent', 0), 2),
                "total_yaffa_sold": round(wallet.get('total_yaffa_sold', 0), 2),
                "total_sol_received": round(wallet.get('total_sol_received', 0), 4),
                "children_count": children_count,
                "trade_count": len(trades.data),
                "first_yaffa_received": wallet.get('first_yaffa_received'),
                "last_activity": wallet.get('last_activity'),
                "is_active": wallet.get('current_yaffa_balance', 0) > 0
            })
        
        # Sort by generation, then by balance
        result.sort(key=lambda x: (x["generation"], -x["current_yaffa_balance"]))
        
        return {
            "mother_wallet": {
                "id": mother_wallet.data[0]['id'],
                "address": mother_wallet.data[0]['address'],
                "label": mother_wallet.data[0].get('label')
            },
            "descendants": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/wallet/{wallet_id}/transactions")
async def get_wallet_transactions(wallet_id: int, supabase = Depends(get_supabase)):
    """Get all transactions for a specific wallet"""
    try:
        wallet = supabase.table('wallets').select('*').eq('id', wallet_id).execute()
        if not wallet.data:
            raise HTTPException(status_code=404, detail="Wallet not found")
        
        wallet_data = wallet.data[0]
        
        # Get all transactions (sent and received)
        sent_txs = supabase.table('transactions').select('*, to_wallet:wallets!transactions_to_wallet_id_fkey(address)').eq('from_wallet_id', wallet_id).execute()
        received_txs = supabase.table('transactions').select('*, from_wallet:wallets!transactions_from_wallet_id_fkey(address)').eq('to_wallet_id', wallet_id).execute()
        
        # Get all trades
        trades = supabase.table('trades').select('*').eq('wallet_id', wallet_id).execute()
        
        transactions = []
        
        # Add sent transactions
        for tx in sent_txs.data:
            transactions.append({
                "type": "transfer_out",
                "transaction_hash": tx['transaction_hash'],
                "amount": tx['yaffa_amount'],
                "timestamp": tx['timestamp'],
                "to_address": tx.get('to_wallet', {}).get('address', 'Unknown'),
                "block_height": tx.get('block_height')
            })
        
        # Add received transactions
        for tx in received_txs.data:
            transactions.append({
                "type": "transfer_in",
                "transaction_hash": tx['transaction_hash'],
                "amount": tx['yaffa_amount'],
                "timestamp": tx['timestamp'],
                "from_address": tx.get('from_wallet', {}).get('address', 'Unknown'),
                "block_height": tx.get('block_height')
            })
        
        # Add trades
        for trade in trades.data:
            transactions.append({
                "type": "trade_sell",
                "transaction_hash": trade['transaction_hash'],
                "yaffa_amount": trade['yaffa_amount_sold'],
                "sol_amount": trade['sol_amount_received'],
                "price_per_token": trade.get('price_per_token'),
                "dex_used": trade.get('dex_used'),
                "timestamp": trade['timestamp']
            })
        
        # Sort by timestamp descending
        transactions.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return {
            "wallet": {
                "id": wallet_data['id'],
                "address": wallet_data['address'],
                "current_balance": wallet_data.get('current_yaffa_balance', 0)
            },
            "transactions": transactions
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export/{mother_id}")
async def export_mother_wallet_data(mother_id: int, supabase = Depends(get_supabase)):
    """Export all data for a mother wallet as JSON"""
    try:
        mother_wallet = supabase.table('mother_wallets').select('*').eq('id', mother_id).execute()
        if not mother_wallet.data:
            raise HTTPException(status_code=404, detail="Mother wallet not found")
        
        mother_data = mother_wallet.data[0]
        descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mother_id).execute()
        
        export_data = {
            "mother_wallet": {
                "address": mother_data['address'],
                "label": mother_data.get('label'),
                "export_timestamp": datetime.utcnow().isoformat()
            },
            "summary": {
                "total_descendants": len(descendants.data),
                "active_descendants": len([w for w in descendants.data if w.get('current_yaffa_balance', 0) > 0]),
                "total_yaffa_held": sum(w.get('current_yaffa_balance', 0) for w in descendants.data),
                "total_yaffa_sold": sum(w.get('total_yaffa_sold', 0) for w in descendants.data),
                "total_sol_profit": sum(w.get('total_sol_received', 0) for w in descendants.data)
            },
            "wallets": []
        }
        
        for wallet in descendants.data:
            trades = supabase.table('trades').select('*').eq('wallet_id', wallet['id']).execute()
            
            wallet_data = {
                "address": wallet['address'],
                "generation": wallet.get('generation', 0),
                "current_yaffa_balance": wallet.get('current_yaffa_balance', 0),
                "total_yaffa_received": wallet.get('total_yaffa_received', 0),
                "total_yaffa_sent": wallet.get('total_yaffa_sent', 0),
                "total_yaffa_sold": wallet.get('total_yaffa_sold', 0),
                "total_sol_received": wallet.get('total_sol_received', 0),
                "trade_count": len(trades.data),
                "first_received": wallet.get('first_yaffa_received'),
                "last_activity": wallet.get('last_activity'),
                "trades": [
                    {
                        "yaffa_sold": trade['yaffa_amount_sold'],
                        "sol_received": trade['sol_amount_received'],
                        "price_per_token": trade.get('price_per_token'),
                        "dex": trade.get('dex_used'),
                        "timestamp": trade['timestamp']
                    }
                    for trade in trades.data
                ]
            }
            
            export_data["wallets"].append(wallet_data)
        
        return export_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add this to api/endpoints.py

@router.post("/start-crawl")
async def start_crawl_endpoint(
    wallet_addresses: List[str],
    background_tasks: BackgroundTasks,
    supabase = Depends(get_supabase)
):
    """Start crawling - frontend expects this endpoint name"""
    # This is just an alias to the existing initialize-mother-wallets functionality
    try:
        # Initialize mother wallets in database
        added_count = 0
        for address in wallet_addresses:
            existing = supabase.table('mother_wallets').select('*').eq('address', address).execute()
            if not existing.data:
                supabase.table('mother_wallets').insert({
                    'address': address,
                    'label': f'Mother Wallet {added_count + 1}'
                }).execute()
                added_count += 1
        
        # Start background crawling
        background_tasks.add_task(run_crawler, wallet_addresses)
        
        return {
            "message": f"Crawling started for {len(wallet_addresses)} wallets ({added_count} new)",
            "status": "started"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))