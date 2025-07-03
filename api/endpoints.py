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
    """Get overall summary statistics with enhanced metrics"""
    try:
        # Mother wallet stats
        mother_wallets = supabase.table('mother_wallets').select('*').execute()
        mother_wallet_count = len(mother_wallets.data)
        
        # Total wallet stats with new fields
        all_wallets = supabase.table('wallets').select(
            'current_yaffa_balance, total_yaffa_sold, total_sol_received, '
            'total_yaffa_bought, total_sol_spent, net_yaffa_balance, '
            'net_sol_balance, is_external, lineage_count'
        ).execute()
        
        total_wallets = len(all_wallets.data)
        active_wallets = len([w for w in all_wallets.data if w.get('current_yaffa_balance', 0) > 0])
        external_wallets = len([w for w in all_wallets.data if w.get('is_external', False)])
        multi_lineage_wallets = len([w for w in all_wallets.data if w.get('lineage_count', 1) > 1])
        
        # Enhanced token stats
        total_yaffa_held = sum(w.get('current_yaffa_balance', 0) for w in all_wallets.data)
        total_yaffa_sold = sum(w.get('total_yaffa_sold', 0) for w in all_wallets.data)
        total_yaffa_bought = sum(w.get('total_yaffa_bought', 0) for w in all_wallets.data)
        total_sol_profit = sum(w.get('total_sol_received', 0) for w in all_wallets.data)
        total_sol_spent = sum(w.get('total_sol_spent', 0) for w in all_wallets.data)
        net_sol_profit = sum(w.get('net_sol_balance', 0) for w in all_wallets.data)
        
        # Transaction and trade stats
        transactions = supabase.table('transactions').select('id, is_lineage_transfer').execute()
        lineage_transactions = len([t for t in transactions.data if t.get('is_lineage_transfer', False)])
        
        trades = supabase.table('trades').select('id, trade_type').execute()
        buy_trades = len([t for t in trades.data if t.get('trade_type') == 'buy'])
        sell_trades = len([t for t in trades.data if t.get('trade_type') == 'sell'])
        
        # Crawl status
        crawl_status = supabase.table('crawl_status').select('status').execute()
        completed_crawls = len([c for c in crawl_status.data if c.get('status') == 'completed'])
        
        return {
            "mother_wallets": mother_wallet_count,
            "total_wallets": total_wallets,
            "active_wallets": active_wallets,
            "external_wallets": external_wallets,
            "multi_lineage_wallets": multi_lineage_wallets,
            "total_yaffa_held": round(total_yaffa_held, 2),
            "total_yaffa_sold": round(total_yaffa_sold, 2),
            "total_yaffa_bought": round(total_yaffa_bought, 2),
            "total_sol_profit": round(total_sol_profit, 4),
            "total_sol_spent": round(total_sol_spent, 4),
            "net_sol_profit": round(net_sol_profit, 4),
            "total_transactions": len(transactions.data),
            "lineage_transactions": lineage_transactions,
            "external_transactions": len(transactions.data) - lineage_transactions,
            "total_trades": len(trades.data),
            "buy_trades": buy_trades,
            "sell_trades": sell_trades,
            "completed_crawls": completed_crawls,
            "crawl_coverage": round((completed_crawls / mother_wallet_count * 100), 1) if mother_wallet_count > 0 else 0
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/mother-wallets")
async def get_mother_wallets_detailed(supabase = Depends(get_supabase)):
    """Get detailed information about all mother wallets with enhanced metrics"""
    try:
        mother_wallets = supabase.table('mother_wallets').select('*').execute()
        result = []
        
        for mw in mother_wallets.data:
            # Use the database function for efficient aggregation
            stats = supabase.rpc('get_wallet_tree_stats', {'mother_wallet_id_param': mw['id']}).execute()
            
            if stats.data and len(stats.data) > 0:
                stat = stats.data[0]
                total_wallets = stat['total_wallets']
                active_wallets = stat['active_wallets']
                max_generation = stat['max_generation']
                total_yaffa_held = stat['total_yaffa_held']
                total_yaffa_sold = stat['total_yaffa_sold']
                total_sol_profit = stat['total_sol_profit']
                total_trades = stat['total_trades']
            else:
                # Fallback to individual queries
                descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mw['id']).execute()
                total_wallets = len(descendants.data)
                active_wallets = len([w for w in descendants.data if w.get('current_yaffa_balance', 0) > 0])
                max_generation = max((w.get('generation', 0) for w in descendants.data), default=0)
                total_yaffa_held = sum(w.get('current_yaffa_balance', 0) for w in descendants.data)
                total_yaffa_sold = sum(w.get('total_yaffa_sold', 0) for w in descendants.data)
                total_sol_profit = sum(w.get('total_sol_received', 0) for w in descendants.data)
                
                # Get trade count
                trade_count_query = supabase.table('trades').select('id', count='exact').execute()
                total_trades = trade_count_query.count if hasattr(trade_count_query, 'count') else 0
            
            # Get multi-lineage connections
            lineage_connections = supabase.table('wallet_lineages').select('wallet_id').eq('mother_wallet_id', mw['id']).execute()
            multi_lineage_connections = len(lineage_connections.data)
            
            # Get external wallet discoveries
            external_discoveries = supabase.table('wallets').select('id').eq('discovered_by_mother', mw['id']).eq('is_external', True).execute()
            external_wallet_count = len(external_discoveries.data)
            
            # Calculate efficiency metrics
            profit_per_wallet = total_sol_profit / total_wallets if total_wallets > 0 else 0
            active_wallet_ratio = (active_wallets / total_wallets * 100) if total_wallets > 0 else 0
            
            result.append({
                "id": mw['id'],
                "address": mw['address'],
                "label": mw.get('label'),
                "total_descendants": total_wallets,
                "active_descendants": active_wallets,
                "external_wallets": external_wallet_count,
                "multi_lineage_connections": multi_lineage_connections,
                "max_generation": max_generation,
                "generation_depth": max_generation + 1,
                "total_yaffa_current": round(float(total_yaffa_held), 2),
                "total_yaffa_sold": round(float(total_yaffa_sold), 2),
                "total_sol_profit": round(float(total_sol_profit), 4),
                "total_trades": total_trades,
                "profit_per_wallet": round(profit_per_wallet, 6),
                "active_wallet_ratio": round(active_wallet_ratio, 1),
                "created_at": mw.get('created_at')
            })
        
        # Sort by total SOL profit descending
        result.sort(key=lambda x: x["total_sol_profit"], reverse=True)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/mother-wallet/{mother_id}/descendants")
async def get_mother_wallet_descendants(mother_id: int, supabase = Depends(get_supabase)):
    """Get all descendants of a mother wallet with enhanced stats"""
    try:
        mother_wallet = supabase.table('mother_wallets').select('*').eq('id', mother_id).execute()
        if not mother_wallet.data:
            raise HTTPException(status_code=404, detail="Mother wallet not found")
        
        descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mother_id).execute()
        
        result = []
        for wallet in descendants.data:
            # Get trading history with enhanced metrics
            trades = supabase.table('trades').select('*').eq('wallet_id', wallet['id']).execute()
            buy_trades = [t for t in trades.data if t.get('trade_type') == 'buy']
            sell_trades = [t for t in trades.data if t.get('trade_type') == 'sell']
            
            # Get children count
            children = supabase.table('wallets').select('id').eq('parent_wallet_id', wallet['id']).execute()
            children_count = len(children.data)
            
            # Get lineage connections
            lineage_connections = supabase.table('wallet_lineages').select('mother_wallet_id').eq('wallet_id', wallet['id']).execute()
            connected_lineages = len(lineage_connections.data)
            
            # Calculate performance metrics
            total_bought = wallet.get('total_yaffa_bought', 0)
            total_sold = wallet.get('total_yaffa_sold', 0)
            net_yaffa = wallet.get('net_yaffa_balance', 0)
            net_sol = wallet.get('net_sol_balance', 0)
            
            # Trading efficiency
            avg_buy_price = 0
            avg_sell_price = 0
            if buy_trades:
                total_sol_spent = sum(t.get('sol_amount_spent', 0) for t in buy_trades)
                total_yaffa_bought = sum(t.get('yaffa_amount_bought', 0) for t in buy_trades)
                avg_buy_price = total_sol_spent / total_yaffa_bought if total_yaffa_bought > 0 else 0
            
            if sell_trades:
                total_sol_received = sum(t.get('sol_amount_received', 0) for t in sell_trades)
                total_yaffa_sold = sum(t.get('yaffa_amount_sold', 0) for t in sell_trades)
                avg_sell_price = total_sol_received / total_yaffa_sold if total_yaffa_sold > 0 else 0
            
            result.append({
                "id": wallet['id'],
                "address": wallet['address'],
                "generation": wallet.get('generation', 0),
                "is_external": wallet.get('is_external', False),
                "current_yaffa_balance": round(wallet.get('current_yaffa_balance', 0), 2),
                "total_yaffa_received": round(wallet.get('total_yaffa_received', 0), 2),
                "total_yaffa_sent": round(wallet.get('total_yaffa_sent', 0), 2),
                "total_yaffa_bought": round(total_bought, 2),
                "total_yaffa_sold": round(total_sold, 2),
                "net_yaffa_balance": round(net_yaffa, 2),
                "total_sol_received": round(wallet.get('total_sol_received', 0), 4),
                "total_sol_spent": round(wallet.get('total_sol_spent', 0), 4),
                "net_sol_balance": round(net_sol, 4),
                "children_count": children_count,
                "trade_count": len(trades.data),
                "buy_trades": len(buy_trades),
                "sell_trades": len(sell_trades),
                "avg_buy_price": round(avg_buy_price, 8),
                "avg_sell_price": round(avg_sell_price, 8),
                "lineage_connections": connected_lineages,
                "first_yaffa_received": wallet.get('first_yaffa_received'),
                "last_activity": wallet.get('last_activity'),
                "is_active": wallet.get('current_yaffa_balance', 0) > 0,
                "performance_score": round(net_sol, 4)  # Simple performance metric
            })
        
        # Sort by generation, then by performance
        result.sort(key=lambda x: (x["generation"], -x["performance_score"]))
        
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

@router.get("/crawl-status")
async def get_crawl_status(supabase = Depends(get_supabase)):
    """Get enhanced crawling status"""
    try:
        status = supabase.table('crawl_status').select('*').execute()
        
        total_wallets = len(status.data)
        completed = len([s for s in status.data if s['status'] == 'completed'])
        in_progress = len([s for s in status.data if s['status'] == 'crawling'])
        errors = len([s for s in status.data if s['status'] == 'error'])
        pending = len([s for s in status.data if s['status'] == 'pending'])
        
        # Get error details
        error_wallets = [s for s in status.data if s['status'] == 'error']
        recent_errors = error_wallets[:5]  # Last 5 errors
        
        return {
            "total_wallets": total_wallets,
            "completed": completed,
            "in_progress": in_progress,
            "pending": pending,
            "errors": errors,
            "progress_percentage": (completed / total_wallets * 100) if total_wallets > 0 else 0,
            "recent_errors": [
                {
                    "wallet": e['wallet_address'][:8] + '...',
                    "error": e.get('error_message', 'Unknown error')[:100],
                    "updated_at": e.get('updated_at')
                }
                for e in recent_errors
            ],
            "is_crawling": in_progress > 0
        }
    except Exception as e:
        return {
            "total_wallets": 0,
            "completed": 0,
            "in_progress": 0,
            "pending": 0,
            "errors": 0,
            "progress_percentage": 0,
            "recent_errors": [],
            "is_crawling": False
        }

@router.get("/wallet/{wallet_id}/lineages")
async def get_wallet_lineages(wallet_id: int, supabase = Depends(get_supabase)):
    """Get all lineage connections for a wallet"""
    try:
        wallet = supabase.table('wallets').select('*').eq('id', wallet_id).execute()
        if not wallet.data:
            raise HTTPException(status_code=404, detail="Wallet not found")
        
        wallet_data = wallet.data[0]
        
        # Get primary lineage (mother_wallet_id)
        primary_mother = None
        if wallet_data.get('mother_wallet_id'):
            primary_mother_data = supabase.table('mother_wallets').select('*').eq('id', wallet_data['mother_wallet_id']).execute()
            if primary_mother_data.data:
                primary_mother = primary_mother_data.data[0]
        
        # Get additional lineages
        additional_lineages = supabase.table('wallet_lineages').select(
            '*, mother_wallets(*)'
        ).eq('wallet_id', wallet_id).execute()
        
        return {
            "wallet": {
                "id": wallet_data['id'],
                "address": wallet_data['address'],
                "generation": wallet_data.get('generation', 0)
            },
            "primary_lineage": primary_mother,
            "additional_lineages": additional_lineages.data,
            "total_lineages": 1 + len(additional_lineages.data) if primary_mother else len(additional_lineages.data)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export/{mother_id}")
async def export_mother_wallet_data(mother_id: int, supabase = Depends(get_supabase)):
    """Export enhanced data for a mother wallet as JSON"""
    try:
        mother_wallet = supabase.table('mother_wallets').select('*').eq('id', mother_id).execute()
        if not mother_wallet.data:
            raise HTTPException(status_code=404, detail="Mother wallet not found")
        
        mother_data = mother_wallet.data[0]
        descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mother_id).execute()
        
        # Enhanced summary with new metrics
        active_descendants = [w for w in descendants.data if w.get('current_yaffa_balance', 0) > 0]
        external_wallets = [w for w in descendants.data if w.get('is_external', False)]
        
        export_data = {
            "mother_wallet": {
                "address": mother_data['address'],
                "label": mother_data.get('label'),
                "export_timestamp": datetime.utcnow().isoformat(),
                "export_version": "2.0"
            },
            "summary": {
                "total_descendants": len(descendants.data),
                "active_descendants": len(active_descendants),
                "external_wallets": len(external_wallets),
                "max_generation": max((w.get('generation', 0) for w in descendants.data), default=0),
                "total_yaffa_held": sum(w.get('current_yaffa_balance', 0) for w in descendants.data),
                "total_yaffa_sold": sum(w.get('total_yaffa_sold', 0) for w in descendants.data),
                "total_yaffa_bought": sum(w.get('total_yaffa_bought', 0) for w in descendants.data),
                "net_yaffa_balance": sum(w.get('net_yaffa_balance', 0) for w in descendants.data),
                "total_sol_profit": sum(w.get('total_sol_received', 0) for w in descendants.data),
                "total_sol_spent": sum(w.get('total_sol_spent', 0) for w in descendants.data),
                "net_sol_profit": sum(w.get('net_sol_balance', 0) for w in descendants.data)
            },
            "wallets": []
        }
        
        for wallet in descendants.data:
            trades = supabase.table('trades').select('*').eq('wallet_id', wallet['id']).execute()
            lineages = supabase.table('wallet_lineages').select('*').eq('wallet_id', wallet['id']).execute()
            
            wallet_data = {
                "address": wallet['address'],
                "generation": wallet.get('generation', 0),
                "is_external": wallet.get('is_external', False),
                "current_yaffa_balance": wallet.get('current_yaffa_balance', 0),
                "total_yaffa_received": wallet.get('total_yaffa_received', 0),
                "total_yaffa_sent": wallet.get('total_yaffa_sent', 0),
                "total_yaffa_bought": wallet.get('total_yaffa_bought', 0),
                "total_yaffa_sold": wallet.get('total_yaffa_sold', 0),
                "net_yaffa_balance": wallet.get('net_yaffa_balance', 0),
                "total_sol_received": wallet.get('total_sol_received', 0),
                "total_sol_spent": wallet.get('total_sol_spent', 0),
                "net_sol_balance": wallet.get('net_sol_balance', 0),
                "lineage_connections": len(lineages.data),
                "trade_count": len(trades.data),
                "first_received": wallet.get('first_yaffa_received'),
                "last_activity": wallet.get('last_activity'),
                "trades": [
                    {
                        "type": trade.get('trade_type', 'sell'),
                        "yaffa_amount": trade.get('yaffa_amount', 0),
                        "yaffa_sold": trade.get('yaffa_amount_sold', 0),
                        "yaffa_bought": trade.get('yaffa_amount_bought', 0),
                        "sol_received": trade.get('sol_amount_received', 0),
                        "sol_spent": trade.get('sol_amount_spent', 0),
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