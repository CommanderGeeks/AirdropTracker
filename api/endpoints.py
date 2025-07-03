from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from database.connection import get_supabase
from crawlers.wallet_crawler import WalletCrawler
from typing import List, Dict
from datetime import datetime
import json
import traceback

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
        print(f"Error in start_crawl: {e}")
        traceback.print_exc()
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
        try:
            mother_wallets = supabase.table('mother_wallets').select('*').execute()
            mother_wallet_count = len(mother_wallets.data) if mother_wallets.data else 0
        except Exception as e:
            print(f"Error getting mother wallets: {e}")
            mother_wallet_count = 0
        
        # Total wallet stats with basic fields (avoiding missing columns)
        try:
            all_wallets = supabase.table('wallets').select('*').execute()
            wallet_data = all_wallets.data if all_wallets.data else []
        except Exception as e:
            print(f"Error getting wallets: {e}")
            wallet_data = []
        
        total_wallets = len(wallet_data)
        active_wallets = len([w for w in wallet_data if (w.get('current_yaffa_balance') or 0) > 0])
        
        # Use safe field access with defaults and handle None values
        external_wallets = len([w for w in wallet_data if w.get('is_external', False)])
        multi_lineage_wallets = len([w for w in wallet_data if (w.get('lineage_count') or 1) > 1])
        
        # Enhanced token stats with safe field access and None handling
        total_yaffa_held = sum((w.get('current_yaffa_balance') or 0) for w in wallet_data)
        total_yaffa_sold = sum((w.get('total_yaffa_sold') or 0) for w in wallet_data)
        total_yaffa_bought = sum((w.get('total_yaffa_bought') or 0) for w in wallet_data)
        total_sol_profit = sum((w.get('total_sol_received') or 0) for w in wallet_data)
        total_sol_spent = sum((w.get('total_sol_spent') or 0) for w in wallet_data)
        net_sol_profit = sum((w.get('net_sol_balance') or 0) for w in wallet_data)
        
        # Transaction and trade stats with error handling
        try:
            transactions = supabase.table('transactions').select('*').execute()
            transaction_data = transactions.data if transactions.data else []
            lineage_transactions = len([t for t in transaction_data if t.get('is_lineage_transfer', False)])
        except Exception as e:
            print(f"Error getting transactions: {e}")
            transaction_data = []
            lineage_transactions = 0
        
        try:
            trades = supabase.table('trades').select('*').execute()
            trade_data = trades.data if trades.data else []
            buy_trades = len([t for t in trade_data if t.get('trade_type') == 'buy'])
            sell_trades = len([t for t in trade_data if t.get('trade_type') == 'sell'])
        except Exception as e:
            print(f"Error getting trades: {e}")
            trade_data = []
            buy_trades = 0
            sell_trades = 0
        
        # Crawl status with error handling
        try:
            crawl_status = supabase.table('crawl_status').select('*').execute()
            status_data = crawl_status.data if crawl_status.data else []
            completed_crawls = len([c for c in status_data if c.get('status') == 'completed'])
        except Exception as e:
            print(f"Error getting crawl status: {e}")
            completed_crawls = 0
        
        return {
            "mother_wallets": mother_wallet_count,
            "total_wallets": total_wallets,
            "active_wallets": active_wallets,
            "external_wallets": external_wallets,
            "multi_lineage_wallets": multi_lineage_wallets,
            "total_yaffa_held": round(float(total_yaffa_held), 2),
            "total_yaffa_sold": round(float(total_yaffa_sold), 2),
            "total_yaffa_bought": round(float(total_yaffa_bought), 2),
            "total_sol_profit": round(float(total_sol_profit), 4),
            "total_sol_spent": round(float(total_sol_spent), 4),
            "net_sol_profit": round(float(net_sol_profit), 4),
            "total_transactions": len(transaction_data),
            "lineage_transactions": lineage_transactions,
            "external_transactions": len(transaction_data) - lineage_transactions,
            "total_trades": len(trade_data),
            "buy_trades": buy_trades,
            "sell_trades": sell_trades,
            "completed_crawls": completed_crawls,
            "crawl_coverage": round((completed_crawls / mother_wallet_count * 100), 1) if mother_wallet_count > 0 else 0
        }
    except Exception as e:
        print(f"Error in get_summary: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Summary error: {str(e)}")

@router.get("/mother-wallets")
async def get_mother_wallets_detailed(supabase = Depends(get_supabase)):
    """Get detailed information about all mother wallets with enhanced metrics"""
    try:
        # Get mother wallets with error handling
        try:
            mother_wallets = supabase.table('mother_wallets').select('*').execute()
            mother_wallet_data = mother_wallets.data if mother_wallets.data else []
        except Exception as e:
            print(f"Error getting mother wallets: {e}")
            return []
        
        result = []
        
        for mw in mother_wallet_data:
            try:
                # Get descendants with basic error handling
                try:
                    descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mw['id']).execute()
                    descendant_data = descendants.data if descendants.data else []
                except Exception as e:
                    print(f"Error getting descendants for mother wallet {mw['id']}: {e}")
                    descendant_data = []
                
                # Calculate basic stats with safe field access
                total_wallets = len(descendant_data)
                active_wallets = len([w for w in descendant_data if w.get('current_yaffa_balance', 0) > 0])
                max_generation = max((w.get('generation', 0) for w in descendant_data), default=0)
                total_yaffa_held = sum(w.get('current_yaffa_balance', 0) for w in descendant_data)
                total_yaffa_sold = sum(w.get('total_yaffa_sold', 0) for w in descendant_data)
                total_sol_profit = sum(w.get('total_sol_received', 0) for w in descendant_data)
                
                # Get trade count with error handling
                try:
                    wallet_ids = [w['id'] for w in descendant_data]
                    if wallet_ids:
                        # Use .in_ filter for multiple IDs
                        trade_query = supabase.table('trades').select('id').in_('wallet_id', wallet_ids).execute()
                        total_trades = len(trade_query.data) if trade_query.data else 0
                    else:
                        total_trades = 0
                except Exception as e:
                    print(f"Error getting trade count: {e}")
                    total_trades = 0
                
                # Get external wallet discoveries with error handling
                try:
                    external_discoveries = supabase.table('wallets').select('id').eq('discovered_by_mother', mw['id']).eq('is_external', True).execute()
                    external_wallet_count = len(external_discoveries.data) if external_discoveries.data else 0
                except Exception as e:
                    print(f"Error getting external wallets: {e}")
                    external_wallet_count = 0
                
                # Get multi-lineage connections with error handling
                try:
                    lineage_connections = supabase.table('wallet_lineages').select('wallet_id').eq('mother_wallet_id', mw['id']).execute()
                    multi_lineage_connections = len(lineage_connections.data) if lineage_connections.data else 0
                except Exception as e:
                    print(f"Error getting lineage connections: {e}")
                    multi_lineage_connections = 0
                
                # Calculate efficiency metrics
                profit_per_wallet = float(total_sol_profit) / total_wallets if total_wallets > 0 else 0
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
                
            except Exception as e:
                print(f"Error processing mother wallet {mw['id']}: {e}")
                # Add minimal entry for failed wallet
                result.append({
                    "id": mw['id'],
                    "address": mw['address'],
                    "label": mw.get('label'),
                    "total_descendants": 0,
                    "active_descendants": 0,
                    "external_wallets": 0,
                    "multi_lineage_connections": 0,
                    "max_generation": 0,
                    "generation_depth": 0,
                    "total_yaffa_current": 0,
                    "total_yaffa_sold": 0,
                    "total_sol_profit": 0,
                    "total_trades": 0,
                    "profit_per_wallet": 0,
                    "active_wallet_ratio": 0,
                    "created_at": mw.get('created_at'),
                    "error": str(e)
                })
        
        # Sort by total SOL profit descending
        result.sort(key=lambda x: x.get("total_sol_profit", 0), reverse=True)
        
        return result
        
    except Exception as e:
        print(f"Error in get_mother_wallets_detailed: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Mother wallets error: {str(e)}")

@router.get("/mother-wallet/{mother_id}/descendants")
async def get_mother_wallet_descendants(mother_id: int, supabase = Depends(get_supabase)):
    """Get all descendants of a mother wallet with enhanced stats"""
    try:
        mother_wallet = supabase.table('mother_wallets').select('*').eq('id', mother_id).execute()
        if not mother_wallet.data:
            raise HTTPException(status_code=404, detail="Mother wallet not found")
        
        descendants = supabase.table('wallets').select('*').eq('mother_wallet_id', mother_id).execute()
        descendant_data = descendants.data if descendants.data else []
        
        result = []
        for wallet in descendant_data:
            try:
                # Get trading history with enhanced metrics
                try:
                    trades = supabase.table('trades').select('*').eq('wallet_id', wallet['id']).execute()
                    trade_data = trades.data if trades.data else []
                    buy_trades = [t for t in trade_data if t.get('trade_type') == 'buy']
                    sell_trades = [t for t in trade_data if t.get('trade_type') == 'sell']
                except Exception as e:
                    print(f"Error getting trades for wallet {wallet['id']}: {e}")
                    trade_data = []
                    buy_trades = []
                    sell_trades = []
                
                # Get children count
                try:
                    children = supabase.table('wallets').select('id').eq('parent_wallet_id', wallet['id']).execute()
                    children_count = len(children.data) if children.data else 0
                except Exception as e:
                    print(f"Error getting children for wallet {wallet['id']}: {e}")
                    children_count = 0
                
                # Get lineage connections
                try:
                    lineage_connections = supabase.table('wallet_lineages').select('mother_wallet_id').eq('wallet_id', wallet['id']).execute()
                    connected_lineages = len(lineage_connections.data) if lineage_connections.data else 0
                except Exception as e:
                    print(f"Error getting lineage connections for wallet {wallet['id']}: {e}")
                    connected_lineages = 0
                
                # Calculate performance metrics with safe field access
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
                    total_yaffa_sold_trades = sum(t.get('yaffa_amount_sold', 0) for t in sell_trades)
                    avg_sell_price = total_sol_received / total_yaffa_sold_trades if total_yaffa_sold_trades > 0 else 0
                
                result.append({
                    "id": wallet['id'],
                    "address": wallet['address'],
                    "generation": wallet.get('generation', 0),
                    "is_external": wallet.get('is_external', False),
                    "current_yaffa_balance": round(float(wallet.get('current_yaffa_balance', 0)), 2),
                    "total_yaffa_received": round(float(wallet.get('total_yaffa_received', 0)), 2),
                    "total_yaffa_sent": round(float(wallet.get('total_yaffa_sent', 0)), 2),
                    "total_yaffa_bought": round(float(total_bought), 2),
                    "total_yaffa_sold": round(float(total_sold), 2),
                    "net_yaffa_balance": round(float(net_yaffa), 2),
                    "total_sol_received": round(float(wallet.get('total_sol_received', 0)), 4),
                    "total_sol_spent": round(float(wallet.get('total_sol_spent', 0)), 4),
                    "net_sol_balance": round(float(net_sol), 4),
                    "children_count": children_count,
                    "trade_count": len(trade_data),
                    "buy_trades": len(buy_trades),
                    "sell_trades": len(sell_trades),
                    "avg_buy_price": round(float(avg_buy_price), 8),
                    "avg_sell_price": round(float(avg_sell_price), 8),
                    "lineage_connections": connected_lineages,
                    "first_yaffa_received": wallet.get('first_yaffa_received'),
                    "last_activity": wallet.get('last_activity'),
                    "is_active": wallet.get('current_yaffa_balance', 0) > 0,
                    "performance_score": round(float(net_sol), 4)  # Simple performance metric
                })
                
            except Exception as e:
                print(f"Error processing descendant wallet {wallet['id']}: {e}")
                # Add minimal entry for failed wallet
                result.append({
                    "id": wallet['id'],
                    "address": wallet['address'],
                    "generation": wallet.get('generation', 0),
                    "error": str(e)
                })
        
        # Sort by generation, then by performance
        result.sort(key=lambda x: (x.get("generation", 0), -x.get("performance_score", 0)))
        
        return {
            "mother_wallet": {
                "id": mother_wallet.data[0]['id'],
                "address": mother_wallet.data[0]['address'],
                "label": mother_wallet.data[0].get('label')
            },
            "descendants": result
        }
    except Exception as e:
        print(f"Error in get_mother_wallet_descendants: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Descendants error: {str(e)}")

@router.get("/crawl-status")
async def get_crawl_status(supabase = Depends(get_supabase)):
    """Get enhanced crawling status"""
    try:
        status = supabase.table('crawl_status').select('*').execute()
        status_data = status.data if status.data else []
        
        total_wallets = len(status_data)
        completed = len([s for s in status_data if s.get('status') == 'completed'])
        in_progress = len([s for s in status_data if s.get('status') == 'crawling'])
        errors = len([s for s in status_data if s.get('status') == 'error'])
        pending = len([s for s in status_data if s.get('status') == 'pending'])
        
        # Get error details
        error_wallets = [s for s in status_data if s.get('status') == 'error']
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
                    "wallet": e['wallet_address'][:8] + '...' if e.get('wallet_address') else 'Unknown',
                    "error": e.get('error_message', 'Unknown error')[:100],
                    "updated_at": e.get('updated_at')
                }
                for e in recent_errors
            ],
            "is_crawling": in_progress > 0
        }
    except Exception as e:
        print(f"Error in get_crawl_status: {e}")
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
            "additional_lineages": additional_lineages.data if additional_lineages.data else [],
            "total_lineages": 1 + len(additional_lineages.data) if primary_mother else len(additional_lineages.data)
        }
    except Exception as e:
        print(f"Error in get_wallet_lineages: {e}")
        traceback.print_exc()
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
        descendant_data = descendants.data if descendants.data else []
        
        # Enhanced summary with new metrics
        active_descendants = [w for w in descendant_data if w.get('current_yaffa_balance', 0) > 0]
        external_wallets = [w for w in descendant_data if w.get('is_external', False)]
        
        export_data = {
            "mother_wallet": {
                "address": mother_data['address'],
                "label": mother_data.get('label'),
                "export_timestamp": datetime.utcnow().isoformat(),
                "export_version": "2.0"
            },
            "summary": {
                "total_descendants": len(descendant_data),
                "active_descendants": len(active_descendants),
                "external_wallets": len(external_wallets),
                "max_generation": max((w.get('generation', 0) for w in descendant_data), default=0),
                "total_yaffa_held": sum(w.get('current_yaffa_balance', 0) for w in descendant_data),
                "total_yaffa_sold": sum(w.get('total_yaffa_sold', 0) for w in descendant_data),
                "total_yaffa_bought": sum(w.get('total_yaffa_bought', 0) for w in descendant_data),
                "net_yaffa_balance": sum(w.get('net_yaffa_balance', 0) for w in descendant_data),
                "total_sol_profit": sum(w.get('total_sol_received', 0) for w in descendant_data),
                "total_sol_spent": sum(w.get('total_sol_spent', 0) for w in descendant_data),
                "net_sol_profit": sum(w.get('net_sol_balance', 0) for w in descendant_data)
            },
            "wallets": []
        }
        
        for wallet in descendant_data:
            try:
                trades = supabase.table('trades').select('*').eq('wallet_id', wallet['id']).execute()
                trade_data = trades.data if trades.data else []
                
                lineages = supabase.table('wallet_lineages').select('*').eq('wallet_id', wallet['id']).execute()
                lineage_data = lineages.data if lineages.data else []
                
                wallet_export = {
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
                    "lineage_connections": len(lineage_data),
                    "trade_count": len(trade_data),
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
                            "timestamp": trade.get('timestamp')
                        }
                        for trade in trade_data
                    ]
                }
                
                export_data["wallets"].append(wallet_export)
            except Exception as e:
                print(f"Error exporting wallet {wallet['id']}: {e}")
        
        return export_data
    except Exception as e:
        print(f"Error in export_mother_wallet_data: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))