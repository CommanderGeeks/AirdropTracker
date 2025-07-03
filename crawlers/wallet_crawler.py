import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Set
from database.connection import get_supabase_client
from .solana_client import SolanaRPCClient
import os

class WalletCrawler:
    def __init__(self):
        self.solana_client = SolanaRPCClient()
        self.processed_signatures: Set[str] = set()
        self.max_depth = 10  # Prevent infinite recursion
        self.supabase = get_supabase_client()
        
    async def close(self):
        await self.solana_client.close()
    
    async def crawl_all_mother_wallets(self, wallet_addresses: List[str]):
        """Crawl all mother wallets and their descendants"""
        print(f"Starting crawl of {len(wallet_addresses)} mother wallets...")
        
        for address in wallet_addresses:
            try:
                print(f"Crawling mother wallet: {address}")
                await self.crawl_wallet_tree(address, is_mother_wallet=True)
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                print(f"Error crawling wallet {address}: {e}")
                self.update_crawl_status(address, "error", str(e))
        
        print("Crawling completed!")
    
    # PRUNING FUNCTION REMOVED - was deleting all transactions!
    # async def prune_empty_branches(self, mother_wallet_id: int):
    #     """This function was removing all transaction data - DISABLED"""
    #     pass
    
    async def crawl_wallet_tree(self, wallet_address: str, is_mother_wallet: bool = False, 
                               parent_wallet_id: int = None, mother_wallet_id: int = None,
                               generation: int = 0):
        """Recursively crawl a wallet and all its descendants"""
        
        if generation > self.max_depth:
            print(f"Max depth reached for wallet {wallet_address}")
            return
        
        try:
            # Update crawl status
            self.update_crawl_status(wallet_address, "crawling")
            
            # Get or create wallet record
            wallet = await self.get_or_create_wallet(
                wallet_address, is_mother_wallet, parent_wallet_id, mother_wallet_id, generation
            )
            
            if not wallet:
                self.update_crawl_status(wallet_address, "error", "Failed to create wallet record")
                return
            
            # Get current token balance
            current_balance = await self.solana_client.get_token_account_balance(wallet_address)
            
            # Update wallet balance
            self.supabase.table('wallets').update({
                'current_yaffa_balance': current_balance,
                'last_activity': datetime.utcnow().isoformat()
            }).eq('id', wallet['id']).execute()
            
            print(f"Wallet {wallet_address[:8]}... - Current balance: {current_balance} YAFFA")
            
            # Get all transactions for this wallet
            await self.crawl_wallet_transactions(wallet)
            
            # NOW find all outbound transfers (after transactions are processed)
            outbound_transfers = self.supabase.table('transactions').select('*').eq('from_wallet_id', wallet['id']).eq('transaction_type', 'transfer').execute()
            
            print(f"Found {len(outbound_transfers.data)} outbound transfers from wallet {wallet_address[:8]}...")
            
            # Crawl each recipient wallet
            for transfer in outbound_transfers.data:
                # Get recipient wallet
                recipient = self.supabase.table('wallets').select('*').eq('id', transfer['to_wallet_id']).execute()
                if recipient.data:
                    recipient_wallet = recipient.data[0]
                    print(f"  → Found recipient wallet: {recipient_wallet['address'][:8]}... (generation {recipient_wallet['generation']})")
                    
                    # Only crawl if we haven't already crawled this generation or deeper
                    if recipient_wallet['generation'] > generation + 1 or recipient_wallet.get('current_yaffa_balance') is None:
                        # Update generation and crawl
                        self.supabase.table('wallets').update({
                            'generation': generation + 1
                        }).eq('id', recipient_wallet['id']).execute()
                        
                        print(f"  → Crawling recipient wallet: {recipient_wallet['address'][:8]}... (new generation: {generation + 1})")
                        await self.crawl_wallet_tree(
                            recipient_wallet['address'], 
                            False, 
                            wallet['id'], 
                            wallet['mother_wallet_id'] or wallet['id'],  # Use current wallet as mother if none set
                            generation + 1
                        )
                    else:
                        print(f"  → Skipping {recipient_wallet['address'][:8]}... (already crawled at generation {recipient_wallet['generation']})")
                else:
                    print(f"  → WARNING: Recipient wallet not found for transfer ID {transfer['id']}")
            
            # Update wallet totals
            await self.calculate_wallet_totals(wallet)
            
            # PRUNING REMOVED - this was deleting all the transaction data!
            # if is_mother_wallet:
            #     await self.prune_empty_branches(wallet['mother_wallet_id'])
            
            self.update_crawl_status(wallet_address, "completed")
            
        except Exception as e:
            print(f"Error crawling wallet {wallet_address}: {e}")
            self.update_crawl_status(wallet_address, "error", str(e))
    
    async def get_or_create_wallet(self, address: str, is_mother: bool,
                                 parent_id: int, mother_id: int, generation: int) -> Dict:
        """Get existing wallet or create new one"""
        
        # Check if wallet already exists
        existing = self.supabase.table('wallets').select('*').eq('address', address).execute()
        if existing.data:
            return existing.data[0]
        
        # Handle mother wallet
        if is_mother:
            # Check if mother wallet exists
            mother_result = self.supabase.table('mother_wallets').select('*').eq('address', address).execute()
            if not mother_result.data:
                # Create mother wallet
                mother_wallet = self.supabase.table('mother_wallets').insert({
                    'address': address,
                    'label': f'Mother {address[:8]}'
                }).execute()
                mother_wallet_id = mother_wallet.data[0]['id']
            else:
                mother_wallet_id = mother_result.data[0]['id']
            
            # Create wallet record
            wallet_data = {
                'address': address,
                'mother_wallet_id': mother_wallet_id,
                'parent_wallet_id': None,
                'generation': 0
            }
        else:
            wallet_data = {
                'address': address,
                'mother_wallet_id': mother_id,
                'parent_wallet_id': parent_id,
                'generation': generation
            }
        
        result = self.supabase.table('wallets').insert(wallet_data).execute()
        return result.data[0]
    
    async def crawl_wallet_transactions(self, wallet: Dict):
        """Get all transactions for a wallet"""
        print(f"Crawling transactions for {wallet['address'][:8]}...")
        
        all_transactions = []
        before_signature = None
        batch_count = 0
        max_batches = 50  # Limit to 5000 transactions (50 * 100)
        max_total_transactions = 1000  # Hard limit on total transactions to process
        
        # Get transactions in batches
        while batch_count < max_batches:
            try:
                print(f"  Fetching batch {batch_count + 1}/{max_batches}...")
                transactions = await self.solana_client.get_token_transactions(
                    wallet['address'], before_signature, limit=100
                )
                
                if not transactions:
                    print(f"  No more transactions found after {batch_count} batches")
                    break
                
                new_transactions = 0
                for tx in transactions:
                    if tx["transaction"]["signatures"][0] not in self.processed_signatures:
                        all_transactions.append(tx)
                        self.processed_signatures.add(tx["transaction"]["signatures"][0])
                        new_transactions += 1
                        
                        # Stop if we hit our transaction limit
                        if len(all_transactions) >= max_total_transactions:
                            print(f"  ⚠️ Hit transaction limit ({max_total_transactions}), stopping crawl")
                            break
                
                print(f"  Found {new_transactions} new transactions in batch {batch_count + 1}")
                
                # Break if we hit transaction limit
                if len(all_transactions) >= max_total_transactions:
                    break
                
                # Set up for next batch
                if len(transactions) < 100:
                    print(f"  Reached end of transaction history ({len(transactions)} in final batch)")
                    break
                
                before_signature = transactions[-1]["transaction"]["signatures"][0]
                batch_count += 1
                await asyncio.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"Error getting transactions for {wallet['address']} at batch {batch_count}: {e}")
                break
        
        print(f"Processing {len(all_transactions)} transactions for {wallet['address'][:8]}...")
        
        # Process each transaction with progress updates
        for i, tx in enumerate(all_transactions):
            if i % 10 == 0:  # Progress update every 10 transactions
                print(f"  Processing transaction {i + 1}/{len(all_transactions)} for {wallet['address'][:8]}...")
            await self.process_transaction(wallet, tx)
    
    async def process_transaction(self, wallet: Dict, transaction: Dict):
        """Process a single transaction"""
        try:
            signature = transaction["transaction"]["signatures"][0]
            
            print(f"Processing transaction: {signature[:16]}...")
            
            # Skip if already processed GLOBALLY (not just by this mother wallet)
            existing = self.supabase.table('transactions').select('*').eq('raw_data->>original_signature', signature).execute()
            if existing.data:
                print(f"Transaction {signature[:16]}... already processed globally, skipping")
                return
            
            # Parse token transfers first to detect Raydium
            transfers = await self.solana_client.parse_yaffa_transfers(transaction)
            print(f"Found {len(transfers)} YAFFA transfers in transaction")
            
            # Check for Raydium DEX interactions
            raydium_interaction = await self.detect_raydium_interaction(wallet, transfers, transaction)
            if raydium_interaction:
                print(f"Found Raydium {raydium_interaction['type']}: {raydium_interaction}")
                await self.record_raydium_trade(wallet, raydium_interaction, transaction)
                return  # Don't process as regular transfers
            
            # Process as regular transfers
            for i, transfer in enumerate(transfers):
                print(f"Transfer {i+1}: {transfer['account'][:16]}... amount_change: {transfer['amount_change']}")
                
                # Skip if it's the same wallet (internal transaction)
                if transfer["account"] == wallet['address']:
                    print(f"Skipping self-transfer for wallet {wallet['address'][:16]}...")
                    continue
                
                # Determine direction and create transaction record
                if transfer["amount_change"] > 0:
                    # This wallet received tokens
                    print(f"Recording incoming transfer: {transfer['account'][:16]}... → {wallet['address'][:16]}... ({abs(transfer['amount_change'])} YAFFA)")
                    await self.record_transfer(
                        transfer["account"], wallet['address'], 
                        abs(transfer["amount_change"]), transfer, wallet.get('mother_wallet_id')
                    )
                else:
                    # This wallet sent tokens
                    print(f"Recording outgoing transfer: {wallet['address'][:16]}... → {transfer['account'][:16]}... ({abs(transfer['amount_change'])} YAFFA)")
                    await self.record_transfer(
                        wallet['address'], transfer["account"],
                        abs(transfer["amount_change"]), transfer, wallet.get('mother_wallet_id')
                    )
        
        except Exception as e:
            print(f"Error processing transaction {signature}: {e}")
            import traceback
            traceback.print_exc()
    
    async def detect_raydium_interaction(self, wallet: Dict, yaffa_transfers: List, transaction: Dict) -> Dict:
        """Detect if this is a Raydium buy or sell transaction"""
        try:
            # Known Raydium program IDs and pool addresses
            raydium_programs = [
                "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM
                "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Raydium AMM V4
                "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv",  # Raydium CLMM
                "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CPMM
            ]
            
            # Extract SOL changes from transaction
            sol_change = await self.solana_client.get_sol_balance_change(wallet['address'], transaction)
            
            # Find YAFFA change for this wallet
            wallet_yaffa_change = 0
            for transfer in yaffa_transfers:
                if transfer['account'] == wallet['address']:
                    wallet_yaffa_change = transfer['amount_change']
                    break
            
            # Detect Raydium patterns
            if wallet_yaffa_change < 0 and sol_change > 0:
                # SELL: Wallet lost YAFFA, gained SOL
                return {
                    'type': 'sell',
                    'yaffa_amount': abs(wallet_yaffa_change),
                    'sol_amount': sol_change,
                    'price_per_token': sol_change / abs(wallet_yaffa_change) if wallet_yaffa_change != 0 else 0
                }
            elif wallet_yaffa_change > 0 and sol_change < 0:
                # BUY: Wallet gained YAFFA, lost SOL  
                return {
                    'type': 'buy',
                    'yaffa_amount': wallet_yaffa_change,
                    'sol_amount': abs(sol_change),
                    'price_per_token': abs(sol_change) / wallet_yaffa_change if wallet_yaffa_change != 0 else 0
                }
            
            return None
            
        except Exception as e:
            print(f"Error detecting Raydium interaction: {e}")
            return None
    
    async def record_raydium_trade(self, wallet: Dict, raydium_data: Dict, transaction: Dict):
        """Record a Raydium buy or sell transaction"""
        try:
            signature = transaction["transaction"]["signatures"][0]
            
            # Check if trade already exists
            existing_trade = self.supabase.table('trades').select('id').eq('transaction_hash', signature).eq('wallet_id', wallet['id']).execute()
            if existing_trade.data:
                print(f"Raydium trade already recorded, skipping")
                return
            
            trade_data = {
                'wallet_id': wallet['id'],
                'transaction_hash': signature,
                'trade_type': raydium_data['type'],  # 'buy' or 'sell'
                'yaffa_amount': raydium_data['yaffa_amount'],
                'sol_amount': raydium_data['sol_amount'],
                'price_per_token': raydium_data['price_per_token'],
                'timestamp': transaction.get('blockTime', datetime.utcnow()).isoformat(),
                'dex_used': 'Raydium',
                'raw_data': {
                    'signature': signature,
                    'trade_type': raydium_data['type'],
                    'yaffa_amount': raydium_data['yaffa_amount'],
                    'sol_amount': raydium_data['sol_amount'],
                    'price_per_token': raydium_data['price_per_token'],
                    'timestamp': transaction.get('blockTime', datetime.utcnow()).isoformat()
                }
            }
            
            # Different fields for buy vs sell
            if raydium_data['type'] == 'sell':
                trade_data.update({
                    'yaffa_amount_sold': raydium_data['yaffa_amount'],
                    'sol_amount_received': raydium_data['sol_amount']
                })
            else:  # buy
                trade_data.update({
                    'yaffa_amount_bought': raydium_data['yaffa_amount'], 
                    'sol_amount_spent': raydium_data['sol_amount']
                })
            
            self.supabase.table('trades').insert(trade_data).execute()
            
            print(f"✅ Recorded Raydium {raydium_data['type']}: {raydium_data['yaffa_amount']} YAFFA ↔ {raydium_data['sol_amount']} SOL")
            
        except Exception as e:
            print(f"Error recording Raydium trade: {e}")
    
    async def record_transfer(self, from_address: str, to_address: str, 
                            amount: float, transfer_data: Dict, discovering_mother_id: int = None):
        """Record a token transfer with multi-lineage tracking"""
        try:
            signature = transfer_data["signature"]
            
            # GLOBAL deduplication by original signature
            existing_global = self.supabase.table('transactions').select('*').eq('raw_data->>original_signature', signature).execute()
            if existing_global.data:
                print(f"Transaction {signature[:16]}... already processed globally, skipping")
                return
            
            unique_tx_hash = f"{signature[:16]}_{from_address[:8]}_{to_address[:8]}"
            print(f"Recording lineage transfer: {from_address[:16]}... → {to_address[:16]}... ({amount} YAFFA)")
            
            # Get or create FROM wallet
            from_wallet_id, from_wallet_data = await self.get_or_create_wallet_for_transfer(from_address, discovering_mother_id, is_sender=True)
            
            # Get or create TO wallet with multi-lineage support
            to_wallet_id, to_wallet_data = await self.get_or_create_wallet_for_transfer(to_address, discovering_mother_id, is_sender=False, sender_data=from_wallet_data)
            
            # Check for cross-lineage connections
            await self.handle_multi_lineage_membership(to_wallet_data, from_wallet_data, discovering_mother_id)
            
            # Create transaction record with lineage attribution
            result = self.supabase.table('transactions').insert({
                'transaction_hash': unique_tx_hash,
                'from_wallet_id': from_wallet_id,
                'to_wallet_id': to_wallet_id,
                'yaffa_amount': amount,
                'timestamp': transfer_data["timestamp"].isoformat(),
                'block_height': transfer_data.get("block_height"),
                'transaction_type': 'transfer',
                'is_lineage_transfer': from_wallet_data.get('mother_wallet_id') is not None,
                'discovering_mother_id': discovering_mother_id,
                'raw_data': {
                    'original_signature': signature,
                    'from_address': from_address,
                    'to_address': to_address,
                    'amount_change': transfer_data.get("amount_change"),
                    'block_height': transfer_data.get("block_height"),
                    'timestamp': transfer_data["timestamp"].isoformat()
                }
            }).execute()
            
            print(f"✅ Successfully recorded transfer: {unique_tx_hash}")
            
        except Exception as e:
            print(f"❌ Error recording transfer: {e}")
            import traceback
            traceback.print_exc()
    
    async def get_or_create_wallet_for_transfer(self, address: str, discovering_mother_id: int, is_sender: bool, sender_data: Dict = None) -> tuple:
        """Get or create wallet with proper lineage attribution"""
        try:
            # Check if wallet already exists
            existing = self.supabase.table('wallets').select('*').eq('address', address).execute()
            
            if existing.data:
                return existing.data[0]['id'], existing.data[0]
            
            # Create new wallet
            print(f"Creating {'sender' if is_sender else 'recipient'} wallet: {address[:16]}...")
            
            if is_sender:
                # Sender wallet - mark as external if not in lineage
                wallet_data = {
                    'address': address,
                    'mother_wallet_id': None,  # Will be updated if we find lineage
                    'generation': 999,
                    'is_external': True,
                    'discovered_by_mother': discovering_mother_id
                }
            else:
                # Recipient wallet - inherit from sender if possible
                if sender_data and sender_data.get('mother_wallet_id') and not sender_data.get('is_external'):
                    # Sender is in lineage - inherit
                    wallet_data = {
                        'address': address,
                        'mother_wallet_id': sender_data['mother_wallet_id'],
                        'generation': sender_data.get('generation', 0) + 1,
                        'parent_wallet_id': sender_data['id'],
                        'is_external': False,
                        'discovered_by_mother': discovering_mother_id
                    }
                    print(f"  └─ Inheriting lineage from sender: mother_wallet_id {sender_data['mother_wallet_id']}")
                else:
                    # External recipient or sender not in lineage
                    wallet_data = {
                        'address': address,
                        'mother_wallet_id': discovering_mother_id if discovering_mother_id else None,
                        'generation': 1 if discovering_mother_id else 999,
                        'is_external': discovering_mother_id is None,
                        'discovered_by_mother': discovering_mother_id
                    }
                    print(f"  └─ {'Direct discovery by mother' if discovering_mother_id else 'External wallet'}")
            
            result = self.supabase.table('wallets').insert(wallet_data).execute()
            wallet_id = result.data[0]['id']
            wallet_data_with_id = {**wallet_data, 'id': wallet_id}
            
            print(f"Created wallet ID: {wallet_id}")
            return wallet_id, wallet_data_with_id
            
        except Exception as e:
            print(f"Error creating wallet for {address}: {e}")
            raise
    
    async def handle_multi_lineage_membership(self, to_wallet_data: Dict, from_wallet_data: Dict, discovering_mother_id: int):
        """Handle multi-lineage membership for wallets"""
        try:
            wallet_id = to_wallet_data['id']
            current_mother = to_wallet_data.get('mother_wallet_id')
            sender_mother = from_wallet_data.get('mother_wallet_id')
            
            # Skip if no lineage involvement
            if not discovering_mother_id or not current_mother:
                return
            
            # Check if this creates a multi-lineage situation
            if current_mother != discovering_mother_id and sender_mother:
                print(f"  🔗 Multi-lineage detected: wallet {to_wallet_data['address'][:8]} connected to mothers {current_mother} & {discovering_mother_id}")
                
                # Create multi-lineage record
                existing_lineage = self.supabase.table('wallet_lineages').select('*').eq('wallet_id', wallet_id).eq('mother_wallet_id', discovering_mother_id).execute()
                
                if not existing_lineage.data:
                    self.supabase.table('wallet_lineages').insert({
                        'wallet_id': wallet_id,
                        'mother_wallet_id': discovering_mother_id,
                        'connection_type': 'transfer',
                        'created_at': datetime.utcnow().isoformat()
                    }).execute()
                    print(f"  ✅ Added lineage connection: wallet {wallet_id} → mother {discovering_mother_id}")
                
        except Exception as e:
            print(f"Error handling multi-lineage membership: {e}")
    
async def calculate_lineage_totals(self, wallet: Dict):
    """Calculate comprehensive lineage-based totals with multi-lineage support"""
    try:
        wallet_id = wallet['id']
        
        # Get all mother wallets this wallet belongs to
        lineages = self.supabase.table('wallet_lineages').select('mother_wallet_id').eq('wallet_id', wallet_id).execute()
        all_mothers = [wallet.get('mother_wallet_id')] + [l['mother_wallet_id'] for l in lineages.data]
        all_mothers = [m for m in all_mothers if m]  # Remove None values
        
        # Calculate totals for transactions
        sent = self.supabase.table('transactions').select('yaffa_amount').eq('from_wallet_id', wallet_id).eq('transaction_type', 'transfer').execute()
        total_sent = sum(tx['yaffa_amount'] for tx in sent.data)
        
        received = self.supabase.table('transactions').select('yaffa_amount, is_lineage_transfer').eq('to_wallet_id', wallet_id).eq('transaction_type', 'transfer').execute()
        total_received = sum(tx['yaffa_amount'] for tx in received.data)
        lineage_received = sum(tx['yaffa_amount'] for tx in received.data if tx.get('is_lineage_transfer', False))
        
        # Calculate trades with enhanced metrics
        trades = self.supabase.table('trades').select('*').eq('wallet_id', wallet_id).execute()
        
        total_sold = 0
        total_bought = 0
        total_sol_received = 0
        total_sol_spent = 0
        
        for trade in trades.data:
            trade_type = trade.get('trade_type', 'sell')  # Default to sell for backward compatibility
            
            if trade_type == 'sell':
                total_sold += trade.get('yaffa_amount_sold', 0)
                total_sol_received += trade.get('sol_amount_received', 0)
            elif trade_type == 'buy':
                total_bought += trade.get('yaffa_amount_bought', 0)
                total_sol_spent += trade.get('sol_amount_spent', 0)
            
            # Handle legacy trades without trade_type
            if not trade_type and trade.get('yaffa_amount_sold', 0) > 0:
                total_sold += trade.get('yaffa_amount_sold', 0)
                total_sol_received += trade.get('sol_amount_received', 0)
        
        # Net calculations
        net_yaffa_balance = total_received + total_bought - total_sent - total_sold
        net_sol_balance = total_sol_received - total_sol_spent
        lineage_yaffa_balance = lineage_received - total_sent - total_sold
        
        # Update wallet with comprehensive metrics
        update_data = {
            'total_yaffa_received': total_received,
            'total_yaffa_sent': total_sent,
            'total_yaffa_sold': total_sold,
            'total_yaffa_bought': total_bought,
            'total_sol_received': total_sol_received,
            'total_sol_spent': total_sol_spent,
            'net_yaffa_balance': net_yaffa_balance,
            'net_sol_balance': net_sol_balance,
            'lineage_yaffa_received': lineage_received,
            'lineage_yaffa_balance': lineage_yaffa_balance,
            'lineage_count': len(set(all_mothers)),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        self.supabase.table('wallets').update(update_data).eq('id', wallet_id).execute()
        
        print(f"✅ Updated totals for {wallet['address'][:8]}:")
        print(f"  Received: {total_received} YAFFA (Lineage: {lineage_received})")
        print(f"  Sent: {total_sent} YAFFA")
        print(f"  Bought: {total_bought} YAFFA")
        print(f"  Sold: {total_sold} YAFFA")
        print(f"  Net YAFFA: {net_yaffa_balance}")
        print(f"  Net SOL: {net_sol_balance}")
        print(f"  Lineages: {len(set(all_mothers))}")
        
        # Calculate lineage-specific metrics for each mother
        for mother_id in set(all_mothers):
            if mother_id:
                await self.calculate_lineage_totals_for_mother(wallet, mother_id)
        
    except Exception as e:
        print(f"Error calculating lineage totals: {e}")
        import traceback
        traceback.print_exc()

async def calculate_lineage_totals_for_mother(self, wallet: Dict, mother_id: int):
    """Calculate totals for a specific mother lineage"""
    try:
        wallet_id = wallet['id']
        
        # Get transactions involving this specific lineage
        lineage_sent = self.supabase.table('transactions').select('yaffa_amount').eq('from_wallet_id', wallet_id).eq('discovering_mother_id', mother_id).execute()
        lineage_received = self.supabase.table('transactions').select('yaffa_amount').eq('to_wallet_id', wallet_id).eq('discovering_mother_id', mother_id).execute()
        
        lineage_sent_total = sum(tx['yaffa_amount'] for tx in lineage_sent.data)
        lineage_received_total = sum(tx['yaffa_amount'] for tx in lineage_received.data)
        
        print(f"  └─ Lineage {mother_id}: Received {lineage_received_total}, Sent {lineage_sent_total}")
        
        # You could store these lineage-specific metrics in a separate table if needed
        # For now, we track the relationships in wallet_lineages table
        
    except Exception as e:
        print(f"Error calculating lineage totals for mother {mother_id}: {e}")

# Also add this method to replace the existing calculate_wallet_totals call
async def calculate_wallet_totals(self, wallet: Dict):
    """Calculate wallet totals - delegates to enhanced lineage totals"""
    await self.calculate_lineage_totals(wallet)
    
    async def record_trade(self, wallet: Dict, trade_info: Dict):
        """Record a trade transaction"""
        try:
            # Check if trade already exists
            existing_trade = self.supabase.table('trades').select('id').eq('transaction_hash', trade_info["signature"]).eq('wallet_id', wallet['id']).execute()
            if existing_trade.data:
                # Trade already recorded, skip
                return
            
            self.supabase.table('trades').insert({
                'wallet_id': wallet['id'],
                'transaction_hash': trade_info["signature"],
                'yaffa_amount_sold': trade_info["yaffa_sold"],
                'sol_amount_received': trade_info["sol_received"],
                'timestamp': trade_info["timestamp"].isoformat(),
                'dex_used': trade_info["dex"],
                'price_per_token': trade_info["price_per_token"],
                'raw_data': {
                    'signature': trade_info["signature"],
                    'yaffa_sold': trade_info["yaffa_sold"],
                    'sol_received': trade_info["sol_received"],
                    'dex': trade_info["dex"],
                    'price_per_token': trade_info["price_per_token"],
                    'timestamp': trade_info["timestamp"].isoformat()
                }
            }).execute()
            
            # Update wallet totals
            current_sold = wallet.get('total_yaffa_sold', 0) + trade_info["yaffa_sold"]
            current_sol = wallet.get('total_sol_received', 0) + trade_info["sol_received"]
            
            self.supabase.table('wallets').update({
                'total_yaffa_sold': current_sold,
                'total_sol_received': current_sol
            }).eq('id', wallet['id']).execute()
            
        except Exception as e:
            # Only print error if it's not a duplicate key error
            if 'already exists' not in str(e):
                print(f"Error recording trade: {e}")
    
    async def calculate_lineage_totals(self, wallet: Dict):
        """Calculate lineage-based totals (transfer-based, not balance-based)"""
        try:
            # Calculate total LINEAGE YAFFA received (only from lineage wallets)
            lineage_received_query = """
                SELECT SUM(t.yaffa_amount) as total_received
                FROM transactions t
                JOIN wallets w ON t.from_wallet_id = w.id
                WHERE t.to_wallet_id = %s 
                AND t.transaction_type = 'transfer'
                AND t.is_lineage_transfer = true
            """
            
            # Calculate total sent (to any wallet)
            sent = self.supabase.table('transactions').select('yaffa_amount').eq('from_wallet_id', wallet['id']).eq('transaction_type', 'transfer').execute()
            total_sent = sum(tx['yaffa_amount'] for tx in sent.data)
            
            # Calculate total sold and SOL received
            trades = self.supabase.table('trades').select('*').eq('wallet_id', wallet['id']).execute()
            total_sold = sum(trade['yaffa_amount_sold'] for trade in trades.data)
            total_sol = sum(trade['sol_amount_received'] for trade in trades.data)
            
            # Calculate lineage YAFFA balance = received - sent - sold
            received = self.supabase.table('transactions').select('yaffa_amount').eq('to_wallet_id', wallet['id']).eq('transaction_type', 'transfer').execute()
            total_received = sum(tx['yaffa_amount'] for tx in received.data if self.is_lineage_transfer(tx))
            
            lineage_balance = total_received - total_sent - total_sold
            
            # Update wallet with lineage-specific metrics
            self.supabase.table('wallets').update({
                'lineage_yaffa_received': total_received,  # Only from lineage
                'total_yaffa_sent': total_sent,
                'total_yaffa_sold': total_sold,
                'total_sol_received': total_sol,
                'lineage_yaffa_balance': lineage_balance,  # Calculated balance
                'updated_at': datetime.utcnow().isoformat()
            }).eq('id', wallet['id']).execute()
            
            print(f"Lineage totals for {wallet['address'][:8]}...")
            print(f"  Lineage received: {total_received} YAFFA")
            print(f"  Sent: {total_sent} YAFFA") 
            print(f"  Sold: {total_sold} YAFFA")
            print(f"  Lineage balance: {lineage_balance} YAFFA")
            print(f"  SOL profit: {total_sol} SOL")
            
        except Exception as e:
            print(f"Error calculating lineage totals: {e}")
    
    def is_lineage_transfer(self, transaction: Dict) -> bool:
        """Check if a transaction is from a lineage wallet"""
        try:
            # Check if the from_wallet has a mother_wallet_id (is in lineage)
            from_wallet = self.supabase.table('wallets').select('mother_wallet_id').eq('id', transaction.get('from_wallet_id')).execute()
            return from_wallet.data and from_wallet.data[0].get('mother_wallet_id') is not None
        except:
            return False
    
    def update_crawl_status(self, wallet_address: str, status: str, error_msg: str = None):
        """Update crawl status in database"""
        try:
            existing = self.supabase.table('crawl_status').select('*').eq('wallet_address', wallet_address).execute()
            
            if not existing.data:
                # Create new status
                self.supabase.table('crawl_status').insert({
                    'wallet_address': wallet_address,
                    'status': status,
                    'error_message': error_msg,
                    'updated_at': datetime.utcnow().isoformat()
                }).execute()
            else:
                # Update existing status
                update_data = {
                    'status': status,
                    'updated_at': datetime.utcnow().isoformat()
                }
                
                if status == 'completed':
                    update_data['last_crawled'] = datetime.utcnow().isoformat()
                
                if error_msg:
                    update_data['error_message'] = error_msg
                
                self.supabase.table('crawl_status').update(update_data).eq('wallet_address', wallet_address).execute()
                
        except Exception as e:
            print(f"Error updating crawl status: {e}")