import httpx
import json
import os
from typing import List, Dict, Optional
import asyncio
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class SolanaRPCClient:
    def __init__(self):
        self.rpc_url = os.getenv("QUICKNODE_RPC_URL")
        self.yaffa_mint = os.getenv("YAFFA_TOKEN_MINT")
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def close(self):
        await self.client.aclose()
    
    async def _make_rpc_call(self, method: str, params: List = None):
        """Make a JSON-RPC call to Solana"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or []
        }
        
        try:
            response = await self.client.post(
                self.rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            data = response.json()
            
            if "error" in data:
                raise Exception(f"RPC Error: {data['error']}")
            
            return data.get("result")
        except Exception as e:
            print(f"RPC call failed for {method}: {e}")
            return None
    
    async def get_token_account_balance(self, wallet_address: str) -> float:
        """Get current Yaffa token balance for a wallet"""
        try:
            # Get token accounts for this wallet
            token_accounts = await self._make_rpc_call(
                "getTokenAccountsByOwner",
                [
                    wallet_address,
                    {"mint": self.yaffa_mint},
                    {"encoding": "jsonParsed"}
                ]
            )
            
            if not token_accounts or not token_accounts.get("value"):
                return 0.0
            
            total_balance = 0.0
            for account in token_accounts["value"]:
                parsed_info = account["account"]["data"]["parsed"]["info"]
                balance = float(parsed_info["tokenAmount"]["uiAmount"] or 0)
                total_balance += balance
            
            return total_balance
        except Exception as e:
            print(f"Error getting token balance for {wallet_address}: {e}")
            return 0.0
    
    async def get_token_transactions(self, wallet_address: str, before: str = None, limit: int = 100) -> List[Dict]:
        """Get token transactions for a wallet"""
        try:
            params = [
                wallet_address,
                {
                    "limit": limit,
                    "encoding": "jsonParsed"
                }
            ]
            
            if before:
                params[1]["before"] = before
            
            result = await self._make_rpc_call("getSignaturesForAddress", params)
            
            if not result:
                return []
            
            # Get full transaction details
            transactions = []
            signatures = [tx["signature"] for tx in result]
            
            # Batch get transaction details
            for signature in signatures:
                tx_detail = await self.get_transaction_detail(signature)
                if tx_detail:
                    transactions.append(tx_detail)
                    
                # Rate limiting
                await asyncio.sleep(0.1)
            
            return transactions
        except Exception as e:
            print(f"Error getting transactions for {wallet_address}: {e}")
            return []
    
    async def get_transaction_detail(self, signature: str) -> Optional[Dict]:
        """Get detailed transaction information"""
        try:
            result = await self._make_rpc_call(
                "getTransaction",
                [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0
                    }
                ]
            )
            return result
        except Exception as e:
            print(f"Error getting transaction detail for {signature}: {e}")
            return None
    
    async def parse_yaffa_transfers(self, transaction: Dict) -> List[Dict]:
        """Parse Yaffa token transfers from a transaction"""
        transfers = []
        
        try:
            if not transaction or not transaction.get("meta"):
                return transfers
            
            # Check if transaction was successful
            if transaction["meta"].get("err"):
                return transfers
            
            # Parse pre and post token balances to find transfers
            pre_balances = transaction["meta"].get("preTokenBalances", [])
            post_balances = transaction["meta"].get("postTokenBalances", [])
            
            # Create balance change map
            balance_changes = {}
            
            # Process pre-balances
            for balance in pre_balances:
                if balance.get("mint") == self.yaffa_mint:
                    account = balance["accountIndex"]
                    amount = float(balance["uiTokenAmount"]["uiAmount"] or 0)
                    balance_changes[account] = {"pre": amount, "post": 0}
            
            # Process post-balances
            for balance in post_balances:
                if balance.get("mint") == self.yaffa_mint:
                    account = balance["accountIndex"]
                    amount = float(balance["uiTokenAmount"]["uiAmount"] or 0)
                    if account in balance_changes:
                        balance_changes[account]["post"] = amount
                    else:
                        balance_changes[account] = {"pre": 0, "post": amount}
            
            # Find actual transfers
            accounts = transaction["transaction"]["message"]["accountKeys"]
            
            for account_idx, change in balance_changes.items():
                diff = change["post"] - change["pre"]
                if abs(diff) > 0:  # There was a change
                    account_address = accounts[account_idx]["pubkey"]
                    
                    transfers.append({
                        "account": account_address,
                        "amount_change": diff,
                        "signature": transaction["transaction"]["signatures"][0],
                        "timestamp": datetime.fromtimestamp(transaction["blockTime"]),
                        "block_height": transaction["slot"]
                    })
            
            return transfers
            
        except Exception as e:
            print(f"Error parsing transfers from transaction: {e}")
            return transfers
    
    async def is_trade_transaction(self, transaction: Dict) -> Optional[Dict]:
        """Check if transaction is a Yaffa->SOL trade and extract details"""
        try:
            if not transaction or not transaction.get("meta"):
                return None
            
            # Look for program interactions that indicate DEX trades
            instructions = transaction["transaction"]["message"]["instructions"]
            
            # Common DEX program IDs
            dex_programs = {
                "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter",
                "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium",
                "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "orca"
            }
            
            for instruction in instructions:
                program_id = instruction.get("programId")
                if program_id in dex_programs:
                    # This is likely a DEX trade
                    # Parse the token changes to confirm Yaffa out, SOL in
                    yaffa_transfers = await self.parse_yaffa_transfers(transaction)
                    sol_change = self._get_sol_balance_change(transaction)
                    
                    # Look for Yaffa decrease and SOL increase (indicating a sell)
                    yaffa_sold = 0
                    for transfer in yaffa_transfers:
                        if transfer["amount_change"] < 0:
                            yaffa_sold += abs(transfer["amount_change"])
                    
                    if yaffa_sold > 0 and sol_change > 0:
                        return {
                            "yaffa_sold": yaffa_sold,
                            "sol_received": sol_change,
                            "dex": dex_programs[program_id],
                            "signature": transaction["transaction"]["signatures"][0],
                            "timestamp": datetime.fromtimestamp(transaction["blockTime"]),
                            "price_per_token": sol_change / yaffa_sold if yaffa_sold > 0 else 0
                        }
            
            return None
            
        except Exception as e:
            print(f"Error checking if transaction is trade: {e}")
            return None
    
    def _get_sol_balance_change(self, transaction: Dict) -> float:
        """Get SOL balance change from transaction"""
        try:
            pre_balances = transaction["meta"]["preBalances"]
            post_balances = transaction["meta"]["postBalances"]
            
            # Find the wallet that received SOL (usually first account)
            if len(pre_balances) > 0 and len(post_balances) > 0:
                # Convert lamports to SOL
                pre_sol = pre_balances[0] / 1_000_000_000
                post_sol = post_balances[0] / 1_000_000_000
                return post_sol - pre_sol
            
            return 0.0
        except Exception as e:
            print(f"Error calculating SOL balance change: {e}")
            return 0.0

async def get_sol_balance_change(self, wallet_address: str, transaction: Dict) -> float:
    """Enhanced SOL balance change detection"""
    try:
        pre_balances = transaction["meta"]["preBalances"]
        post_balances = transaction["meta"]["postBalances"]
        accounts = transaction["transaction"]["message"]["accountKeys"]
        
        # Find the wallet's position in the accounts array
        wallet_index = None
        for i, account in enumerate(accounts):
            if account.get("pubkey") == wallet_address or account == wallet_address:
                wallet_index = i
                break
        
        if wallet_index is not None and wallet_index < len(pre_balances) and wallet_index < len(post_balances):
            # Convert lamports to SOL
            pre_sol = pre_balances[wallet_index] / 1_000_000_000
            post_sol = post_balances[wallet_index] / 1_000_000_000
            return post_sol - pre_sol
        
        return 0.0
    except Exception as e:
        print(f"Error calculating SOL balance change: {e}")
        return 0.0

async def detect_enhanced_trade_patterns(self, wallet_address: str, transaction: Dict) -> Optional[Dict]:
    """Enhanced trade detection with better pattern recognition"""
    try:
        if not transaction or not transaction.get("meta"):
            return None
        
        # Skip failed transactions
        if transaction["meta"].get("err"):
            return None
        
        # Parse token transfers and SOL changes
        yaffa_transfers = await self.parse_yaffa_transfers(transaction)
        sol_change = await self.get_sol_balance_change(wallet_address, transaction)
        
        # Find YAFFA change for this specific wallet
        wallet_yaffa_change = 0
        for transfer in yaffa_transfers:
            if transfer['account'] == wallet_address:
                wallet_yaffa_change = transfer['amount_change']
                break
        
        # Enhanced trade detection patterns
        if wallet_yaffa_change != 0 or sol_change != 0:
            # Pattern 1: Clear sell (YAFFA out, SOL in)
            if wallet_yaffa_change < 0 and sol_change > 0:
                return {
                    'type': 'sell',
                    'yaffa_amount': abs(wallet_yaffa_change),
                    'sol_amount': sol_change,
                    'price_per_token': sol_change / abs(wallet_yaffa_change) if wallet_yaffa_change != 0 else 0,
                    'confidence': 'high'
                }
            
            # Pattern 2: Clear buy (YAFFA in, SOL out)
            elif wallet_yaffa_change > 0 and sol_change < 0:
                return {
                    'type': 'buy',
                    'yaffa_amount': wallet_yaffa_change,
                    'sol_amount': abs(sol_change),
                    'price_per_token': abs(sol_change) / wallet_yaffa_change if wallet_yaffa_change != 0 else 0,
                    'confidence': 'high'
                }
            
            # Pattern 3: SOL-only change (might be partial trade or fee)
            elif sol_change != 0 and wallet_yaffa_change == 0:
                # Check if there are any YAFFA transfers in the transaction
                if len(yaffa_transfers) > 0:
                    return {
                        'type': 'sell' if sol_change > 0 else 'buy',
                        'yaffa_amount': 0,  # Will be updated by caller if needed
                        'sol_amount': abs(sol_change),
                        'price_per_token': 0,
                        'confidence': 'medium',
                        'note': 'SOL-only change detected'
                    }
            
            # Pattern 4: YAFFA-only change (internal transfer or complex trade)
            elif wallet_yaffa_change != 0 and sol_change == 0:
                return {
                    'type': 'transfer_or_complex_trade',
                    'yaffa_amount': abs(wallet_yaffa_change),
                    'sol_amount': 0,
                    'price_per_token': 0,
                    'confidence': 'low',
                    'note': 'YAFFA change without SOL movement'
                }
        
        return None
        
    except Exception as e:
        print(f"Error in enhanced trade detection: {e}")
        return None

async def analyze_transaction_context(self, transaction: Dict) -> Dict:
    """Analyze transaction for additional context"""
    try:
        context = {
            'program_ids': [],
            'instruction_count': 0,
            'has_dex_interaction': False,
            'transaction_fee': 0,
            'compute_units_consumed': 0
        }
        
        # Extract program IDs
        instructions = transaction["transaction"]["message"]["instructions"]
        accounts = transaction["transaction"]["message"]["accountKeys"]
        
        context['instruction_count'] = len(instructions)
        
        for instruction in instructions:
            program_id = None
            if isinstance(instruction.get("programId"), str):
                program_id = instruction["programId"]
            elif isinstance(instruction.get("programIdIndex"), int):
                program_idx = instruction["programIdIndex"]
                if program_idx < len(accounts):
                    program_id = accounts[program_idx].get("pubkey") or accounts[program_idx]
            
            if program_id:
                context['program_ids'].append(program_id)
        
        # Check for known DEX program interactions
        dex_programs = {
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter",
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium",
            "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv": "raydium_clmm",
            "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "raydium_cpmm",
            "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "orca",
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca_whirlpool"
        }
        
        for program_id in context['program_ids']:
            if program_id in dex_programs:
                context['has_dex_interaction'] = True
                context['dex_used'] = dex_programs[program_id]
                break
        
        # Extract transaction fee
        if transaction.get("meta") and transaction["meta"].get("fee"):
            context['transaction_fee'] = transaction["meta"]["fee"] / 1_000_000_000  # Convert to SOL
        
        # Extract compute units if available
        if transaction.get("meta") and transaction["meta"].get("computeUnitsConsumed"):
            context['compute_units_consumed'] = transaction["meta"]["computeUnitsConsumed"]
        
        return context
        
    except Exception as e:
        print(f"Error analyzing transaction context: {e}")
        return {'error': str(e)}

async def validate_trade_legitimacy(self, trade_data: Dict, transaction: Dict) -> Dict:
    """Validate if a detected trade is legitimate"""
    try:
        validation = {
            'is_valid': True,
            'confidence_score': 1.0,
            'warnings': [],
            'trade_type_confirmed': trade_data.get('type')
        }
        
        # Check for reasonable price ranges
        price = trade_data.get('price_per_token', 0)
        if price > 0:
            # Flag extremely high or low prices (adjust these based on YAFFA's typical range)
            if price > 1.0:  # More than 1 SOL per YAFFA token
                validation['warnings'].append('Unusually high price detected')
                validation['confidence_score'] *= 0.8
            elif price < 0.000001:  # Less than 0.000001 SOL per YAFFA
                validation['warnings'].append('Unusually low price detected')
                validation['confidence_score'] *= 0.8
        
        # Check for reasonable amounts
        yaffa_amount = trade_data.get('yaffa_amount', 0)
        sol_amount = trade_data.get('sol_amount', 0)
        
        if yaffa_amount > 1_000_000:  # More than 1M YAFFA
            validation['warnings'].append('Large YAFFA amount detected')
            validation['confidence_score'] *= 0.9
        
        if sol_amount > 100:  # More than 100 SOL
            validation['warnings'].append('Large SOL amount detected')
            validation['confidence_score'] *= 0.9
        
        # Check transaction context
        context = await self.analyze_transaction_context(transaction)
        if not context.get('has_dex_interaction'):
            validation['warnings'].append('No known DEX interaction detected')
            validation['confidence_score'] *= 0.7
        
        # Final validation
        if validation['confidence_score'] < 0.5:
            validation['is_valid'] = False
            validation['warnings'].append('Low confidence score')
        
        return validation
        
    except Exception as e:
        print(f"Error validating trade: {e}")
        return {'is_valid': False, 'error': str(e)}

# Enhanced version of the existing method
async def parse_enhanced_yaffa_transfers(self, transaction: Dict) -> List[Dict]:
    """Enhanced YAFFA transfer parsing with better error handling"""
    transfers = []
    
    try:
        if not transaction or not transaction.get("meta"):
            return transfers
        
        # Check if transaction was successful
        if transaction["meta"].get("err"):
            return transfers
        
        # Parse pre and post token balances
        pre_balances = transaction["meta"].get("preTokenBalances", [])
        post_balances = transaction["meta"].get("postTokenBalances", [])
        
        # Create enhanced balance change map
        balance_changes = {}
        
        # Process pre-balances with better error handling
        for balance in pre_balances:
            try:
                if balance.get("mint") == self.yaffa_mint:
                    account_index = balance["accountIndex"]
                    amount = float(balance["uiTokenAmount"]["uiAmount"] or 0)
                    balance_changes[account_index] = {
                        "pre": amount, 
                        "post": 0,
                        "mint": balance["mint"],
                        "decimals": balance["uiTokenAmount"]["decimals"]
                    }
            except Exception as e:
                print(f"Error processing pre-balance: {e}")
                continue
        
        # Process post-balances
        for balance in post_balances:
            try:
                if balance.get("mint") == self.yaffa_mint:
                    account_index = balance["accountIndex"]
                    amount = float(balance["uiTokenAmount"]["uiAmount"] or 0)
                    if account_index in balance_changes:
                        balance_changes[account_index]["post"] = amount
                    else:
                        balance_changes[account_index] = {
                            "pre": 0, 
                            "post": amount,
                            "mint": balance["mint"],
                            "decimals": balance["uiTokenAmount"]["decimals"]
                        }
            except Exception as e:
                print(f"Error processing post-balance: {e}")
                continue
        
        # Extract account addresses
        accounts = transaction["transaction"]["message"]["accountKeys"]
        
        # Build enhanced transfer records
        for account_idx, change_data in balance_changes.items():
            try:
                diff = change_data["post"] - change_data["pre"]
                if abs(diff) > 0:  # There was a change
                    # Handle different account key formats
                    if account_idx < len(accounts):
                        account_key = accounts[account_idx]
                        account_address = account_key.get("pubkey") if isinstance(account_key, dict) else account_key
                        
                        transfer_record = {
                            "account": account_address,
                            "amount_change": diff,
                            "pre_balance": change_data["pre"],
                            "post_balance": change_data["post"],
                            "signature": transaction["transaction"]["signatures"][0],
                            "timestamp": datetime.fromtimestamp(transaction["blockTime"]) if transaction.get("blockTime") else datetime.utcnow(),
                            "block_height": transaction["slot"],
                            "mint": change_data["mint"],
                            "decimals": change_data["decimals"]
                        }
                        
                        transfers.append(transfer_record)
            except Exception as e:
                print(f"Error building transfer record for account {account_idx}: {e}")
                continue
        
        return transfers
        
    except Exception as e:
        print(f"Error in enhanced YAFFA transfer parsing: {e}")
        return transfers