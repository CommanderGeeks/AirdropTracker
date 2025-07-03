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