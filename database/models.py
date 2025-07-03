from sqlalchemy import Column, String, Integer, Float, DateTime, Boolean, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class MotherWallet(Base):
    __tablename__ = "mother_wallets"
    
    id = Column(Integer, primary_key=True)
    address = Column(String(50), unique=True, nullable=False)
    label = Column(String(100))  # Optional naming/labeling
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    wallets = relationship("Wallet", back_populates="mother_wallet")
    
    # Calculated fields (will be updated via queries)
    total_yaffa_current = Column(Float, default=0.0)
    total_sol_profit = Column(Float, default=0.0)
    total_descendants = Column(Integer, default=0)

class Wallet(Base):
    __tablename__ = "wallets"
    
    id = Column(Integer, primary_key=True)
    address = Column(String(50), unique=True, nullable=False)
    mother_wallet_id = Column(Integer, ForeignKey("mother_wallets.id"), nullable=False)
    parent_wallet_id = Column(Integer, ForeignKey("wallets.id"))  # Self-referencing for tree structure
    
    # Current state
    current_yaffa_balance = Column(Float, default=0.0)
    total_yaffa_received = Column(Float, default=0.0)
    total_yaffa_sent = Column(Float, default=0.0)
    total_yaffa_sold = Column(Float, default=0.0)
    total_sol_received = Column(Float, default=0.0)
    
    # Metadata
    first_yaffa_received = Column(DateTime)
    last_activity = Column(DateTime)
    is_active = Column(Boolean, default=True)  # False if pruned
    generation = Column(Integer, default=0)  # 0 = mother, 1 = direct child, etc.
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    mother_wallet = relationship("MotherWallet", back_populates="wallets")
    parent_wallet = relationship("Wallet", remote_side=[id])
    children = relationship("Wallet", back_populates="parent_wallet")
    
    # Transaction relationships
    sent_transactions = relationship("Transaction", foreign_keys="Transaction.from_wallet_id", back_populates="from_wallet")
    received_transactions = relationship("Transaction", foreign_keys="Transaction.to_wallet_id", back_populates="to_wallet")
    trades = relationship("Trade", back_populates="wallet")

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True)
    transaction_hash = Column(String(100), unique=True, nullable=False)
    from_wallet_id = Column(Integer, ForeignKey("wallets.id"))
    to_wallet_id = Column(Integer, ForeignKey("wallets.id"))
    
    yaffa_amount = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    block_height = Column(Integer)
    
    # Transaction type
    transaction_type = Column(String(20))  # 'transfer', 'trade', 'initial_airdrop'
    
    # Raw transaction data for debugging
    raw_data = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    from_wallet = relationship("Wallet", foreign_keys=[from_wallet_id], back_populates="sent_transactions")
    to_wallet = relationship("Wallet", foreign_keys=[to_wallet_id], back_populates="received_transactions")

class Trade(Base):
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True)
    wallet_id = Column(Integer, ForeignKey("wallets.id"), nullable=False)
    transaction_hash = Column(String(100), nullable=False)
    
    yaffa_amount_sold = Column(Float, nullable=False)
    sol_amount_received = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    
    # DEX information
    dex_used = Column(String(50))  # 'jupiter', 'raydium', etc.
    price_per_token = Column(Float)  # SOL per Yaffa
    
    # Raw trade data
    raw_data = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    wallet = relationship("Wallet", back_populates="trades")

class CrawlStatus(Base):
    __tablename__ = "crawl_status"
    
    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(50), nullable=False)
    last_crawled = Column(DateTime)
    last_transaction_signature = Column(String(100))
    crawl_depth = Column(Integer, default=0)
    status = Column(String(20), default='pending')  # 'pending', 'crawling', 'completed', 'error'
    error_message = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)