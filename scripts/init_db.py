#!/usr/bin/env python3
"""
Database initialization script for PowerBI Chat Integration.
Creates tables, indexes, and seeds initial data.
"""
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

# Database models
from sqlalchemy import Column, String, DateTime, JSON, Text, Integer, ForeignKey, Boolean
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class ChatSession(Base):
    """Chat session model."""
    __tablename__ = "chat_sessions"
    
    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(36), nullable=False, index=True)
    dataset_id = Column(String(36), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    metadata = Column(JSON, default=dict)
    
    messages = relationship("ChatMessage", back_populates="session", cascade="all, delete-orphan")


class ChatMessage(Base):
    """Chat message model."""
    __tablename__ = "chat_messages"
    
    id = Column(String(36), primary_key=True)
    session_id = Column(String(36), ForeignKey("chat_sessions.id"), nullable=False, index=True)
    role = Column(String(20), nullable=False)  # user, assistant, system
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    metadata = Column(JSON, default=dict)
    
    session = relationship("ChatSession", back_populates="messages")


class QueryHistory(Base):
    """Query execution history."""
    __tablename__ = "query_history"
    
    id = Column(String(36), primary_key=True)
    session_id = Column(String(36), ForeignKey("chat_sessions.id"), nullable=False, index=True)
    workspace_id = Column(String(36), nullable=False)
    dataset_id = Column(String(36), nullable=False)
    query = Column(Text, nullable=False)
    result_summary = Column(JSON, default=dict)
    execution_time_ms = Column(Integer)
    status = Column(String(20), default="success")
    created_at = Column(DateTime, default=datetime.utcnow)


class SchemaCache(Base):
    """Cached dataset schemas."""
    __tablename__ = "schema_cache"
    
    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(36), nullable=False)
    dataset_id = Column(String(36), nullable=False, unique=True)
    schema_data = Column(JSON, nullable=False)
    business_dictionary = Column(JSON, default=dict)
    cached_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)
    
    __table_args__ = (
        {"sqlite_autoincrement": True},
    )


class UserPreferences(Base):
    """User preferences and settings."""
    __tablename__ = "user_preferences"
    
    id = Column(String(36), primary_key=True)
    user_id = Column(String(100), nullable=False, unique=True, index=True)
    default_workspace_id = Column(String(36))
    default_dataset_id = Column(String(36))
    visualization_preferences = Column(JSON, default=dict)
    language = Column(String(10), default="pt-BR")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def get_database_url():
    """Get database URL from environment."""
    return os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/powerbi_chat"
    )


def create_sync_engine():
    """Create synchronous database engine."""
    url = get_database_url()
    # Convert async URL to sync if needed
    if url.startswith("postgresql+asyncpg"):
        url = url.replace("postgresql+asyncpg", "postgresql")
    return create_engine(url, echo=True)


async def create_async_engine_instance():
    """Create async database engine."""
    url = get_database_url()
    # Convert to async URL if needed
    if not url.startswith("postgresql+asyncpg"):
        url = url.replace("postgresql://", "postgresql+asyncpg://")
    return create_async_engine(url, echo=True)


def init_database():
    """Initialize database with all tables."""
    print("🚀 Initializing PowerBI Chat Integration database...")
    
    engine = create_sync_engine()
    
    # Create all tables
    print("📦 Creating tables...")
    Base.metadata.create_all(engine)
    
    # Create indexes
    print("📇 Creating indexes...")
    with engine.connect() as conn:
        # Additional indexes for better query performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_messages_created ON chat_messages(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_queries_created ON query_history(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_schema_expires ON schema_cache(expires_at)",
        ]
        
        for idx_sql in indexes:
            try:
                conn.execute(text(idx_sql))
            except Exception as e:
                print(f"  ⚠️ Index may already exist: {e}")
        
        conn.commit()
    
    print("✅ Database initialization complete!")
    return engine


def seed_sample_data(engine):
    """Seed sample data for development/testing."""
    print("🌱 Seeding sample data...")
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Check if data already exists
        existing = session.query(UserPreferences).first()
        if existing:
            print("  ℹ️ Sample data already exists, skipping...")
            return
        
        # Create sample user preferences
        import uuid
        
        sample_user = UserPreferences(
            id=str(uuid.uuid4()),
            user_id="sample_user@example.com",
            default_workspace_id="sample-workspace-id",
            default_dataset_id="sample-dataset-id",
            visualization_preferences={
                "theme": "light",
                "chartColors": ["#3B82F6", "#10B981", "#F59E0B", "#EF4444"],
                "defaultChartType": "bar"
            },
            language="pt-BR"
        )
        session.add(sample_user)
        
        # Create sample chat session
        sample_session = ChatSession(
            id=str(uuid.uuid4()),
            workspace_id="sample-workspace-id",
            dataset_id="sample-dataset-id",
            metadata={"source": "sample_data"}
        )
        session.add(sample_session)
        
        # Create sample messages
        messages = [
            ChatMessage(
                id=str(uuid.uuid4()),
                session_id=sample_session.id,
                role="user",
                content="Mostre o total de vendas por produto",
                metadata={}
            ),
            ChatMessage(
                id=str(uuid.uuid4()),
                session_id=sample_session.id,
                role="assistant",
                content="Aqui está o total de vendas por produto. O produto mais vendido é o Widget A com R$ 15.000 em vendas.",
                metadata={
                    "query_generated": True,
                    "visualization_type": "bar"
                }
            )
        ]
        for msg in messages:
            session.add(msg)
        
        session.commit()
        print("✅ Sample data seeded successfully!")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error seeding data: {e}")
        raise
    finally:
        session.close()


def drop_all_tables(engine):
    """Drop all tables (use with caution!)."""
    print("⚠️ Dropping all tables...")
    Base.metadata.drop_all(engine)
    print("✅ All tables dropped!")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Database initialization script")
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed sample data after initialization"
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop all tables before initialization"
    )
    parser.add_argument(
        "--url",
        type=str,
        help="Override database URL"
    )
    
    args = parser.parse_args()
    
    if args.url:
        os.environ["DATABASE_URL"] = args.url
    
    # Create engine
    engine = create_sync_engine()
    
    # Drop tables if requested
    if args.drop:
        confirm = input("Are you sure you want to drop all tables? (yes/no): ")
        if confirm.lower() == "yes":
            drop_all_tables(engine)
        else:
            print("Aborted.")
            return
    
    # Initialize database
    init_database()
    
    # Seed data if requested
    if args.seed:
        seed_sample_data(engine)
    
    print("\n🎉 Done!")


if __name__ == "__main__":
    main()
