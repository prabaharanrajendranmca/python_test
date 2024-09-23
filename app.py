import asyncio
import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from typing import List, Optional
import logging

# Initialize FastAPI app
app = FastAPI()

# Database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Models for database
class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    email = Column(String)
    phone = Column(String)

class Campaign(Base):
    __tablename__ = "campaigns"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    budget = Column(Float)

Base.metadata.create_all(bind=engine)

# Pydantic models for request validation
class WebhookData(BaseModel):
    event: str
    data: dict

class TaskResponse(BaseModel):
    task_id: int
    status: str

# In-memory task tracker
tasks = {}

# External API URLs
CRM_API_URL = "https://challenge.berrydev.ai/api/crm/customers"
MARKETING_API_URL = "https://challenge.berrydev.ai/api/marketing/campaigns"

# API Key
API_KEY = "prabaharanrajendranmca"

# Helper to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Async function to fetch data from external APIs
async def fetch_data(url: str, headers: dict):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error(f"Error fetching data: {exc}")
            raise HTTPException(status_code=exc.response.status_code, detail="Failed to fetch data")

# Background task to sync CRM data
async def sync_crm_data(db_session):
    page = 0
    while True:
        url = f"{CRM_API_URL}?limit=100&offset={page * 100}"
        data = await fetch_data(url, headers={'X-API-Key': API_KEY})
        if not data['customers']:
            break
        for customer in data['customers']:
            db_customer = Customer(id=customer['id'], name=customer['name'], email=customer['email'], phone=customer['phone'])
            db_session.add(db_customer)
        db_session.commit()
        page += 1
    logger.info("CRM data sync complete")

# Background task to sync Marketing data
async def sync_marketing_data(db_session):
    data = await fetch_data(MARKETING_API_URL, headers={'X-API-Key': API_KEY})
    for campaign in data['campaigns']:
        db_campaign = Campaign(id=campaign['id'], title=campaign['title'], budget=campaign['budget'])
        db_session.add(db_campaign)
    db_session.commit()
    logger.info("Marketing data sync complete")

# API Endpoints

@app.post("/webhook")
async def webhook(data: WebhookData, db: SessionLocal = next(get_db())):
    if data.event == "customer_update":
        # Handle customer updates
        pass  # Implement logic to handle webhook
    return {"status": "success"}

@app.get("/data")
async def get_data(offset: int = 0, limit: int = 10, db: SessionLocal = next(get_db())):
    query = select(Customer).offset(offset).limit(limit)
    customers = db.execute(query).scalars().all()
    return customers

@app.get("/sync/{source}")
async def sync_data(source: str, background_tasks: BackgroundTasks, db: SessionLocal = next(get_db())):
    if source == "crm":
        task_id = len(tasks) + 1
        tasks[task_id] = "running"
        background_tasks.add_task(sync_crm_data, db)
        return {"task_id": task_id, "status": "CRM sync started"}
    elif source == "marketing":
        task_id = len(tasks) + 1
        tasks[task_id] = "running"
        background_tasks.add_task(sync_marketing_data, db)
        return {"task_id": task_id, "status": "Marketing sync started"}
    else:
        raise HTTPException(status_code=400, detail="Unknown data source")

@app.get("/tasks")
async def list_tasks():
    return [{"task_id": task_id, "status": status} for task_id, status in tasks.items()]

@app.post("/tasks/cancel")
async def cancel_task(task_id: int):
    if task_id in tasks and tasks[task_id] == "running":
        tasks[task_id] = "cancelled"
        return {"status": f"Task {task_id} cancelled"}
    raise HTTPException(status_code=404, detail="Task not found or already completed")

# For Testing Purposes: Unit and Integration Testing
@app.on_event("startup")
async def startup_event():
    logger.info("Application startup complete")

# The code can be run using:
# `uvicorn filename:app --reload`

