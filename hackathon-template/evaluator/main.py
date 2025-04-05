from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from models.submissions import Base
from api.submissions import router as submissions_router
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./evaluation.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Hackathon Evaluator API",
    description="API for evaluating and tracking team submissions",
    version="1.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(submissions_router, prefix="/api")

@app.get("/")
async def root():
    return {"message": "Hackathon Evaluator API"}
