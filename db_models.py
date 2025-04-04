from sqlalchemy import create_engine, Column, String, Integer, Float, JSON, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class SubmissionResult(Base):
    __tablename__ = 'submission_results'
    
    id = Column(String, primary_key=True)
    participant_id = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    status = Column(String)
    score = Column(Integer, nullable=False, default=0)
    details = Column(JSON)
    processed_data = Column(JSON)
    processing_time = Column(Float)

# Database setup
engine = create_engine('sqlite:///evaluation.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
