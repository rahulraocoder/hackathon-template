from fastapi import APIRouter, HTTPException, Body, Depends
from datetime import datetime
import json
from sqlalchemy.orm import Session
from models.submissions import Submission, Team, get_db
from core.scoring import calculate_score
from .models import MetricsPayload

router = APIRouter()

@router.post("/submit", response_model=dict)
async def submit_metrics(
    team_key: str = Body(...),
    metrics: MetricsPayload = Body(...),
    db: Session = Depends(get_db)
):
    try:
        # Check submission limit
        team = db.query(Team).filter_by(team_key=team_key).first()
        if team and team.submission_count >= 5:
            raise HTTPException(status_code=400, detail="Submission limit reached")

        # Convert metrics to dict and calculate score
        metrics_dict = metrics.dict()
        score = calculate_score(metrics_dict)

        # Create submission record
        submission = Submission(
            team_key=team_key,
            metrics=json.dumps(metrics_dict),
            score=score,
            status='completed',
            timestamp=datetime.utcnow()
        )
        db.add(submission)

        # Update team counters
        if not team:
            team = Team(team_key=team_key)
            db.add(team)
        team.submission_count = (team.submission_count or 0) + 1
        team.last_submission = datetime.utcnow()
        if not team.best_score or score > team.best_score:
            team.best_score = score

        db.commit()
        return {
            "status": "success",
            "score": score,
            "submissions_remaining": 5 - team.submission_count
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/scores", response_model=list)
async def get_scores(db: Session = Depends(get_db)):
    try:
        teams = db.query(Team).all()
        return [
            {
                "team_key": t.team_key,
                "best_score": t.best_score,
                "last_submission": t.last_submission.isoformat() if t.last_submission else None
            }
            for t in teams
        ]
    finally:
        db.close()
