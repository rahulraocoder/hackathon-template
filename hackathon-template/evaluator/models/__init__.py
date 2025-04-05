from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from .submissions import Team, Submission
