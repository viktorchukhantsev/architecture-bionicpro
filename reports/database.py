from sqlalchemy import create_engine
from decouple import config
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = config("DATABASE_URL")

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
