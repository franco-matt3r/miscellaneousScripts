import boto3
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Integer, Date, VARCHAR, UniqueConstraint, and_
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
import sys

_, PREFIX = sys.argv
K3Y_ID = PREFIX.split('/')[1]
RAW_BUCKET = 'haoming-canserver-raw-test'
PARSED_BUCKET = 'haoming-canserver-test'
EVENT_BUCKET = 'haoming-canserver-event-test'
# RAW_BUCKET = 'matt3r-canserver-raw-us-west-2'
# PARSED_BUCKET = 'matt3r-canserver-us-west-2'
# EVENT_BUCKET = 'matt3r-canserver-event-us-west-2'
HOST = 'data-catalog.cbbarg1ot9rc.us-west-2.rds.amazonaws.com'
PORT = 5432
USERNAME = 'postgres'
PASSWORD = 'DHF.yep5uke_tfq-nbt'
DB = 'postgres'
DB_URI = f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'

s3_client = boto3.client('s3')
engine = create_engine(DB_URI)
Base = declarative_base()
Session = sessionmaker(bind=engine)

class TeslaDashcam(Base):
    __tablename__ = 'tesla_dashcam'
    id = Column(Integer, primary_key=True)
    k3y_id = Column(VARCHAR(20), nullable=False)
    date = Column(Date, nullable=False)
    updated_time = Column(Integer)
    created_time = Column(Integer)
    meta_data = Column(JSONB)
    __table_args__ = (
        UniqueConstraint('k3y_id', 'date', name='k3y_with_date'),
        {'extend_existing': True}
    )

class TeslaApi(Base):
    __tablename__ = 'tesla_api'
    id = Column(Integer, primary_key=True)
    vehicle_id = Column(VARCHAR(20), nullable=False)
    date = Column(Date, nullable=False)
    updated_time = Column(Integer)
    created_time = Column(Integer)
    meta_data = Column(JSONB)
    __table_args__ = (
        UniqueConstraint('vehicle_id', 'date', name='vehicle_with_date'),
        {'extend_existing': True}
    )

class CanServer(Base):
    __tablename__ = 'can_server'
    id = Column(Integer, primary_key=True)
    k3y_id = Column(VARCHAR(20), nullable=False)
    org_id = Column(VARCHAR(20), nullable=False)
    date = Column(Date, nullable=False)
    parsed_field_version = Column(Integer, nullable=False)
    last_updated_time = Column(Integer)
    created_time = Column(Integer)
    meta_data = Column(JSONB)
    __table_args__ = (
        UniqueConstraint('k3y_id', 'date', name='k3y_to_date'),
        {'extend_existing': True}
    )

# Delete records based on k3y_id, each time can only delete one record
def postgres_delete_all_entries_by_k3y_id():
    session = Session()
    entries = session.query(CanServer).filter(CanServer.k3y_id == K3Y_ID).all()
    for obj in entries:
        session.delete(obj)
    session.commit()
    session.close()
    print("Deleted all postgres entries with k3y_id ", K3Y_ID, ":", entries)

if __name__ == "__main__":
    postgres_delete_all_entries_by_k3y_id()
