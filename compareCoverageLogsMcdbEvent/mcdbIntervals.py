import boto3
import os
import json
import time
import pytest
import pandas as pd
import random
import datetime
import numpy as np
from io import BytesIO
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Integer, Date, VARCHAR, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker

# RAW_BUCKET = 'haoming-canserver-raw-test'
# PARSED_BUCKET = 'haoming-canserver-test'
# EVENT_BUCKET = 'haoming-canserver-event-test'
RAW_BUCKET = 'matt3r-canserver-raw-us-west-2'
PARSED_BUCKET = 'matt3r-canserver-us-west-2'
EVENT_BUCKET = 'matt3r-canserver-event-us-west-2'
HOST = 'data-catalog.cbbarg1ot9rc.us-west-2.rds.amazonaws.com'
PORT = 5432
USERNAME = 'postgres'
PASSWORD = 'DHF.yep5uke_tfq-nbt'
DB = 'postgres'
DB_URI = f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'

engine = create_engine(DB_URI)
Session = sessionmaker(bind=engine)
Base = declarative_base()

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
    
def get_intervals(k3y_id, date):
    session = Session() 
    records = session.query(CanServer).filter(CanServer.k3y_id == k3y_id, CanServer.date == date).all()
    session.close()
    intervals = records[0].meta_data['timestamps']
    interval_dict = {}
    interval_dict['data'] = intervals
    with open('./data/mcdbIntervals.json', 'w') as outfile:
        json.dump(interval_dict, outfile)


if __name__ == "__main__":
    # k3y_id = "k3yusb-e731c27b"
    k3y_id = "key123"
    date = "2023-04-18"
    get_intervals(k3y_id, date)