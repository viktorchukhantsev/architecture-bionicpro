from sqlalchemy import Column, Integer, UnicodeText, Date, Numeric, DateTime
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from database import Base


class Report(Base):
    __tablename__ = "reports"

    user_id = Column(Integer, primary_key=True, index=True)
    user_name = Column(UnicodeText, index=True)
    customer_id = Column(Integer, index=True)
    full_name = Column(UnicodeText)
    prosthesis_model = Column(UnicodeText, index=True)
    prosthesis_serial = Column(UnicodeText, unique=True)
    activation_date = Column(Date)
    last_usage_date = Column(Date)
    days_active = Column(Integer, default=0)
    avg_daily_usage_hours = Column(Numeric, default=0)
    total_sessions = Column(Integer, default=0)
    avg_session_duration = Column(Integer, default=0)
    movements_count = Column(Integer, default=0)
    grip_usage_count = Column(Integer, default=0)
    battery_avg_duration = Column(Numeric, default=0)
    error_count = Column(Integer, default=0)
    common_issue = Column(UnicodeText)
    calibration_status = Column(UnicodeText, default='normal')
    satisfaction_score = Column(Integer)
    therapy_compliance = Column(Numeric)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class ReportSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Report
        load_instance = True
