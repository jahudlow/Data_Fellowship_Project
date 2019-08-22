from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence
from net_db import Base
from sqlalchemy.orm import relationship


class Case(Base):
    """Table with Case IDs associated with each suspect."""
    __tablename__ = "cases"
    id = Column(Integer, Sequence('case_id_seq'), primary_key=True)
    case_number = Column(String(8))
    case_date = Column(String(8))

    case_sus = relationship("CaseSuspect", cascade="all, delete-orphan")

    def __repr__(self):
        return "<Case(case_number='%s', case_date='%s')>" % (
            self.case_number, self.case_date)
