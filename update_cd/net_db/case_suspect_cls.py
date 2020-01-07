from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence
from sqlalchemy.orm import relationship
from net_db import Base
from net_db.suspect_cls import Suspect
from net_db.case_cls import Case


class CaseSuspect(Base):
    """Table linking cases and suspects (many to many)."""
    __tablename__ = "case_suspects"
    id = Column(Integer, Sequence('case_suspect_id_seq'), primary_key=True)
    case_id = Column(Integer, ForeignKey('cases.id'))
    suspect_id = Column(Integer, ForeignKey('suspects.id'))
    suspect_case_id = Column(String(20))

    case = relationship(Case, primaryjoin=case_id == Case.id)
    suspect = relationship(Suspect, primaryjoin=suspect_id == Suspect.id)

    def __repr__(self):
        return "<CaseSuspect(suspect_id='%d', case_id='%d', suspect_case_id='%d')>" % (
            self.suspect_id, self.case_id, self.suspect_case_id)

