from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Sequence
from net_db import Base
from sqlalchemy.orm import relationship


class Suspect(Base):
    """Table containing suspect names and links metadata."""
    __tablename__ = "suspects"
    #__table_args__ = {'sqlite_autoincrement': True}

    id = Column(Integer, Sequence('sus_id_seq'), primary_key=True)
    name = Column(String(50))
    first_degree_links = Column(Integer)
    second_degree_links = Column(Integer)
    first_degree_case_links = Column(Integer)
    second_degree_case_links = Column(Integer)

    account = relationship("Account", cascade="all, delete-orphan")
    edge = relationship("Edge", cascade="all, delete-orphan",)

    def __repr__(self):
        return "<Suspect(name='%s', first_degree_case_links='%d', second_degree_case_links='%d', \
        first_degree_links='%d', second_degree_links='%d')>" % (
            self.name, self.first_degree_case_links, self.second_degree_case_links,
            self.first_degree_links, self.second_degree_links)
