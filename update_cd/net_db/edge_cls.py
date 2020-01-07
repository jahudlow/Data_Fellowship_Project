from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence
from net_db import Base
from net_db.suspect_cls import Suspect
from net_db.account_cls import Account


class EdgeType(Base):
    """Table for the types of connections between accounts."""
    __tablename__ = "edge_types"
    # __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True)
    edge_type = Column(String(20), nullable=False)
    edge_type_weight = Column(Integer)

    def __repr__(self):
        return "<EdgeType(edge_type='%s', edge_type_weight='%d')>" % (
            self.edge_type, self.edge_type_weight)


class Edge(Base):
    """Table with all known associations between accounts."""
    __tablename__ = "edges"
    id = Column(Integer, Sequence('edge_id_seq'), primary_key=True)
    source_suspect_id = Column(Integer, ForeignKey('suspects.id'))
    source_account_id = Column(Integer, ForeignKey('accounts.id'))
    target_account_id = Column(Integer, ForeignKey('accounts.id'))
    edge_type_id = Column(Integer, ForeignKey('edge_types.id'))
    edge_direction = Column(Integer)
    edge_combo_id = Column(String(20))

    def __repr__(self):
        return "<Edge(source_suspect_id='%d', source_account_id='%d', target_account_id='%d')>" % (
            self.source_suspect_id, self.source_account_id, self.target_account_id)
