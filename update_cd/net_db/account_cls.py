from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence
from sqlalchemy.orm import relationship
from net_db import Base
from net_db.suspect_cls import Suspect


class Account(Base):
    """Table containing data on accounts, including phone numbers and facebook pages."""
    __tablename__ = "accounts"
    # __table_args__ = {'extend_existing': True}
    id = Column(Integer, Sequence('acc_id_seq'), primary_key=True)
    account_name = Column(String(50), nullable=False)
    account_label = Column(String(50))
    account_type_id = Column(Integer, ForeignKey('account_types.id'))
    suspect_id = Column(Integer, ForeignKey('suspects.id'))
    suspect = relationship(Suspect, primaryjoin=suspect_id == Suspect.id)

    def __repr__(self):
        return "<Account(account_name='%s', account_label='%s', account_type_id'%d')>" % (
            self.account_name, self.account_label, self.account_type_id)


Suspect.accounts = relationship(
    "Account", order_by=Account.id, back_populates="suspect")


class AccountType(Base):
    """Table for the types of connections between accounts."""
    __tablename__ = "account_types"
    id = Column(Integer, primary_key=True)
    account_type = Column(String(20), nullable=False)

    def __repr__(self):
        return "<AccountType(account_type='%s')>" % self.account_type
