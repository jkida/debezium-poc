from sqlalchemy import (
    create_engine,
    update,
    Table,
    Column,
    ForeignKey,
    Integer, String,
    alias
)
from sqlalchemy.orm import (
    sessionmaker,
    relationship
)

engine = create_engine('postgres://user:pass@postgres/poc', echo=True)

from sqlalchemy.ext.declarative import declarative_base

ModelBase = declarative_base()


class User(ModelBase):
    "Represents a user with login access"

    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String)


class AccountContactsXREF(ModelBase):
    "Many To Many XREF"
    __tablename__ = 'xref_accounts_contacts'
    account_id = Column(Integer, ForeignKey('accounts.id'), primary_key=True)
    person_id = Column(Integer, ForeignKey('persons.id'), primary_key=True)


class Company(ModelBase):
    "Represents a Company"
    __tablename__ = 'accounts'

    id = Column(Integer, primary_key=True)
    account_number = Column(String)
    name = Column(String)
    website = Column(String)
    address = Column(String)
    phone = Column(String)

    sold_by_id = Column(Integer, ForeignKey('users.id'))
    sold_by = relationship("User", foreign_keys=sold_by_id)

    ceo_id = Column(Integer, ForeignKey('persons.id'))
    ceo = relationship("Person", foreign_keys=ceo_id)

    primary_contact_id = Column(Integer, ForeignKey('persons.id'))
    primary_contact = relationship('Person', foreign_keys=primary_contact_id)

    contacts = relationship("Person", secondary=AccountContactsXREF.__table__)


class Person(ModelBase):
    "Represents a Company Contact"
    __tablename__ = 'persons'  # :)

    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)
    phone = Column(String)

    companies = relationship(
        "Company", secondary=AccountContactsXREF.__table__)


Session = sessionmaker(bind=engine)


def initdb():
    ModelBase.metadata.create_all(engine)


def dummy_data():
    from faker import Faker
    from itertools import zip_longest

    fake = Faker()

    companies = [Company(name=fake.company(),
                         website=fake.url(),
                         address=fake.address(),
                         phone=fake.phone_number())
                 for _ in range(1000)]

    persons = [Person(name=fake.name(),
                      email=fake.email(),
                      phone=fake.phone_number())
               for _ in range(12000)]

    contact_persons = list(zip_longest(*[iter(persons)]*3))

    for company in companies:
        company.contacts = list(contact_persons.pop())
        company.ceo = company.contacts[0]
        company.primary_contact = company.contacts[1]

    db = Session()
    db.add_all(companies)
    db.add_all(persons)
    db.commit()


ceo = alias(Person)
primary_contact = alias(Person)
accounts_view_select = (
    Session().query(
        Company.id,
        Company.name.label('company_name'),
        Company.phone.label('company_phone'),
        Company.website.label('website'),
        ceo.id.label('ceo_id'),
        ceo.name.label('ceo'),
        primary_contact.id.label('primary_contact_id'),
        primary_contact.name.label('primary_contact_name'),
        primary_contact.phone.label('primary_contact_phone')
        )
    .outerjoin(ceo, ceo.id == Company.ceo_id)
    .outerjoin(primary_contact, primary_contact.id == Company.primary_contact_id)


def init_denormalized_view():
    """
    Creates a Denormalized view of account table

    id |        company_name        |    company_phone    |           website           | ceo_id |       ceo        | primary_contact_id | primary_contact_name | primary_contact_phone
    ----+----------------------------+---------------------+-----------------------------+--------+------------------+--------------------+----------------------+-----------------------
    1 | Bates-Wood                 | 708-356-1107x4884   | https://saunders.biz/       |      1 | Charlotte Beck   |                  2 | James Whitney        | +02(0)8018988608
    2 | Fields, Brandt and Douglas | (785)729-1174x18662 | http://morris.com/          |      4 | Angela Mills DDS |                  5 | Diana Moran          | 1-323-500-6831
    3 | Myers-York                 | +36(7)0769490360    | http://mejia-guzman.com/    |      7 | April Harrell    |                  8 | Ian Evans            | (567)994-0018x555
    4 | Peck-Roach                 | 1-430-020-0722x1310 | http://www.smith-bauer.net/ |     10 | Abigail Reed     |                 11 | Terry Perry MD       | (897)978-4509
    5 | Ruiz-Lopez                 | +54(7)0554389882    | http://www.deleon.com/      |     13 | Stacie Jenkins   |                 14 | Natasha Malone       | 359.847.4975x484

    """
    session=Session()
    session.execute("""
        CREATE TABLE accounts_view AS
        SELECT accounts.id,
            accounts.name as company_name,
            accounts.phone as company_phone,
            accounts.website,
            ceo_person.id as ceo_id,
            ceo_person.name as ceo,
            primary_contact_person.id as primary_contact_id,
            primary_contact_person.name as primary_contact_name,
            primary_contact_person.phone as primary_contact_phone
        FROM accounts
        LEFT JOIN persons ceo_person
            ON ceo_person.id = accounts.ceo_id
        LEFT JOIN persons primary_contact_person
            ON primary_contact_person.id = accounts.primary_contact_id
    """)
    session.execute("CREATE INDEX idx_accounts_view_id on accounts_view (id)")
    session.execute(
        "CREATE INDEX idx_accounts_view_ceo_id on accounts_view (ceo_id)")
    session.execute(
        "CREATE INDEX idx_accounts_view_primary_contact_id on accounts_view (primary_contact_id)")
    session.commit()


account_view=Table(
    'accounts_view', ModelBase.metadata,
    Column('id', Integer, index=True),
    Column('company_name', String),
    Column('company_phone', String),
    Column('website', String),
    Column('ceo_id', Integer, index=True),
    Column('ceo', String),
    Column('primary_contact_id', Integer, index=True),
    Column('primary_contact_name', String),
    Column('primary_contact_phone', String)
)

account_view_mapping={
    'name': ['company_name'],
    'phone': ['company_phone'],
    'website': ['website']
}

person_view_mapping={
    'name': ['ceo', 'primary_contact_name'],
    'phone': ['primary_contact_phone']
}

def update_denormalized_view(db, pk, table, envelope):
    """
    Args:
        pk (dict): Primary key of database table
        envelope (dict): Debezium Envelope containing op, before, after, source, ts_ms keys.
    """

    before=envelope['before']
    after=envelope['after']
    op=envelope['op']

    account_columns=set(['name', 'phone', 'website'])
    person_columns=set(['name', 'phone'])

    if op == 'u' and before:  # Update
        if table == 'accounts':
            print("Updating Account\n")
            print(envelope)
            diff_acct_keys=[
                key for key in after if key in account_columns and after[key] != before[key]]
            if diff_acct_keys:
                stmt=(update(account_view)
                        .where(account_view.c.id == pk['payload']['id'])
                        .values({'company_name': after['name'],
                                 'company_phone': after['phone'],
                                 'website': after['website']}))
                db.execute(stmt)
                db.commit()

        elif table == 'persons':
            print("Updating Person")
            diff_person_keys=[
                key for key in after if key in person_columns and after[key] != before[key]]
            if diff_person_keys:
                stmt=(update(account_view)
                        .where(account_view.c.primary_contact_id == pk['payload']['id'])
                        .values({'primary_contact_name': after['name'],
                                 'primary_contact_phone': after['phone']})
                stmt=(update(account_view)
                        .where(account_view.c.ceo_id == pk['payload']['id'])
                        .values({'ceo_name': after['name']},
                db.execute(stmt)
                db.commit()
