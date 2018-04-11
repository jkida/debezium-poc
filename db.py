import json
from sqlalchemy import (
    create_engine,
    update,
    Table,
    Column,
    ForeignKey,
    Integer, String,
    alias,
    or_
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

    manager_id = Column(Integer, ForeignKey('users.id'))
    manager = relationship('User', remote_side=id)

class Company(ModelBase):
    "Represents a Company"
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True)
    account_number = Column(String)
    name = Column(String)
    website = Column(String)
    address = Column(String)
    phone = Column(String)

    sold_by_id = Column(Integer, ForeignKey('users.id'))
    sold_by = relationship("User", foreign_keys=sold_by_id)

    ceo_id = Column(Integer, ForeignKey('people.id'))
    ceo = relationship("Person", foreign_keys=ceo_id)

    primary_contact_id = Column(Integer, ForeignKey('people.id'))
    primary_contact = relationship('Person', foreign_keys=primary_contact_id)


class Person(ModelBase):
    "Represents a Company Contact"
    __tablename__ = 'people'  # :)

    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)
    phone = Column(String)


Session = sessionmaker(bind=engine)


def initdb():
    ModelBase.metadata.drop_all(engine)
    ModelBase.metadata.create_all(engine)
    session = Session()
    session.execute("ALTER TABLE companies REPLICA IDENTITY FULL")
    session.execute("ALTER TABLE people REPLICA IDENTITY FULL")
    session.execute("ALTER TABLE users REPLICA IDENTITY FULL")
    session.commit()


def dummy_data():
    from faker import Faker
    from itertools import zip_longest, cycle

    fake = Faker()

    companies = [Company(name=fake.company(),
                         website=fake.url(),
                         address=fake.address(),
                         phone=fake.phone_number())
                 for _ in range(10000)]

    people = [Person(name=fake.name(),
                      email=fake.email(),
                      phone=fake.phone_number())
               for _ in range(20000)]

    users = [User(name=fake.name(),
                       email=fake.email())
                  for _ in range(100)]

    db = Session()
    db.add_all(companies)
    db.add_all(people)                      
    db.add_all(users)
    
    manager_and_users = zip_longest(*[iter(users)]*10)
    
    for manager, *rest in manager_and_users:
        for user in rest:
            user.manager = manager

    contact_people = zip_longest(*[iter(people)]*2)
    cycle_users = cycle(users)
    for company in companies:
        contacts = list(next(contact_people))
        company.ceo = contacts[0]
        company.primary_contact = contacts[1]
        company.sold_by = next(cycle_users)
    
    db.commit()

# Denormalized Table View
company_view_denormalized=Table(
    'company_view_denormalized', ModelBase.metadata,
    # Company Attrs
    Column('id', Integer, primary_key=True),
    Column('company_name', String),
    Column('company_phone', String),
    Column('company_website', String),
    
    # Person - CEO
    Column('ceo_name', String),
    
    # Person - Primary Contact
    Column('primary_contact_name', String),
    Column('primary_contact_phone', String),
    
    # User - Sales User
    Column('sold_by_name', String),
    
    # User - Manager of the Sales User
    Column('sold_by_manager_name', String)
)

# Query to populate Denormalized Table View
ceo = alias(Person)
primary_contact = alias(Person)
sold_by = alias(User)
manager = alias(User)
company_view_query = (
    Session().query(
        Company.id,
        Company.name.label('company_name'),
        Company.phone.label('company_phone'),
        Company.website.label('company_website'),
        ceo.c.name.label('ceo_name'),
        primary_contact.c.name.label('primary_contact_name'),
        primary_contact.c.phone.label('primary_contact_phone'),
        sold_by.c.name.label('sold_by_name'),
        manager.c.name.label('sold_by_manager_name')
        )
    .outerjoin(ceo, ceo.c.id == Company.ceo_id)
    .outerjoin(primary_contact, primary_contact.c.id == Company.primary_contact_id)
    .outerjoin(sold_by, sold_by.c.id == Company.sold_by_id)
    .outerjoin(manager, manager.c.id == sold_by.c.manager_id))

def update_denormalized_view_batch(db, chunk):
    ## This method is designed to keep the denormalized company_view_denormalized table synced. With the query that created it.
    insert_company_pkeys = set()
    
    update_company_pkeys = set()
    update_person_pkeys = set()
    update_user_pkeys = set()

    ceo_pkeys = set()
    primary_contact_pkeys = set()
    sold_by_pkeys = set()
    sold_by_manager_pkeys = set()

    # The keys the chunked messages are collected and aggregated to
    # to update the effected rows.
    for chunked in chunk.values():
        for message in chunked:
            data = json.loads(message.value)
            before = data['payload']['before']
            after = data['payload']['after']
            diff = {k:v for k,v in after.items() if before and (after[k] != before[k])}
            tablename = message.topic.split(".")[-1]
            op = data['payload']['op']

            if op == 'c' and tablename == 'companies': # Create CompanyEvent
                # Collect keys that need to be inserted into company_view_denormalized table
                pkey = json.loads(message.key)['payload']['id']
                insert_company_pkeys.add(pkey)
            elif op == 'u': # Update Event
                if tablename == 'companies':
                    pkey = json.loads(message.key)['payload']['id']
                    # This ideally would collect keys only if an attribute was changed - not worried about it
                    update_company_pkeys.add(pkey)
                    
                    # Collect realtionship FK changes
                    if 'ceo_id' in diff:
                        ceo_pkeys.add(diff['ceo_id'])
                    if 'primary_contact_id' in diff:
                        primary_contact_pkeys.add(diff['primary_contact_id'])
                    if 'sold_by_id' in diff:
                        sold_by_pkeys.add(diff['sold_by_id'])

                elif tablename == 'people':
                    # Collect changes of people table that is joined in the view for both ceo and primary_contact relationships.
                    pkey = json.loads(message.key)['payload']['id']
                    update_person_pkeys.add(pkey)
                
                elif tablename == 'users':
                    # Collect changes of users table that is joined in the view for sold_by/manager relationships
                    pkey = json.loads(message.key)['payload']['id']
                    update_user_pkeys.add(pkey)

                    if 'manager_id' in diff:
                        sold_by_manager_pkeys.add(diff['manager_id'])

    ## All Mutations to the denormalize view happen by updating/inserting from a select. The select query
    # filters for only the rows that were changed by using the aggregated pkeys collected in the previous step.
    if insert_company_pkeys:
        ## Insert all Create events into the denormalized view from Company create events.
        stmt = (company_view_denormalized.insert()
            .from_select(company_view_denormalized.columns, # Update Columns
                         company_view_query.filter(Company.id.in_(insert_company_pkeys))))
        db.execute(stmt)
        
    if update_company_pkeys:
        ## Update the company_view_denormalized direct Company attributes from Company update events
        subq_select = company_view_query.filter(Company.id.in_(update_company_pkeys)).subquery()
        stmt = (company_view_denormalized.update()
            .values(company_name=subq_select.c.company_name,
                    company_phone=subq_select.c.company_phone,
                    company_website=subq_select.c.company_website)
            .where(company_view_denormalized.c.id == subq_select.c.id))
        db.execute(stmt)

    if update_person_pkeys:
        # Update the company_view_denormalized Person attributes from people update events
        subq_select = company_view_query.filter(or_(ceo.c.id.in_(update_person_pkeys),
                                                    primary_contact.c.id.in_(update_person_pkeys))).subquery()
        stmt = (company_view_denormalized.update()
            .values(ceo_name=subq_select.c.ceo_name,
                    primary_contact_name=subq_select.c.primary_contact_name,
                    primary_contact_phone=subq_select.c.primary_contact_phone)
            .where(company_view_denormalized.c.id == subq_select.c.id))
        db.execute(stmt)
    
    if update_user_pkeys:
        # Update both the Sold By name and the Manager names
        subq_select = company_view_query.filter(or_(sold_by.c.id.in_(update_user_pkeys),
                                                      manager.c.id.in_(update_user_pkeys))).subquery()
        stmt = (company_view_denormalized.update()
            .values(sold_by_name=subq_select.c.sold_by_name,
                    sold_by_manager_name=subq_select.c.sold_by_manager_name)
            .where(company_view_denormalized.c.id == subq_select.c.id))
        db.execute(stmt)

    # Relationships
    # This is designed to handle the case when a Foreign Key is changed.
    if ceo_pkeys:
        # Update the company_view_denormalized CEO Relationship. From Company.ceo_id change.
        subq_select = company_view_query.filter(ceo.c.id.in_(ceo_pkeys)).subquery()

        stmt = (company_view_denormalized.update()
            .values(ceo_name=subq_select.c.ceo_name)
            .where(company_view_denormalized.c.id == subq_select.c.id))
        db.execute(stmt)
    
    if primary_contact_pkeys:
        # Update the company_view_denormalized PrimaryContact Relationship. From Company.primary_contact_id change
        subq_select = company_view_query.filter(primary_contact.c.id.in_(primary_contact_pkeys)).subquery()

        stmt = (company_view_denormalized.update()
            .values(primary_contact_name=subq_select.c.primary_contact_name,
                    primary_contact_phone=subq_select.c.primary_contact_phone)
            .where(company_view_denormalized.c.id == subq_select.c.id))
            
        db.execute(stmt)
    
    if sold_by_pkeys:
        # When the sold_by user changes it also effects the manager_name column. 
        # You must have an understanding of the full QUERY/JOIN to know which fields could be effected.
        subq_select = company_view_query.filter(sold_by.c.id.in_(sold_by_pkeys)).subquery()

        stmt = (company_view_denormalized.update()
            .values(sold_by_name=subq_select.c.sold_by_name,
                    sold_by_manager_name=subq_select.c.sold_by_manager_name)
            .where(company_view_denormalized.c.id == subq_select.c.id))
        db.execute(stmt)
    
    if sold_by_manager_pkeys:
        # Update the company_view_denormalized sold_by_manager_name column
        subq_select = company_view_query.filter(manager.c.id.in_(sold_by_manager_pkeys)).subquery()

        stmt = (company_view_denormalized.update()
            .values(sold_by_manager_name=subq_select.c.sold_by_manager_name)
            .where(company_view_denormalized.c.id == subq_select.c.id))
        
        db.execute(stmt)
