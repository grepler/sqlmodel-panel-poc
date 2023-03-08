from typing import Optional, List, DefaultDict
from sqlite_utils import Database
from sqlite_utils.db import Table, Column
import pydantic_panel
import sqlite_utils

from collections import defaultdict

from helpers import DynamicAttrDefaultDictList, OptionedList

from sqlalchemy import event, and_
from sqlalchemy.orm import validates, object_session, relationship
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlmodel import Field, Session, SQLModel, Relationship, create_engine, select


from typing import Optional, Union, List
import pathlib
from sqlmodel import create_engine, SQLModel, Field, Session, select, Relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import reconstructor
from pydantic import Extra
from sqlite3 import OperationalError
from sqlite_utils import Database

import logging

logger = logging.getLogger(__name__)
logger.setLevel(5)

  
class DataFieldRoleLink(SQLModel, table=True):
    __tablename__ = "__beed_datafieldrolelink"
    field_id: Optional[int] = Field(
        default=None, foreign_key="__beed_datafield.id", primary_key=True
    )
    role_id: Optional[int] = Field(
        default=None, foreign_key="__beed_datarole.id", primary_key=True
    )
    priority: Optional[int] = Field(default=0, index=True)


class DataRole(SQLModel, table=True):
    __tablename__ = "__beed_datarole"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)
    is_unique: bool = Field(default=False, 
                            description="If this is True, then there can only be one DataField which has this Role in a given dataset."
                           )
    
    fields: List["DataField"] = Relationship(back_populates="roles", link_model=DataFieldRoleLink)

    beediscovery_id: int = Field(default=1, index=True, foreign_key="__beediscovery.id")
    beediscovery: Optional["BeeDiscovery"] = Relationship(back_populates="roles")

    def __repr__(self):
        return f"DataRole: {self.name}, used by DataFields {[x.name for x in self.fields]}"


class Dataset(SQLModel, table=True):
    __tablename__ = "__beed_dataset"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    table: str = Field(index=True, description="the name of the sqlite table containing this dataset")
    fields: List["DataField"] = Relationship(
        back_populates="dataset", 
        sa_relationship_kwargs={
            'cascade': 'all, delete-orphan', 
            'lazy':'joined', 
            },
        )

    beediscovery_id: int = Field(default=1, index=True, foreign_key="__beediscovery.id")
    beediscovery: Optional["BeeDiscovery"] = Relationship(back_populates="datasets")
    
    #: tracks the ArangoDB Graph identifier.
    _graph: str
    #: tracks the ArangoDB Collection identifier.
    _vertex_collection: str

    #: tracks the ArangoDB Edge Collection
    _edge_collection: str

    
    def __repr__(self):
        return f"Dataset: {self.name}, {self.t.count if self.beediscovery and self.t.exists() else 0} records with DataFields {[x.name for x in self.fields]}"
    
    def __str__(self):
        return self.__repr__()


    @property
    def roles_available(self):
        """
        Return a list of DataRoles which are available (i.e., can be currently assigned to new fields in the dataset.
        If they are flagged as unique roles, then they are not a part of this list if they have already been assigned.
        """
        
        # get all roles

        # TODO: THIS NEEDS WORK, not working correctly
        session = Session.object_session(self)

        roles_available = list()

        statement = select(DataRole).filter(DataRole.is_unique==False)
        roles_non_unique = session.exec(statement)
        for role in roles_non_unique:
            logger.debug(f'available non_unique roles: {role=}')
            if role not in roles_available:
                roles_available.append(role)

        statement = select(DataRole).where(~DataRole.fields.any(DataField.id == self.id)).where(DataRole.is_unique==False)
        roles_unique_no_fields = session.exec(statement)
        for role in roles_unique_no_fields:
            logger.debug(f'available unique role: {role=}')
            if role not in roles_available:
                roles_available.append(role)

        return sorted(roles_available, key=id, reverse=True)

    

    @property
    def roles(self) -> dict[str:"DataField"]:
        # TODO: clean this up with a better SQL select statement        
        role_mapping = dict()

        for field in self.fields:
            for role in field.roles:
                logger.debug(f'assessing {role=} for {field=}')
                if role.name in role_mapping:
                    if not isinstance(role_mapping[role.name], list):
                        convert_to_list = list()
                        convert_to_list.append(role_mapping[role.name])
                        role_mapping[role.name] = convert_to_list
                    role_mapping[role.name].append(field)
                else:
                    role_mapping[role.name] = field
                
        return role_mapping
    
    @property
    def r(self) -> dict[str:"DataField"]:
        return DynamicAttrDefaultDictList(self.roles)
    
    @property
    def ra(self) -> dict[str:"DataField"]:
        return DynamicAttrDefaultDictList(self.roles_available, lambda x: x.name.replace(' ','').replace('-','_'))
    
    @property
    def f(self) -> dict[str:"DataField"]:
        return DynamicAttrDefaultDictList(self.fields, lambda x: x.name.replace(' ','').replace('-','_'))

    @property
    def t(self) -> Table:
        """
        Return the sqlite_utils table for this Dataset
        """
        return self.beediscovery.db[self.table]

    def datafield(self, 
        **kwargs
        ) -> "DataField":
        """
        return the DataField object(s) which matches the supplied attributes.
        Note: this method structure is very useful because it allows you to fetch a list of matching rows for arbitrary attributes.
        """

        try:
            # print(kwargs)
            where_clauses = [getattr(DataField, k) == v for k,v in kwargs.items()]

            statement = select(DataField).where(and_(*where_clauses))
            # print(statement)
            return self.beediscovery._session.exec(statement).unique().all()

        except Exception as e:
            print(f"Oops, something went wrong with your lookup: {type(e)} \n{e}")



    def sync_columns(self):
        """
        evaluate the referenced table and create DataFields for each column which exists.
        """

        try:

            with self.beediscovery._session.begin_nested():
                
                matched_pairs = list()
                matched_datafields = list()
                extra_datafields = list()

                for column in self.t.columns:
                    # print(f'here is some column info: {column}')
                    column: Column
                    
                    db_fields = self.datafield(db_name=column.name, db_is_primary_key=column.is_pk, dataset=self)

                    if len(db_fields) > 1:
                        # print("looks like there are lots of fields which match, oops")
                        matched_pairs.append((column, db_fields))

                    elif len(db_fields) == 1:
                        # print(f'There is one match for these attributes! Great, use this as the match.')
                        matched_pairs.append((column, db_fields))
                        matched_datafields.append(db_fields[0])

                    else:
                        field = DataField(dataset=self, name=column.name, db_name=column.name, db_type=column.type, db_default_value=column.default_value, db_is_primary_key=column.is_pk)

                        print(f"no match in the database, creating a new DataField for the Table's column:\n{field}")
                
                # look for any DataFields which were not matched against in the Table:
                for field in self.fields:
                    # print(f'examining field: {field}')
                    if field not in matched_datafields:
                        print(f'wait, we found a field that did not have a match: {field=}')
                        extra_datafields.append(field)

                return dict(matched_datafields=matched_datafields, extra_datafields=extra_datafields)

        except Exception as e:
            print(f"shoot, it didn't work: {e}")



class DataField(SQLModel, table=True):
    __tablename__ = "__beed_datafield"
    id: Optional[int] = Field(default=None, primary_key=True)

    #: The original name of the field in the data source, and in the database.
    #: _Should not be changed_ after it is set.
    #: This is because it is used as a reference for all queries in the database.
    #: Must be unique within the Dataset.
    db_name: str | None

    #: The type of field in the database table.
    #: _Should not be changed_ after it is set.
    db_type: str | None
    
    #: The default field value in the database table.
    db_default_value: str | None

    #: The default field value in the database table.
    db_is_primary_key: bool | None


    #: Friendly name of the field, for display in UI elements. Can be changed, but must be unique within the Dataset.
    #: the friendly name of the field, can be changed on the fly without affecting access to the data.
    #: Defaults to the value in `name_db`, using the function in `helpers.orm_helpers` module.
    name: str = Field(index=True)
    
    #: Array State
    #: used to determine if the field is a list of values.
    #: If `is_array` is TRUE, then you should specify an `array_delimiter`.
    is_json: bool = Field(default=False)

    priority: Optional[int] = Field(default=None, index=True)
    
    #: A short written description of the field.
    description: str | None = Field(default=None, index=True)

    dataset_id: int | None = Field(default=None, foreign_key="__beed_dataset.id")
    dataset: Optional[Dataset] = Relationship(back_populates="fields")
    
    
    #: DataRoles list
    #: When <someobject>.role is set to a string, a new, anonymous Role() object
    #: is created with that name and assigned to <someobject>._role.   However it
    #: does not have a database id.  This will have to be fixed later when the
    #: object is associated with a Session where we will replace this
    #: Role() object with the correct one.
    
    #: We are using the UniqueObject pattern/recipe here, because we want the Roles to be globally unique.
    #: https://github.com/sqlalchemy/sqlalchemy/wiki/UniqueObjectValidatedOnPending
    roles: OptionedList["DataRole"] = Relationship(back_populates="fields", link_model=DataFieldRoleLink,
    sa_relationship_kwargs={
            # 'cascade': 'all, delete-orphan', 
            'lazy':'joined', 
            'collection_class': lambda: OptionedList(
                optionspath='_sa_adapter.owner_state.object.roles_available',
                parentpath='_sa_adapter.owner_state.object'),
            },
            )
    

    #: the same 'field_roles'->'role' proxy as in the basic dictionary example.
    #: This maps the Role_Value to the Role Name
    #: _r:  List["DataRole"] = association_proxy(
    #:         'roles',
    #:         'name',
    #:         creator=lambda role: DataRole(name=name)
    #:     )
    #: Association Proxies aren't working, so we will define an hybrid property to handle this.
    @property
    def r(self) -> list[DataRole]:
        return [x.name for x in self.roles]
    
    @property
    def roles_available(self):
        """
        Return the list of DataRoles which can be applied to this DataField.
        """
        available = self.dataset.roles_available.copy()
        available.extend([x for x in self.roles if x not in available])

        return available
    

    def first_nonblank(self, n:int=1):
        """
        Return the first n non-blank values in the field.
        """
        results = self.dataset.t.rows_where(
            f'{self.db_name} IS NOT NULL AND {self.db_name} != ""', 
            select=self.db_name,
            limit=n)

        return [x[self.db_name] for x in results]


    @validates("roles")
    def _validate_role(self, key, value):
        """Receive the event that occurs when <someobject>._role is set.
        If the object is present in a Session, then make sure it's the Role
        object that we looked up from the database.
        
        Otherwise, do nothing and we'll fix it later when the object is
        put into a Session.
        """
        print(F"validating the 'role', changed to {key, value, type(value)}")
        logger.debug(F"validating the 'role', changed to {key, value, type(value)}")
        
        sess = object_session(self)
        if sess is not None:
            logger.debug(f"running _setup_role on {value}")
            return _setup_role(sess, value)
        else:
            logger.debug(f'no session for this object ({value}) yet, not validating')
            return value 

    
    def __repr__(self):
        return f"DataField: {self.name}, in dataset {self.dataset.name if self.dataset else ''}" + (f" with role(s): {[x.name for x in self.roles]}" if self.roles else "")

    def __str__(self):
        return f"DataField: {self.name}, in dataset {self.dataset.name if self.dataset else ''}" + (f" with role(s): {[x.name for x in self.roles]}" if self.roles else "")
        

@event.listens_for(Session, "transient_to_pending")
def _validate_role(session, object_):
    """Receive the HasRole object when it gets attached to a Session to correct
    its Role object.
    Note that this discards the existing Role object.
    """

    if isinstance(object_, DataRole):  #
        print(f"it's a DataRole: {object_}")
        logger.debug("it's a DataRole")
        object_: DataRole
        if object_.id is not None:
            logger.debug(f"object.id attr is: {repr(object_.id)}, type: {type(object_.name)}")
            logger.debug("# something set object_.role = DataRole()")
            if object_.id is None:
                logger.debug("# and it has no database id")
                logger.debug(repr(object_))
                # the id-less Role object that got created
                old_role = object_._role
                # make sure it's not going to be persisted.
                if old_role in session:
                    session.expunge(old_role)
                    del old_role

                object_._role = _setup_role(session, object_._role)
    # else:
        # print(f'it was a {type(object_)}')

                
                
def _setup_role(session, role_object: DataRole):
    """Given a Session and a Role object, return
    the correct Role object from the database.
    """

    with session.no_autoflush:
        return session.query(DataRole).filter_by(name=role_object.name).one() 






class BeeDiscovery(SQLModel, table=True):
    """
    The BeeDiscovery object is the main instance for an interactive data exploration session.
    
    it has a reference to the sqlite database that holds all of it's data and configurations.
    
    
    """
    __tablename__ = "__beediscovery"


    @classmethod
    def load(cls, beed_file: str):
        """
        Primary entryway to creating new BeeDiscovery projects.
        >>> bdb = BeeDiscovery.load('name_of_file.beedb')
        """
        
        engine = create_engine(f'sqlite:///{beed_file}')
        session = Session(engine)
        
        id_ = dict()
        id_['id'] = 1
            
        try:
            SQLModel.metadata.create_all(engine)
            instance = session.query(cls).filter_by(**id_).one()

        except NoResultFound:
            try:
                with session.begin_nested():
                    instance = cls(beed_file,
                                   engine=engine, 
                                   session=session
                                  )
                    session.add(instance)
                session.commit()

            except IntegrityError:
                instance = session.query(cls).filter_by(**id_).one()
                    
        instance._engine = engine
        instance._session = session
        return instance
    
    
    id: Optional[int] = Field(default=1, primary_key=True)
    name: Optional[str] = Field(default=None)
    beed_file_path: Optional[str]
    test_option: Optional[str]
    
    datasets: List[Dataset] = Relationship(back_populates="beediscovery", 
                                             sa_relationship_kwargs={'cascade': 'all, delete-orphan', 'lazy':'joined'}
                                            )
    roles: List[DataRole] = Relationship(back_populates="beediscovery", 
                                             sa_relationship_kwargs={'lazy':'joined'}
                                            )

    
    
    
    class Config:
        extra = Extra.allow # or 'allow' str
    
    def __init__(self, beed_file: str = None, name: str = None, engine=None, session: Session=None):
        
        super().__init__()

        self.id : int = 1
        self.name: str = name if name else beed_file
        self.beed_file_path: str = beed_file
        
        self._engine = create_engine(f'sqlite:///{self.beed_file_path}')
        self._session = Session(self._engine)
        
        self.__init_on_load()
        
    @reconstructor
    def __init_on_load(self):
        self.__sqlite_utils_db = Database(self.beed_file_path)


    def __repr__(self):
        return f"BeeDiscovery: {self.beed_file_path}\n" + "\n".join([str(x) for x in self.datasets])
    @property
    def db(self):
        return self.__sqlite_utils_db

    def dataset(self, dataset_name: str, **kwargs) -> Dataset:
        """
        # TODO: Fix docstring
        Return a table object, optionally configured with default options.
        See :ref:`reference_db_table` for option details.
        :param table_name: Name of the table
        """

        with self._session.begin_nested():
            try:
                table = kwargs['table'] if 'table' in kwargs else dataset_name
                dataset = Dataset(name=dataset_name, table=table, beediscovery=self, **kwargs)
            except:
                print(f"Error adding {dataset_name}, skipped.")
            
        return dataset

    def __getitem__(self, dataset_name: str) -> Dataset:
        """
        # TODO: Fix docstring
        ``bee[dataset_name]`` returns a :class:`.Dataset` object for the dataset with the specified name.
        If the Dataset does not exist yet it will be created the first time data is inserted into it.
        :param dataset_name: The name of the dataset
        """

        try:
            statement = select(Dataset).where(Dataset.name == dataset_name)
            return self._session.exec(statement).unique().one()
        except NoResultFound:
            new_dataset = self.dataset(dataset_name)
            print(new_dataset)
            return new_dataset


    @property
    def d(self) -> dict[str:Dataset]:
        return DynamicAttrDefaultDictList(self.datasets, lambda x: x.name.replace(' ','').replace('-','_'))
        
    

# Lifecycle Events
#@event.listens_for(Dataset.fields, 'remove')
def receive_persistent_to_deleted_datafield(dataset, datafield, initiator):
    """
    Dangerous - drops column data regardless of transaction completion status
    """
    print(f'something was removed from a relationship: {dataset=}, {datafield=}')
    print(f'WARNING: dropping the column: {datafield.db_name}')
    dataset.t.transform(drop={datafield.db_name})
    dataset.sync_columns()

# Lifecycle Events
#@event.listens_for(BeeDiscovery.datasets, 'remove')
def receive_persistent_to_deleted_dataset(beediscovery, dataset, initiator):
    """
    Dangerous - drops entire tables regardless of transaction completion status
    """
    print(f'something was removed from a relationship: {beediscovery=}, {dataset=}')
    if dataset.t.exists():
        print(f'WARNING: dropping the table: {dataset.table}')
        dataset.t.drop()
