from collections import UserDict, UserList
from typing import Callable

import logging

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

class DynamicAttrDefaultDictList(UserDict):
    """
    A Test Object which was a proof of concept for dynamic auto-completion of datset roles.
    
    Inspired by: 
    https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000665110-auto-completion-for-dynamic-module-attributes-in-python
    
    
    """
    
    
    def __setitem__(self, key:str, value):
        
        key = key.replace('-', '_').replace(' ','')
        
        if key in self.data:
            if not isinstance(self.data[key], list):
                convert_to_list = list()
                convert_to_list.append(self.data[key])
                self.data[key] = convert_to_list
            self.data[key].append(value)
        else:
            super().__setitem__(key, value)
    

    
    
    def __init__(self, mapping: dict | list, attribute_func=None):
        """
        Provide an existing mapping, or, if a list is provided, 
        mapping: a mapping of dynamic attribute names to return objects, or a list of return objects
        attribute_func: if a list is provided, then you must provide a lambda or other function 
                        which outputs strings for the instance attribute keys.

        >>> test_dad = DynamicAttrDefaultDictList({'test': 1, 'testing': 2, 'space-buster': 4})
        >>> test_dad['testing'] = 3
        {'test': 1, 'testing': [2, 3], 'space_buster': 4}


        """       
        super().__init__()
        
        if isinstance(mapping, dict):
            self.update(mapping)

        elif isinstance(mapping, list):
            if attribute_func is None:
                raise ValueError(f"List provided ({mapping}) but no attribute function was given.")
            else:
                self._generate_mapping_from_list(mapping, attribute_func)
        
        else:
            raise ValueError(f'Unknown instantiating mapper: {mapping}')
        

        
    def __repr__(self):
        return str(self.data)
    
    def __str__(self):
        return repr(self)
    
    def _generate_mapping_from_list(self, list_items: list, attribute_func: Callable):
        
        for item in list_items:
            
            attr_value = attribute_func(item)
            
            if attr_value not in self.data:
                # insert solo
                self.data[attr_value] = item
            else:
                # it is already in the mapping, so we have a duplicate. Convert to list.
                if not isinstance(self.data[attr_value], list):
                    replacement_list = list()
                    replacement_list.append(self.data[attr_value])
                    self.data[attr_value] = replacement_list

                self.data[attr_value].append(item)

    def __getattr__(self, item):
        if item in self.data:
            return self.data[item]
        else:
            try:
                super().__getattr__(item)
            except AttributeError as e:
                return AttributeError(f'The requested attribute does not exist: {item}, raised this error: {e}')
    
    
    
    # change the result of dir(foo) to change the auto-completion box
    def __dir__(self):
        return list(self.data.keys())
                # + super().__dir__()


# This is inspired /copied from https://docs.sqlalchemy.org/en/20/orm/collection_api.html#custom-collection-implementations
from sqlalchemy.orm.collections import attribute_mapped_collection

import operator


class OptionedList(list):
    def __init__(self, optionspath: str, parentpath: str=None):
        super().__init__()
        # print(f'instantiated an OptionedList')
        self._optionspath = optionspath
        self._attrgetter = operator.attrgetter(self._optionspath)

        # dirty hack to preserve a reference to the parent entity so that SQLAlchemy references can be queries directly
        # this is important because SQLAlchemy may decide to change the pointers to the list objects at any time,
        # so we must fetch them directly off of the SQLModel, not simply pass the list objects through.
        if parentpath is not None:
            self._parentpath = parentpath
            self._parentattrgetter = operator.attrgetter(self._parentpath)

    @property
    def options(self):
        return self._attrgetter(self)

    @property
    def parent(self):
        return self._parentattrgetter(self)
    
     ############## TESTING W STANDARD ATTRIBUTES, UNNECESSARY #############

    # def __setitem__(self, index, item):
    #     print(f'setitem called with: {item=}')
    #     super().__setitem__(index, item)

    # def insert(self, index, item):
    #     print(f'insert called with {item=}')
    #     super().insert(index, item)

    # def append(self, item):
    #     print(f'append called with {item=}')
    #     super().append(item)

    # def extend(self, other):
    #     print(f'extend called with {other=}')
    #     super().extend(other)

