from pydantic_panel import infer_widget
from pydantic_panel.widgets import PydanticModelEditorCard, PydanticModelEditor, BaseCollectionEditor
from pydantic.fields import FieldInfo, ModelField
from typing import Optional, ClassVar, Type, List, Dict, Tuple, Any, Union

import pandas as pd

import param
import panel as pn
from panel.layout import Column, Divider, ListPanel, Card
from panel.layout.gridstack import GridStack
from panel.widgets import MultiChoice


from sqlmodels import DataField, Dataset, DataRole
from helpers import OptionedList

class DataFieldEditorCard(PydanticModelEditorCard):
    """Same as PydanticModelEditor but uses a Card container
    to hold the widgets and synces the header with the widget `name`
    """

    _composite_type: ClassVar[Type[ListPanel]] = pn.Card
    collapsed = param.Boolean(False)
    collapsible = param.Boolean(False)

    is_json = param.Boolean(False)
    value: DataField = param.ClassSelector(class_=DataField)
    
    _selected_roles: pn.widgets.MultiChoice = param.ClassSelector(class_=pn.widgets.MultiChoice)


    def __init__(self, **params):
        super().__init__(**params)
        print(f'instantiating DataFieldEditorCard: {params=}')
        print(self.value)

        self._selected_roles = pn.widgets.MultiChoice(
            name='ROLES', 
            value=self.value.roles,
            options={x.name: x for x in self.value.dataset.roles_available}
            )

        self._composite.header = self.name
        self.link(self._composite, name="header")
        self.link(self._composite, collapsed="collapsed")

        # TODO: Find a better way to get custom items into the widget
        self._widgets['roles'] = self._selected_roles
        self._composite[:] = self.widgets

    @param.depends("_selected_roles.value", watch=True)
    def _update_value(self, value=None):
        if self.value is None or self._selected_roles is None:
            return

        self.value: DataField
        self.value.roles.clear()
        self.value.roles.extend(self._selected_roles.value)
        # update the set of available options
        self._selected_roles.options = dict(self.value.dataset.ra)

class OptionedListEditor(PydanticModelEditor):
    """Same as PydanticModelEditor but uses a Card container
    to hold the widgets and synces the header with the widget `name`
    """

    _composite_type: ClassVar[Type[ListPanel]] = pn.Card
    collapsed = param.Boolean(False)
    collapsible = param.Boolean(False)

    is_json = param.Boolean(False)
    value: DataField = param.ClassSelector(class_=DataField)
    
    _selected_roles: pn.widgets.MultiChoice = param.ClassSelector(class_=pn.widgets.MultiChoice)


    def __init__(self, **params):
        super().__init__(**params)
        print(f'instantiating DataFieldEditorCard: {params=}')
        print(self.value)

        self._selected_roles = pn.widgets.MultiChoice(
            name='ROLES', 
            value=self.value.roles,
            options={x.name: x for x in self.value.dataset.roles_available}
            )

        self._composite.header = self.name
        self.link(self._composite, name="header")
        self.link(self._composite, collapsed="collapsed")

        # TODO: Find a better way to get custom items into the widget
        self._widgets['roles'] = self._selected_roles
        self._composite[:] = self.widgets

    @param.depends("_selected_roles.value", watch=True)
    def _update_value(self, value=None):
        if self.value is None or self._selected_roles is None:
            return

        self.value: DataField
        self.value.roles.clear()
        self.value.roles.extend(self._selected_roles.value)
        # update the set of available options
        self._selected_roles.options = dict(self.value.dataset.ra)


############################
#        OptionedList      #
############################

# The problem now is that the list reference is being changed whenever Panel makes a move. We need to mutate the original list, so that
# SQLAlchemy can pass through the changes to the backend.

# Before going any further let us discover what these Event objects are. An Event is used to signal the change in a parameter value. Event objects provide a number of useful attributes that provides additional information about the event:

    # name: The name of the parameter that has changed
    # new: The new value of the parameter
    # old: The old value of the parameter before the event was triggered
    # type: The type of event (‘triggered’, ‘changed’, or ‘set’)
    # what: Describes what about the parameter changed (usually the value but other parameter attributes can also change)
    # obj: The Parameterized instance that holds the parameter
    # cls: The Parameterized class that holds the parameter


def mutate_list_roles_value(value, *events):
    """
    Return a new callable that references the original list, and mutate that referenced list when modification events occur.
    """
    value: DataField

    def mutate_existing(*events):
        
        print(f'{value=}')
        for event in events:
            items_to_remove = value.roles.copy()

            for role in event.new:
                # check if in existing list of Roles
                if role in value.roles:
                    try:
                        items_to_remove.remove(role)
                    except ValueError as e:
                        pass
                else:
                    value.roles.append(role)

            for role in items_to_remove:
                value.roles.remove(role)

    return mutate_existing


@infer_widget.dispatch(precedence=10)
def infer_widget(value: List[DataRole], field: Optional[FieldInfo] = None, **kwargs):
    """
    Dispatcher for OptionedList Pydantic type.
    """

    print(f'using infer_widget for OptionedList: {type(value)=} and {value=}')
    
    widget = pn.widgets.MultiChoice(
            value=value,
            options={x.name: x for x in value.options},
            solid=False,
            )
    
    watcher = widget.param.watch(mutate_list_roles_value(value.parent), 'value')
    return widget

############# USING A Custom MultiChoice editor CLASS ##############

def mutate_to_match(original, values):
    items_to_remove = original.copy()

    for item in values:
        # check if in existing list of Roles
        if item in original:
            try:
                items_to_remove.remove(item)
            except ValueError as e:
                pass
        else:
            original.append(item)

    for item in items_to_remove:
        original.remove(item)



class OptionedListEditor(MultiChoice):
    value = param.ClassSelector(Union[OptionedList,list], default=None)
    # parent = param.ClassSelector(DataField, default=None)

    def __init__(self, **params):
        self._optioned_list = params['value']
        self._parent = params.pop("parent", None)

        super().__init__(**params)

    
    @param.depends("value", watch=True)
    def _sync_params(self):
        print(f'{self.value=}')
        self.options = {x.name: x for x in self._optioned_list.options}


    @param.depends("value", watch=True)
    def _update_parent(self):
        mutate_to_match(self._optioned_list.parent.roles, self.value)
        print(self._optioned_list.parent.roles)





# @infer_widget.dispatch(precedence=3)
# def infer_widget(value: List[DataRole], field: Optional[FieldInfo] = None, **kwargs):
#     """
#     Dispatcher for OptionedList Pydantic type.
#     """

#     print(f'using infer_widget for OptionedList: {type(value)=} and {value=}')
    
#     return OptionedListEditor(value=value, parent=value.parent, options={x.name: x for x in value.options}, height=200)