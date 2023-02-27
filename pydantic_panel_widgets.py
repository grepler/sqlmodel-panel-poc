from pydantic_panel import infer_widget
from pydantic_panel.widgets import PydanticModelEditorCard
from pydantic.fields import FieldInfo, ModelField
from typing import Optional, ClassVar, Type, List, Dict, Tuple, Any

import pandas as pd

import param
import panel as pn
from panel.layout import Column, Divider, ListPanel, Card
from panel.layout.gridstack import GridStack


from sqlmodels import DataField, Dataset, DataRole

# precedence > 0 will ensure this function will be called
# instead of the default which has precedence = 0


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





@infer_widget.dispatch(precedence=2)
def infer_widget(value: DataField, field: Optional[FieldInfo] = None, **kwargs):
    """
    Dispatcher for DataField Pydantic types.
    """
    # discovered that this seems to be necessary in: 
    # https://github.com/jmosbacher/pydantic-panel/blob/4cbcd380037df28476a4c7c4911994a6f246ff6f/pydantic_panel/widgets.py#L648
    class_ = kwargs.pop("class_", type(value))
    
    name = kwargs.pop("name", value.name)

    return DataFieldEditorCard(
        name=value.name, 
        value=value, 
        class_=class_, 
        fields=list(('name', 'roles', 'description')),
        **kwargs
        )

