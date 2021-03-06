from gooddataclient.columns import Date, Reference, Label, HyperLink
from gooddataclient.dataset import DateDimension
from gooddataclient.exceptions import MigrationFailed
from gooddataclient.migration.utils import get_changed_attributes
from gooddataclient.text import to_identifier


class Action(object):
    """
    An abstract layer representing an action
    during a LDM migration.
    """

    def __init__(self, schema_name, col_name, column, label_references_cp=None):
        """
        Initialize the action to execute.

        :param schema_name: the name of the dataset to modify
        :param col_name:    the name of the column in the dataset
        :param column:      the Column object in the dataset
        """
        self.schema_name = schema_name
        self.col_name = col_name
        self.column = column
        self.label_references_cp = label_references_cp

    def get_maql(self):
        """
        The method to execute the action
        """
        raise NotImplementedError


class AddColumn(Action):
    """
    The action used to add a column to an
    existing dataset.
    """

    def get_maql(self):
        return self.column.get_maql(to_identifier(self.schema_name), self.col_name, self.label_references_cp)


class AddDate(AddColumn):
    """
    The action used to add a date to an
    existing dataset. It creates the date
    dimension.
    """

    def create_dimension(self, project):
        DateDimension(project).create(
            name=self.column.schemaReference, include_time=self.column.datetime
        )


class DeleteColumn(Action):
    """
    The action used to delete a column from an
    existing dataset.
    """

    def get_maql(self):
        return self.column.get_drop_maql(to_identifier(self.schema_name), self.col_name)


class AlterColumn(Action):
    SIMPLE_ALTER = set(['title', 'dataType'])

    def __init__(self, new_column, *args, **kwargs):
        """
        The AlterColumn takes an additional mandatory
        argument which is the new format of the column,
        as opposed to the old format (passed as the column
        argument).

        :param new_column: the Column object representing
                           the column after migration.
        """
        super(AlterColumn, self).__init__(*args, **kwargs)
        self.new_column = new_column
        # if the user did not specify that the label references
        # a connection point, that means that we copy it from the
        # old column
        if self.label_references_cp is None:
            self.new_column.references_cp = self.column.references_cp
        # in other case, that means that we explicitely want
        # to change this attribute
        elif self.new_column.references_cp is None:
            self.new_column.references_cp = self.label_references_cp

        self.new_attrs = get_changed_attributes(self.new_column.__dict__, self.column.__dict__)
        # we can't perform actions on folders (useless and painful)
        self.new_attrs.pop('folder', None)
        self.new_attrs.pop('folder_title', None)

        self.alteration_state = self._alteration_state()

    def _alteration_state(self):
        # if same column classes
        same_columns = issubclass(self.new_column.__class__, self.column.__class__)

        # if Label -> HyperLink or the other way around
        hyperlink = False
        if (
            ((isinstance(self.column, Label) and isinstance(self.new_column, HyperLink))
             or (isinstance(self.column, HyperLink) and isinstance(self.new_column, Label)))
            and not (isinstance(self.column, HyperLink) and isinstance(self.new_column, HyperLink))
        ):
            same_columns = False
            hyperlink = True

        # if simple alteration
        simple_alter = set(self.new_attrs).issubset(self.SIMPLE_ALTER)

        # we cannot modify dataType or title of those two
        invalid = False
        if same_columns and simple_alter:
            if isinstance(self.column, Reference):
                invalid = True
            elif isinstance(self.column, Date) and 'dataType' in self.new_attrs:
                invalid = True

        return {
            'nb_changes': len(self.new_attrs) + hyperlink,
            'simple': simple_alter,
            'same_columns': same_columns,
            'hyperlink': hyperlink,
            'invalid': invalid,
        }

    def get_maql(self):
        if not self.alteration_state['nb_changes']:
            return ''

        hyperlink_change = self.alteration_state['simple'] & self.alteration_state['hyperlink']

        return self.column.get_alter_maql(
            schema_name=to_identifier(self.schema_name), name=self.col_name,
            new_attributes=self.new_attrs, hyperlink_change=hyperlink_change
        )


class DeleteRow(object):
    """
    An action to generate the maql to delete the rows of
    a dataset or a dataset's column, given a criterion.
    """

    def __init__(self, dataset, column=None, where_clause=None, where_values=None):
        """
        Initialize this action. The `dataset` parameter
        should be an instance of a dataset class.
        If column is set, it will delete rows from the dataset's column
        instead of dataset rows.
        If where_clause is set, it will override the where clause sent to GD.
        It should be follow GD form.
        Else if where_values is set, it will delete the rows of the selected column
        where the value is in where_values. It should be a list.
        """
        self.schema_name = dataset.schema_name
        self.dataset = dataset
        self.column = column
        self.where_values = where_values
        self.where_clause = where_clause

    def get_maql(self):
        return self.dataset.get_maql_delete(
            where_clause=self.where_clause,
            where_values=self.where_values,
            column=self.column
        )
