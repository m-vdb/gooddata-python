from gooddataclient.columns import Label, HyperLink
from gooddataclient.dataset import DateDimension
from gooddataclient.exceptions import MigrationFailed
from gooddataclient.migration.utils import get_changed_attributes
from gooddataclient.text import to_identifier


class Action(object):
    """
    An abstract layer representing an action
    during a LDM migration.
    """

    def __init__(self, schema_name, col_name, column, label_references_cp=False):
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


# TODO: this shouldn't be here, it will be treated in ANA-460
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

    def get_maql(self):
        new_attrs, old_attrs = get_changed_attributes(self.column.__dict__, self.new_column.__dict__)

        # in the case of simple alteration
        if set(new_attr.keys()).issubset(SIMPLE_ALTER):

            # if strictly same columns
            if isinstance(self.column, self.new_column.__class__) and \
               isinstance(self.new_column, self.column.__class__):
                return self.get_maql_same_columns(new_attrs)

            # labels and hyperlinks are different
            elif isinstance(self.column, Label) and isinstance(self.column, Label):
                return self.get_maql_same_columns(new_attrs, hyperlink=True)

        # FIXME : we need to be smarter than that, because some labels / hyperlink / references
        #         might be dropped...
        # complex cases: equivalent to Delete + Add
        maql_delete = DeleteColumn(self.schema_name, self.col_name, self.column).get_maql()
        maql_add = AddColumn(self.schema_name, self.col_name, self.new_column).get_maql()

        return maql_delete + maql_add

    def get_maql_same_columns(self, new_attributes, hyperlink=False):
        """
        A function to get the MAQL to migrate two columns of the same type.
        The new_attributes and old_attributes are dictionaries which
        contains the keys that differ from one column to another.

        :param new_attributes:    the new column keys that are different
                                  from the old column keys.
        :param hyperlink:         a boolean that says if we need to
                                  handle the case of a hyperlink.
        """
        return self.column.get_alter_maql(self.schema_name, self.col_name, new_attributes, hyperlink)


class DeleteRow(object):
    """
    An action to generate the maql to delete the rows of
    a dataset, given a criterion.
    """

    def __init__(self, dataset, where_clause):
        """
        Initialize this action. The `dataset` parameter
        should be an instance of a dataset class, and
        the where clause should be follow this:

            column [=,>,<,<=,>=, IN] values [[AND, OR] condition]
        """
        self.schema_name = dataset.schema_name
        self.dataset = dataset
        self.where_clause = where_clause

    def get_maql(self):
        # FIXME: for now, we need to pass a manual WHERE clause,
        #        foung in GD documentation. In the future, when
        #        we want to programatically migrate datasets,
        #        we may need to change that, and construct the
        #        where clause from objects.
        return self.dataset.get_maql_delete(self.where_clause)
