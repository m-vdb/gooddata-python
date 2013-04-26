from gooddataclient.text import to_identifier


class Action(object):
    """
    An abstract layer representing an action
    during a LDM migration.
    """

    def __init__(self, schema_name, col_name, column):
        """
        Initialize the action to execute.

        :param schema_name: the name of the dataset to modify
        :param col_name:    the name of the column in the dataset
        :param column:      the Column object in the dataset
        """
        self.schema_name = schema_name
        self.col_name = col_name
        self.column = column

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
        return self.column.get_maql(to_identifier(self.schema_name), self.col_name)


class DeleteColumn(Action):
    """
    The action used to delete a column from an
    existing dataset.
    """

    def get_maql(self):
        # FIXME : check that all labels are removed
        return self.column.get_drop_maql(to_identifier(self.schema_name), self.col_name)


class AlterColumn(Action):
    # alter cases: title, dataType (without changing LDM type)
    # if changing the LDM type, I think it's equivalent to delete+add, appart from attribute -> ConnectionPoint
    pass
