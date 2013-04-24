

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

    def execute(self):
        """
        The method to execute the action
        """
        return None


class AddColumn(Action):
    """
    The action used to add a column to an
    existing dataset.
    """

    def execute(self):
        return self.column.get_maql(self.schema_name, self.name)


class DeleteColumn(Action):
    pass


class AlterColumn(Action):
    pass
