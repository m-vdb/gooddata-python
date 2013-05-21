from gooddataclient.dataset import Dataset
from gooddataclient.columns import Date
from gooddataclient.exceptions import MaqlExecutionFailed, MigrationFailed
from gooddataclient.migration.actions import (
    AddDate, AddColumn, AlterColumn, DeleteColumn
)


class BaseChain(object):
    """
    A base class for migrations.
    """

    chain = []

    def __init__(self, project, chain=None):
        """
        Initialize the migration chain with a project.

        :param project:  a Project instance
        """
        self.project = project
        if chain:
            self.chain = chain

    def execute(self):
        """
        A function to execute every atomic migration of the chain.
        """
        maql = self.get_maql()
        self.pre_push()

        if maql:
            try:
                self.push_maql(maql)
            except MaqlExecutionFailed as e:
                err_msg = 'Migration failed, MAQL execution error %(original_error)s'
                raise MigrationFailed(
                    err_msg, chain=self.chain,
                    original_error=e
                )
            else:
                self.post_push()

    def pre_push(self):
        """
        A hook useful if some actions need to be done
        before the push.
        """
        pass

    def post_push(self):
        """
        A hook useful to perform some actions after everything
        is pushed.
        """
        pass

    def push_maql(self, maql):
        raise NotImplementedError

    def get_maql(self):
        raise NotImplementedError


class MigrationChain(BaseChain):
    """
    A class representing a migration. It should be subclassed,
    and class attributes should be overriden as is:

    - data_chain:   the chain corresponding to data migration, if needed
                    by complex migrations. This chain is execute after
                    structure is migration
    """
    data_chain = []

    def __init__(self, post_push=True, data_chain=None, *args, **kwargs):
        super(MigrationChain, self).__init__(*args, **kwargs)
        self.simplify_chain()
        self.dates = []
        self.do_post_push = post_push
        if data_chain:
            self.data_chain = data_chain
        self.data_migration = DataMigrationChain(
            chain=self.data_chain, project=self.project
        )

    def simplify_chain(self):
        """
        A function that reads the chain of the migration and that,
        when encountering an AlterColumn action, splits it in Delete + Add
        actions in complex cases.

        NB: in some cases, GD does not provide a way to do alteration,
        that is why we fall back to Delete + Add.
        """
        simple_chain = []

        for action in self.chain:
            if isinstance(action, AlterColumn):
                if action.alteration_state['invalid']:
                    # FIXME: issue a INFO log here
                    pass

                if (action.alteration_state['simple'] and
                    (action.alteration_state['same_columns'] or
                     action.alteration_state['hyperlink'])):
                    simple_chain.append(action)
                else:
                    # complex cases = Delete + Add
                    simple_chain.append(
                        DeleteColumn(action.schema_name, action.col_name, action.column)
                    )
                    if isinstance(action.new_column, Date):
                        add_action = AddDate(action.schema_name, action.col_name, action.new_column)
                    else:
                        add_action = AddColumn(action.schema_name, action.col_name, action.new_column)

                    simple_chain.append(add_action)
            else:
                simple_chain.append(action)

        self.chain = simple_chain

    def pre_push(self):
        # create the date dimentions if needed
        for date in self.dates:
            date.create_dimension(self.project)

    def push_maql(self, maql):
        self.project.execute_maql(maql)

    def post_push(self):
        if self.do_post_push:
            self.data_migration.execute()

    def get_maql(self):
        """
        A function to retrieve the maql to execute to
        achieve the migration.
        """
        maql = ''
        target_datasets = set()
        for migration in self.chain:
            if isinstance(migration, AddDate):
                self.dates.append(migration)
            maql = maql + migration.get_maql()
            target_datasets.add(migration.schema_name)

        return self.add_synchronize(maql, target_datasets)

    def add_synchronize(self, maql, dataset_names):
        """
        A function to complete the maql statements with the
        SYNCHRONIZE statement, for all the datasets that will
        be migrated.

        :param maql:             the maql statements
        :param dataset_names:    the list of dataset names
        """
        # FIXME: maybe references are not treated the same way,
        #        and will need to synchronize the schemaReferences.

        for name in dataset_names:
            maql = maql + Dataset.get_synchronize_statement(name, preserve=True)

        return maql


class DataMigrationChain(BaseChain):

    def __init__(self, chain, *args, **kwargs):
        self.chain = chain
        super(DataMigrationChain, self).__init__(*args, **kwargs)

    def push_maql(self, maql):
        self.project.execute_dml(maql)

    def get_maql(self):
        maql = ''
        for migration in self.chain:
            maql += migration.get_maql()

        return maql
