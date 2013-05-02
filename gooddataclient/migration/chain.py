from gooddataclient.dataset import Dataset
from gooddataclient.exceptions import MaqlExecutionFailed, MigrationFailed
from gooddataclient.migration.actions import AddDate


class BaseChain(object):
    """
    A base class for migrations.
    """

    name = 'Migration'
    chain = []

    def __init__(self, project):
        """
        Initialize the migration chain with a project.

        :param project:  a Project instance
        """
        self.project = project

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
                err_msg = 'Migration "%(name)s" failed'
                raise MigrationFailed(
                    err_msg, name=self.name, chain=self.chain
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

    def __init__(self, post_push=True, *args, **kwargs):
        super(MigrationChain, self).__init__(*args, **kwargs)
        self.dates = []
        self.do_post_push = post_push
        self.data_migration = DataMigrationChain(
            chain=self.data_chain, project=self.project
        )

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
