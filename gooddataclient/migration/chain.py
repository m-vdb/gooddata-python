from gooddataclient.dataset import Dataset
from gooddataclient.exceptions import MaqlExecutionFailed, MigrationFailed
from gooddataclient.migration.actions import AddDate


class MigrationChain(object):

    name = 'Migration'
    chain = []

    def __init__(self, project):
        """
        Initialize the migration chain with a project.

        :param project:  a Project instance
        """
        self.project = project
        self.dates = []

    def execute(self):
        """
        A function to execute every atomic migration of the chain.
        """
        maql = self.get_maql()

        # create the date dimentions if needed
        for date in self.dates:
            date.create_dimension(self.project)

        try:
            self.project.execute_maql(maql)
        except MaqlExecutionFailed as e:
            err_msg = 'Migration "%(name)s" failed'
            raise MigrationFailed(
                err_msg, name=self.name, chain=self.chain
            )

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
        # FIXME: maybe referencies are not treated the same way,
        #        and will need to synchronize the schemaReferences.

        for name in dataset_names:
            maql = maql + Dataset.get_synchronize_statement(name, preserve=True)

        return maql
