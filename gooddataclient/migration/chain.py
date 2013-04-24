from gooddataclient.dataset import Dataset


class MigrationChain(object):

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
        maql = ''
        target_datasets = set()
        for migration in self.chain:
            maql = maql + migration.execute()
            target_datasets.append(migration.schema_name)

        maql = self.add_synchronize(maql, target_datasets)

        try:
            self.project.execute_maql(maql)
        except MaqlExecutionFailed as e:
            err_msg = 'Migration "%(name)s" failed'
            raise MigrationFailed(
                err_msg, name=self.name, chain=self.chain
            )

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
            maql = maql + Dataset.get_synchronize_statement(name)

        return maql
