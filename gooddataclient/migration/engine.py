from gooddataclient.columns import Date
from gooddataclient.migration.chain import AddColumn, AddDate, DeleteColumn, AlterColumn


class MigrationEngine(object):
    """
    A class to generate a migration chain from a comparison
    between the state of the dataset onto the API and a dataset
    object.
    """

    def __init__(self, project, dataset_class):
        self.project = project
        self.dataset = dataset_class(project)

    def migrate(self):
        """
        A method to execute a migration for a given dataset.
        """
        # get the diff of a dataset
        dataset_diff = self.dataset.get_remote_diff()
        # generate the chain according to this diff
        chain = self.generate_chain(dataset_diff)
        # execute the migration chain
        chain.execute()

    def generate_chain(self, dataset_diff):
        """
        A function to generate a migration chain from a diff
        of a dataset.

        :param dataset_diff:       a dictionary representing
                                   a diff between a dataset
                                   and its remote state on the API.
        """
        chain = []

        for name, column in dataset_diff['added']:
            pass
