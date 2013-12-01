import os

from gooddataclient.columns import Date
from gooddataclient.migration.actions import AddColumn, AddDate, DeleteColumn, AlterColumn
from gooddataclient.migration.chain import MigrationChain


class MigrationEngine(object):
    """
    A class to generate a migration chain from a comparison
    between the state of the dataset onto the API and a dataset
    object.
    """

    def __init__(self, project, dataset_class):
        self.project = project
        self.dataset = dataset_class(project)
        self.chain = None

    def migrate(self, dump_folder=None):
        """
        A method to execute a migration for a given dataset.

        :param dump_folder:     if specified, the generated maql would
                                be dumped in this folder.
        """
        # get the diff of a dataset
        dataset_diff = self.dataset.get_remote_diff()
        # generate the chain according to this diff
        self.chain = self.generate_chain(dataset_diff)
        # dump the maql if needed
        self.dump_maql(dump_folder)
        # execute the migration chain
        self.chain.execute()

    def generate_chain(self, dataset_diff):
        """
        A function to generate a migration chain from a diff
        of a dataset.

        :param dataset_diff:       a dictionary representing
                                   a diff between a dataset
                                   and its remote state on the API.
        """
        chain = []

        for name, column in dataset_diff['added'].iteritems():
            Action = AddDate if isinstance(column, Date) else AddColumn
            chain.append(Action(self.dataset.identifier, name, column))

        for name, columns in dataset_diff['altered'].iteritems():
            chain.append(
                AlterColumn(
                    schema_name=self.dataset.identifier, col_name=name,
                    column=columns['old'], new_column=columns['new']
                )
            )

        for name, column in dataset_diff['deleted'].iteritems():
            chain.append(DeleteColumn(self.dataset.identifier, name, column))

        return MigrationChain(project=self.project, chain=chain)

    def dump_maql(self, dump_folder):
        """
        A function to dump the maql generated by the migration.
        """
        if dump_folder:
            file_name = 'migrate_%s.maql' % self.dataset.identifier
            file_path = os.path.join(dump_folder, file_name)
            with open(file_path, 'w') as f:
                f.write(self.chain.get_maql())
