from gooddataclient.dataset import Dataset
from gooddataclient.columns import ConnectionPoint


class Country(Dataset):

    country = ConnectionPoint(title='Country', folder='Country')
