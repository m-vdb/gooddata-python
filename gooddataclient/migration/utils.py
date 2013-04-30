

class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values

    From: https://github.com/hughdbrown/dictdiffer/blob/master/src/dictdiffer.py
    """

    def __init__(self, new_dict, old_dict):
        self.new_dict, self.old_dict = new_dict, old_dict
        self.new_keys, self.old_keys = [
            set(d.keys()) for d in (new_dict, old_dict)
        ]
        self.intersect = self.new_keys.intersection(self.old_keys)

    def changes(self):
        new_state = {}
        old_state = {}
        for key in self.intersect:
            if self.old_dict[key] != self.new_dict[key]:
                new_state[key] = self.new_dict[key]
                old_state[key] = self.old_dict[key]

        return new_state, old_state


def get_changed_attributes(new_dict, old_dict):
    return DictDiffer(new_dict, old_dict).changes()
