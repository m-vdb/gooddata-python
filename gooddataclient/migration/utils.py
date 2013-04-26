

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
        for o in self.intersect:
            if self.old_dict[o] != self.new_dict[o]:
                new_state[o] = self.new_dict[o]
                old_state[o] = self.old_dict[o]

        return new_state, old_state


def get_changed_attributes(new_dict, old_dict):
    return DictDiffer(new_dict, old_dict).changes()
