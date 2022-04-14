from BTrees.OOBTree import OOBTree

class Row:
  def __init__(self, values):
    self.values = values
    #print(values)

  def __len__(self):
    return len(self.values)

  def __str__(self):
    return str(self.values)

  def __eq__(self, other):
    for i in range(len(self.values)):
      if self.values[i] != other.values[i]:
        return False
    return True

  def get_vals(self):
    return self.values


