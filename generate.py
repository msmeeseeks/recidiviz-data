class IngestInfo:
  def __init__(self):
    self._value_generators = {}
    self._values = {}

  def set(self, key, value_generator, transformer=None):
    self._value_generators[key] = (value_generator, transformer)

  def fill_values(self):
    for key, (value_generator, transformer) in self._value_generators.items():
      value = value_generator()
      self._values[key] = transformer(value) if transformer else value

class Values:
  """This is supposed to represent the generic extractor."""
  def __init__(self):
    self._keys = set()
    self._values = {}

  def compute_values(self):
    # This is the actual 'scrape', to show that we need the keys first before we
    # can compute the values.
    for key in self._keys:
      self._values[key] = 'asdf_%s' % key

  def __getitem__(self, key):
    # This doesn't actually return the value, it returns a lambda that can be
    # used to get the value once |compute_values| has been called.
    self._keys.add(key)
    return lambda: self._values[key]

def child_scrape(values):
  ii = IngestInfo()
  ii.set('aardvark', values['aardvark'])
  ii.set('banana', values['banana'], transformer=lambda x: x.split('_')[0])
  ii.set('mango', values['banana'], transformer=lambda x: x.split('_')[1])
  return ii

def generic_scrape():
  values = Values()
  ii = child_scrape(values)
  values.compute_values()
  ii.fill_values()
  for key_value in ii._values.items():
    print key_value

generic_scrape()
