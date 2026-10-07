from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.testing import TestingSource

flow = Dataflow("quickstart")

# Input: Local test source for demonstration
inp = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

# Transform: Filter even numbers and multiply by 10
filtered = op.filter("keep_even", inp, lambda x: x % 2 == 0)
results = op.map("multiply_by_10", filtered, lambda x: x * 10)

# Output: Print results to stdout
op.inspect("print_results", results)