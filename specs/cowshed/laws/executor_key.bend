# packages/nx-plugin/README.md "Bounded exec key": an executor keys its runtime
# imports up to, but not including, a module declared output-neutral. A neutral
# module stops only the path through it, not a different path to its imports.
# The inputs are resolved runtime import paths to one candidate, not source text.
# This decides membership; cache_key.bend's key_is_pure and key_moves_with_input
# already give the byte laws for a key's selected inputs.
# Check: bend2 specs/cowshed/laws/executor_key.bend --verdict
import Base

# The entry is keyed by definition. Each path starts just after the entry and
# includes the candidate (the empty path represents the entry itself). True
# marks a module declared neutral by the executor record; False marks no cut.
def path_keys(path: +List<Bool>) -> Bool:
  match path:
    case Nil{}:
      True{}
    case True{} <> rest:
      False{}
    case False{} <> rest:
      path_keys(rest)

# A candidate is keyed if any path to it stays open. An empty outer list means
# no path reaches it. The same module can have both a cut path and an open one.
def closure_keys(paths: +List<+List<Bool>>) -> Bool:
  match paths:
    case Nil{}:
      False{}
    case path <> rest:
      Bool.or(path_keys(path), closure_keys(rest))

law neutral_stops_path:
  for +prefix: +List<Bool>
  for +suffix: +List<Bool>
  {path_keys(List.append(&2, Bool, prefix, True{} <> suffix)) == False{} : Bool}

def neutral_stops_path(prefix, suffix):
  match prefix:
    case Nil{}:
      {==}
    case True{} <> rest:
      {==}
    case False{} <> rest:
      neutral_stops_path(rest, suffix)

# Even an arbitrarily deep cut contributes no keyed read; the answer is what
# all the other paths say, whether or not they also pass through neutral modules.
law cut_does_not_hide_other_paths:
  for +prefix: +List<Bool>
  for +suffix: +List<Bool>
  for +other: +List<+List<Bool>>
  {closure_keys(List.append(&2, Bool, prefix, True{} <> suffix) <> other) == closure_keys(other) : Bool}

def cut_does_not_hide_other_paths(prefix, suffix, other):
  Equal.cong(Bool, Bool, open => Bool.or(open, closure_keys(other)),
    path_keys(List.append(&2, Bool, prefix, True{} <> suffix)), False{}, neutral_stops_path(prefix, suffix))

# Witness against walking through a cut: an otherwise reachable nonneutral
# leaf is not keyed behind the neutral module, and is keyed with the cut removed.
law ignoring_neutral_cut_is_refuted:
  {(closure_keys([[False{}, True{}, False{}]]), closure_keys([[False{}, False{}, False{}]])) == (False{}, True{}) : Bool & Bool}

def ignoring_neutral_cut_is_refuted():
  {==}

# Witness against globally excluding a neutral module's descendants: two nested
# routes to the same leaf, exactly one cut. Either traversal order keeps the leaf.
law mixed_paths_keep_the_module_keyed:
  {(closure_keys([[False{}, True{}, False{}], [False{}, False{}, False{}]]), closure_keys([[False{}, False{}, False{}], [False{}, True{}, False{}]])) == (True{}, True{}) : Bool & Bool}

def mixed_paths_keep_the_module_keyed():
  {==}