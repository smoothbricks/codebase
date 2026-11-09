# The disk-lease law the kernel refutes (specs/cowshed/05_gateway.md "Disk-lifecycle lease", the
# GATEWAY-IO-PRIORITY work item), over the model in disk_lease.bend with the code as
# it is. It has no proof: it is a red. `gates` names the tickets that are gates' leases; the kernel
# never reads that label (`Ledger::ask` takes a class, a share and a claim). disk_lease.bend
# proves the counterexample, gate_io_priority_refuted:
#   [DAsk{CNamespace, SShared}, DAsk{CStorage, SShared}], gates = [2]
#     ticket 1 (a background namespace lease) holds the ledger; ticket 2, the gate's storage
#     lease, waits behind it. No shorter trace exists: a lease waits only behind another.
# Driver: concatenate disk_lease.bend with this law (the emitter reads one book, so drop the import
# and the `D.` prefix), emit with --len 3 --cap 400 inside cowshed under a memory ceiling, and run
# <out>/<stem>.ts: the corpus reaches the counterexample.
import Base
import ./disk_lease.bend as D

# Open, refuted by the code: no gate's lease waits behind a background lease. A lease waits behind
# every holder of the running phase and every older waiter, whoever asked it.
law gate_io_priority:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<D.Op>
  for +gates: +List<Nat>
  {D.gate_ok_b(D.lg(D.run(os, D.empty(c0, b0))), gates) == True{} : Bool}
