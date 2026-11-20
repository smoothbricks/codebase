# proofs-over-tests sections 4 and 16: canonical values, a guarded proof and an open control.
import Base

type Packet is Data:
  Missing{}
  Packed{n: Nat, live: Bool, items: +List<Nat>}

def advance(n: Nat) -> Nat:
  match n:
    case 0n:
      1n
    case 1n+p:
      1n+(1n+p)

def number(x: Packet) -> Nat:
  match x:
    case Missing{}:
      0n
    case Packed{n, live, items}:
      advance(n)

law packets_agree:
  for +x: Packet
  {x == x : Packet}

def packets_agree(x):
  {==}

law guarded_lists_agree:
  for +xs: +List<Nat>
  for +live: Bool
  for h: {live == True{} : Bool}
  {List.append(&2, Nat, xs, [0n]) == List.append(&2, Nat, xs, [0n]) : +List<Nat>}

def guarded_lists_agree(xs, live, h):
  {==}

# This is deliberately not proved. Empty packets pass; a packed zero is the first counterexample.
law all_packets_are_zero:
  for +x: Packet
  {number(x) == 0n : Nat}