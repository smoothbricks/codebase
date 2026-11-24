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

# Small state machine for model-based testing conformance.
type GateState is Data:
  Closed{}
  Open{}
  Locked{}

type GateInput is Data:
  OpenGate{}
  CloseGate{}
  LockGate{}
  Ping{}

def gate_init() -> GateState:
  Closed{}

def gate_step(s: GateState, i: GateInput) -> GateState:
  match s:
    case Closed{}:
      match i:
        case OpenGate{}:
          Open{}
        case LockGate{}:
          Locked{}
        case CloseGate{}:
          Closed{}
        case Ping{}:
          Closed{}
    case Open{}:
      match i:
        case CloseGate{}:
          Closed{}
        case OpenGate{}:
          Open{}
        case LockGate{}:
          Open{}
        case Ping{}:
          Open{}
    case Locked{}:
      match i:
        case CloseGate{}:
          Closed{}
        case OpenGate{}:
          Locked{}
        case LockGate{}:
          Locked{}
        case Ping{}:
          Locked{}

def gate_handled(s: GateState, i: GateInput) -> Bool:
  match s:
    case Open{}:
      match i:
        case Ping{}:
          True{}
        case OpenGate{}:
          False{}
        case CloseGate{}:
          False{}
        case LockGate{}:
          False{}
    case Closed{}:
      False{}
    case Locked{}:
      False{}
