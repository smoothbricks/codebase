package bendmodel

import "testing"

func TestBoundInputs(t *testing.T) {
	const largest = (1 << 48) - 1
	if n := Fn_number(Ctor_Packed{F0: largest - 1}); n != largest {
		t.Fatalf("largest representable successor: got %d", n)
	}
	xs := &List[uint64]{Head: 2}
	if ok, lhs := Law_guarded_lists_agree(xs, true); !ok || lhs != "[2,0]" || xs.Tail != nil {
		t.Fatalf("append changed its input or result: %v %q %+v", ok, lhs, xs)
	}
	if ok, lhs := Law_guarded_lists_agree(nil, false); !ok || lhs != "vacuous" {
		t.Fatalf("false hypothesis: %v %q", ok, lhs)
	}
	cases, draws, failures := 0, 0, 0
	Drive_all_packets_are_zero(func(kind string, index int, ok bool, lhs string, x Type_Packet) {
		switch kind {
		case "law":
			if index != cases {
				t.Fatalf("case %d after %d", index, cases)
			}
			cases++
		case "prop":
			if index != draws {
				t.Fatalf("draw %d after %d", index, draws)
			}
			draws++
		default:
			t.Fatalf("unexpected row kind %q", kind)
		}
		_, missing := x.(Ctor_Missing)
		if ok != missing {
			t.Fatalf("%s %d: %v, lhs %s for %#v", kind, index, ok, lhs, x)
		}
		if !ok {
			failures++
		}
	})
	if cases != 19 || draws != 16 || failures == 0 {
		t.Fatalf("open control: got %d cases, %d draws, %d counterexamples", cases, draws, failures)
	}
}

func TestNatLimit(t *testing.T) {
	defer func() {
		if reason := recover(); reason != "Nat past 2^48" {
			t.Fatalf("overflow: %v", reason)
		}
	}()
	Fn_number(Ctor_Packed{F0: (1 << 48) - 1})
}
