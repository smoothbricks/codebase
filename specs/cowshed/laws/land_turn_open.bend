# The land-turn law the install's read alone refutes (specs/cowshed/05_gateway.md
# "The read fences the turns", "A land installs its own cowshed after its turn"),
# over the model in land_turn.bend: today's install, held back only by its own read
# of the turns, which fences a free queue and changes nothing else. The land that
# asks after that read is numbered and queued ahead of the install's next read, and
# so is every land after it. It has no proof: it is a red. land_turn.bend proves
# its twin for the waiting install (waiting_install_numbers_no_ask) and computes
# the recorded schedule (a_reading_install_is_overtaken_by_every_later_ask).
#   install_read_holds_back_later_asks
#     e = E{0n, False{}, [], False{}}, t = [], c = 0n
# The read finds a ticket and sets no fence, and the next ask is numbered 1n and
# queued behind that ticket, ahead of the install. Recorded 2026-11-10: land
# 124380eca9's after-land install deferred at 12:16, 12:48 and 13:17Z, each time
# behind a batch-4 land that asked after its read (tickets 9, 10 and 11), and each
# rerun lost the gap to the next land's ask.
#
# Check: bun bend2/main.ts land_turn_open.bend --verdict (the law stays open).
import Base
import ./land_turn.bend as L

# Open, refuted by the queue: while an install that read the turns and found
# tickets waits to read again, an ask changes no queue.
law install_read_holds_back_later_asks:
  for +e: L.E
  for +t: +List<L.E>
  for +c: Nat
  {L.queue(L.step(L.TAsk{}, L.step(L.TFence{}, L.St{e <> t, c, 0n, 0n, 0n}))) == e <> t : +List<L.E>}
