# The land-turn law today's queue refutes (specs/cowshed/05_gateway.md "The read
# fences the turns", "A land installs its own cowshed after its turn"), over the
# model in land_turn.bend. An install that finds a land queued is held back only by
# its own read of the turns, which fences a free queue and changes nothing else:
# the land that asks after that read is numbered and queued ahead of the install's
# next read, and so is every land after it. It has no proof: it is a red.
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

# Open, refuted by the queue: while an install that found tickets waits, an ask
# changes no queue.
law install_read_holds_back_later_asks:
  for +e: L.E
  for +t: +List<L.E>
  for +c: Nat
  {L.queue(L.step(L.TAsk{}, L.step(L.TFence{}, L.St{e <> t, c, 0n}))) == e <> t : +List<L.E>}
