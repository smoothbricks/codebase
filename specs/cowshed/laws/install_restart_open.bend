# The restart laws the code refutes (specs/cowshed/05_gateway.md "An install
# refuses while a land runs", "A land step waits for an install in progress"),
# over the model in install_restart.bend with fenced=False, the code as it is,
# and the old land-install whose build asked a land turn of its own (05_gateway.md
# "A land installs its own cowshed after its turn"). They have no proof: each is a
# red. install_restart.bend proves their twins for the repairs (fenced_*,
# no_land_waits_on_install_build) and computes each counterexample:
#   restart_never_cuts_a_turn             the_race_cuts_a_turn
#     [ROLand(0),ROInstall,ROInstall,ROInstall,ROLand(0),ROInstall]
#   restart_never_kills_an_admitted_gate  the_race_kills_a_gate
#     [ROLand(0),ROInstall,ROInstall,ROInstall,ROLand(0),ROLand(0),ROInstall,ROLand(0)]
#   restart_never_refuses_an_ask          the_race_refuses_an_ask
#     [ROLand(0),ROInstall,ROInstall,ROInstall,ROInstall,ROLand(0)]
#   install_build_turn_never_delays_a_land  a_land_queues_behind_a_turn_build
#     [ROInstall,ROLand(0),ROLand(0)]
# ROLand(0) is one land's next step, ROInstall the install's. None is shorter: a
# restart needs the install's three steps before it (readiness, slot, read), the
# ticket it cuts is asked after the read (the read refuses one asked before), and
# the land's slot check before that ask cannot pass while the install holds the
# slot, so it comes before the install takes it.
#
# Driver: concatenate install_restart.bend with these laws (the emitter reads
# one book), emit with --len 3 --cap 400 inside cowshed under a memory ceiling,
# and run <out>/<stem>.ts: the corpus reaches the build-turn counterexample, the
# seeded draws (2000 schedules of up to 24 steps) the restart ones.
import Base
import ./install_restart.bend as R

# Open, refuted by the code: no restart closes a held land turn. A land that
# passed its slot check before the install took the slot asks its turn after the
# install read no blockers, and the restart then closes it.
law restart_never_cuts_a_turn:
  for +os: +List<R.RO>
  {R.n_cuts(R.run(os, R.empty(False{}, False{}))) == 0n : Nat}

# Open, refuted by the code: no restart makes a land send its admitted gate SIGTERM.
law restart_never_kills_an_admitted_gate:
  for +os: +List<R.RO>
  {R.n_kills(R.run(os, R.empty(False{}, False{}))) == 0n : Nat}

# Open, refuted by the code: no turn ask meets a gateway that is restarting.
law restart_never_refuses_an_ask:
  for +os: +List<R.RO>
  {R.n_refused(R.run(os, R.empty(False{}, False{}))) == 0n : Nat}


# Open, refuted by the old land-install: no land waits behind an install's build.
law install_build_turn_never_delays_a_land:
  for +os: +List<R.RO>
  {R.waits_on_install(R.queue(R.run(os, R.empty(False{}, True{})))) == False{} : Bool}
