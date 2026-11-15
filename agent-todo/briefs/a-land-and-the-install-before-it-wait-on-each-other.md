# A land and the install of the land before it wait on each other

## Observed (2026-11-16, 24 minutes lost, ended by hand)

Land A landed. Its after-land install (`tooling/land-install.sh <old> <new>`, run by `cowshed __land-after`) reached
`cowshed gateway await-turns` and printed:

```
cowshed: await-turns: the install waits for 1 land turn(s) queued before it: <B> holds <repo> (ticket 77, pid …,
asked 01:21:45); lands that ask meanwhile wait behind it
```

Land B, asked while A's gate ran, passed its own gate and then printed, every few seconds, for 24 minutes:

```
cowshed: land waits for the ref turn <repo>.ref: parked behind an install that waits for the turns or fences them;
numbered once its fence lifts, or asked again once the install holding … ends
```

The install waits for B's turn to end; B waits for the install to end. Neither process is on a CPU. Ending B's
`cowshed land` by hand let the install finish in 40 s ("install installed"); B was then asked again.

## The rule to find and state

Read `specs/cowshed/05_gateway.md` and the land-turn sections on `await-turns`, the install's fence and the ref turn
before changing anything, and quote the sentences that order them. One of the two waits is wrong by that text: either an
install waits only for turns that were RUNNING when it asked (a land parked behind the install's own fence is not one),
or a land that has passed its gate takes the ref turn ahead of an install that has not yet fenced.

## Done when

- A test holds land A's install at `await-turns` while land B is queued behind A, and both end without a hand.
- The wait line of whichever side still waits names what it waits on and that this wait ends by itself.
