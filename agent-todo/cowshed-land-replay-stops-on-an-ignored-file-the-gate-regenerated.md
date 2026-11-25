# cowshed land's replay stops on an ignored file the gate regenerated

A land whose main moved while its gate ran replays the branch onto the new main. The replay stopped with git's
"untracked working tree files would be overwritten … Please move or remove them before you merge". The file was an
ignored generated output that the gate had just regenerated. An earlier commit in the branch had tracked it, and a later
one had untracked it and added the ignore line. The gate was green and the land failed, three times in one night on one
batch (2026-11-26).

What must hold: a replay of the workspace's own branch never fails on a file that is ignored at the branch tip and that
a step of the land produced. Either the replay runs in a tree that holds only tracked content (the gate's outputs set
aside and restored), or ignored generated outputs that the history once tracked are cleared before the replay. A red: a
branch that adds a file, then untracks and ignores it; the file exists in the tree; main moves; the land replays and
lands.
