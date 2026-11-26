/** Inputs that decide the stderr exit line bounded-exec writes after a command ends. */
export interface ExitDiagnosticInput {
  readonly code: number;
  readonly exitCode: number | null;
  readonly exitSignal: NodeJS.Signals | null;
  readonly expired: boolean;
  readonly endedByMessage: string | null;
  readonly fixtureCommand: string | null;
  readonly mainStarted: boolean;
  readonly command: string;
}

/**
 * The stderr line bounded-exec writes when a command ends non-zero without a bound firing, or `null` when none is
 * written (exit 0 or a bound already explained the stop). A failure before the main segment starts attributes the exit
 * to the fixture ("Fixture command ..."); once the main segment has started, or without a fixture, it attributes the
 * exit to the command ("Command ...").
 */
export function formatExitDiagnostic(input: ExitDiagnosticInput): string | null {
  if (input.code === 0 || input.expired) {
    return null;
  }
  // The executor ended it for a cause of its own, which is said; otherwise no bound of this executor ended it, so a
  // signal it died of came from outside (the kernel's OOM killer sends SIGKILL): the signal is named, never folded
  // into a bare status.
  const signalled = input.exitCode === null && input.exitSignal !== null;
  const ended =
    input.endedByMessage !== null
      ? `was ended by bounded-exec because ${input.endedByMessage} (${signalled ? `${input.exitSignal}, ` : ''}status ${input.code})`
      : signalled
        ? `was killed by ${input.exitSignal} (status ${input.code})`
        : `exited with status ${input.code}`;
  return input.fixtureCommand !== null && !input.mainStarted
    ? `Fixture command ${ended}: ${input.fixtureCommand}\n`
    : `Command ${ended}: ${input.command}\n`;
}
