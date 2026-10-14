/**
 * The thrown value as `span-exception` evidence (01h, span-exception row).
 *
 * A thrown value is arbitrary: an `Error` whose outer message is empty and whose
 * meaning sits in its `cause`, an `AggregateError` whose meaning sits in its
 * `errors`, an `Error` from another realm, or no `Error` at all. Row 1 records
 * each `Error` link's name, message, a string or number `code`, and stack, an
 * `AggregateError`'s errors, and the `cause` chain. Other own fields are never
 * read: traces carry values only when they are explicitly logged (01a, Security
 * by Default), and an error's fields can hold request configuration or
 * credentials. A non-`Error` value is recorded as its type and a bounded
 * `String()`.
 *
 * The evidence has one total budget of links and characters, because a thrown
 * value can be any size and a getter can mint a new link on every read: what
 * the budget cannot hold is counted, never walked. Rendering never throws,
 * because a throw here would replace the exception being recorded: a part that
 * cannot be read is written as `[unreadable <part>: <reason>]` in its place and
 * the rest of the value is still recorded. Every operation that can reach user
 * code (a property read, `instanceof AggregateError`, `Array.isArray`, a list's
 * `length`, `String()`) runs inside a guarded read, and a read's answer is told
 * apart by its own tag, never by asking the user's value.
 */

/** The most characters one message or non-`Error` value contributes. */
const VALUE_TEXT_LIMIT = 4096;
/** The most characters the whole evidence records. */
export const EVIDENCE_CHARS_LIMIT = 16_384;
/** The most links (thrown value, causes, aggregated errors) the whole evidence records. */
export const EVIDENCE_LINKS_LIMIT = 64;

/** One render's shared state: what was recorded, and what the budget still allows. */
interface Render {
  readonly seen: Set<object>;
  linksLeft: number;
  charsLeft: number;
  omittedChars: number;
  /** Links the budget left unwalked; a stopped cause chain counts as one. */
  skippedLinks: number;
  text: string;
}

/** A guarded read: its answer, or what it threw. */
type Read<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly failure: unknown };

/** Row 1's `message`: the outer `Error` message, or the non-`Error` value, each bounded. */
export function thrownMessage(value: unknown): string {
  try {
    // Classification reads nothing of the value, but a host without `Error.isError` fails here, not in the caller.
    const error = readPart((): Error | undefined => (isError(value) ? value : undefined));
    if (!error.ok) return `${typeof value}: ${unreadable('value', error.failure)}`;
    const outer = error.value;
    return outer === undefined
      ? renderValue(value, 'value')
      : readText('message', () => bounded(String(outer.message)));
  } catch (failure) {
    return unreadable('value', failure);
  }
}

/**
 * Row 1's `exception_stack`: for each link, `name: message`, its `code` and its
 * stack, each on a labelled line; an `AggregateError`'s errors after
 * `\n[errors <i>]: `; every `cause` link after `\n[cause]: `. An object recorded
 * earlier in the same value is written as `[already recorded]`.
 */
export function thrownEvidence(value: unknown): string {
  const render: Render = {
    seen: new Set(),
    linksLeft: EVIDENCE_LINKS_LIMIT,
    charsLeft: EVIDENCE_CHARS_LIMIT,
    omittedChars: 0,
    skippedLinks: 0,
    text: '',
  };
  try {
    renderChain(value, 'value', render);
  } catch (failure) {
    write(render, `\n${unreadable('value', failure)}`);
  }
  const skipped = render.skippedLinks === 0 ? '' : `\n[evidence budget spent: ${render.skippedLinks} links not walked]`;
  const cut = render.omittedChars === 0 ? '' : `\n[${render.omittedChars} more characters not recorded]`;
  return render.text + skipped + cut;
}

/** Appends `piece`, cut to the characters the budget has left; the cut is counted. */
function write(render: Render, piece: string): void {
  if (piece.length <= render.charsLeft) {
    render.text += piece;
    render.charsLeft -= piece.length;
    return;
  }
  render.text += piece.slice(0, render.charsLeft);
  render.omittedChars += piece.length - render.charsLeft;
  render.charsLeft = 0;
}

/** `value` and every `cause` after it, while the budget lasts. */
function renderChain(value: unknown, part: string, render: Render): void {
  let link = value;
  let linkPart = part;
  for (;;) {
    if (render.linksLeft === 0 || render.charsLeft === 0) {
      render.skippedLinks++;
      write(render, `[${linkPart} not recorded]`);
      return;
    }
    render.linksLeft--;
    if ((typeof link === 'object' && link !== null) || typeof link === 'function') {
      if (render.seen.has(link)) {
        write(render, '[already recorded]');
        return;
      }
      render.seen.add(link);
    }
    const current = link;
    // Classification reads nothing of the value, but a host without `Error.isError` fails here; that link is recorded
    // in its place, not over the whole value.
    const error = readPart((): Error | undefined => (isError(current) ? current : undefined));
    if (!error.ok) {
      write(render, `${typeof current}: ${unreadable(linkPart, error.failure)}`);
      return;
    }
    const outer = error.value;
    if (outer === undefined) {
      write(render, renderValue(current, linkPart));
      return;
    }
    renderLink(outer, render);
    const hasCause = readPart(() => Object.hasOwn(outer, 'cause'));
    if (hasCause.ok && !hasCause.value) return;
    write(render, '\n[cause]: ');
    const cause = hasCause.ok ? readPart(() => outer.cause) : hasCause;
    if (!cause.ok) {
      write(render, unreadable('cause', cause.failure));
      return;
    }
    link = cause.value;
    linkPart = 'cause';
  }
}

function renderLink(error: Error, render: Render): void {
  const name = readText('name', () => bounded(String(error.name)));
  write(render, `${name}: ${readText('message', () => bounded(String(error.message)))}`);
  const code = readPart((): unknown => Reflect.get(error, 'code'));
  if (!code.ok) write(render, `\n[code]: ${unreadable('code', code.failure)}`);
  else if (typeof code.value === 'string' || typeof code.value === 'number') write(render, `\n[code]: ${code.value}`);
  const stack = readPart((): unknown => error.stack);
  if (!stack.ok) write(render, `\n[stack]: ${unreadable('stack', stack.failure)}`);
  else if (typeof stack.value === 'string') write(render, `\n[stack]: ${stack.value}`);
  // A real Error can still carry a proxy in its prototype chain, which `instanceof` walks.
  const aggregate = readPart(() => error instanceof AggregateError);
  if (!aggregate.ok) write(render, `\n[errors]: ${unreadable('errors', aggregate.failure)}`);
  else if (aggregate.value) renderAggregated(error, render);
}

/** An `AggregateError`'s own `errors`, each as a chain of its own, while the budget lasts. */
function renderAggregated(error: Error, render: Render): void {
  const listed = readPart((): { readonly errors: readonly unknown[]; readonly length: number } | string | undefined => {
    const errors: unknown = Object.hasOwn(error, 'errors') ? Reflect.get(error, 'errors') : undefined;
    if (!Array.isArray(errors)) return undefined;
    // A proxy's `length` trap may answer anything; only a real count reaches the loop's arithmetic.
    const length: unknown = Reflect.get(errors, 'length');
    return typeof length === 'number' && Number.isSafeInteger(length) && length >= 0
      ? { errors, length }
      : `a list whose length is not a count (${typeof length})`;
  });
  if (!listed.ok) {
    write(render, `\n[errors]: ${unreadable('errors', listed.failure)}`);
    return;
  }
  if (listed.value === undefined) return;
  if (typeof listed.value === 'string') {
    write(render, `\n[errors]: [unreadable errors: ${listed.value}]`);
    return;
  }
  const { errors, length } = listed.value;
  for (let index = 0; index < length; index++) {
    if (render.linksLeft === 0 || render.charsLeft === 0) {
      render.skippedLinks += length - index;
      write(render, `\n[${length - index} more errors not recorded]`);
      return;
    }
    write(render, `\n[errors ${index}]: `);
    const entry = readPart((): unknown => errors[index]);
    if (entry.ok) renderChain(entry.value, `errors ${index}`, render);
    else {
      // An unreadable entry is still one aggregated error: it spends a link, so the reads stay within the budget.
      render.linksLeft--;
      write(render, unreadable(`errors ${index}`, entry.failure));
    }
  }
}

/** The host's `DOMException.prototype.code` getter: a brand check that throws for anything but a real `DOMException`. */
const DOM_EXCEPTION_CODE =
  typeof DOMException === 'function' ? Object.getOwnPropertyDescriptor(DOMException.prototype, 'code')?.get : undefined;

/**
 * An `Error` by its `[[ErrorData]]` brand, or a `DOMException` by its own brand: some hosts (bun) answer
 * `Error.isError` false for a `DOMException`, whose `AbortError`/`TimeoutError` must still keep their name, message,
 * code and stack. The `DOMException` brand is the `code` getter's own receiver check, so neither an object made from
 * `DOMException.prototype` nor a proxy over a `DOMException` can claim it, and no prototype chain is walked.
 */
function isError(value: unknown): value is Error {
  if (Error.isError(value)) return true;
  if (DOM_EXCEPTION_CODE === undefined || typeof value !== 'object' || value === null) return false;
  try {
    Reflect.apply(DOM_EXCEPTION_CODE, value, []);
    return true;
  } catch {
    // The getter's receiver check refused: that refusal is the answer "not a DOMException", not a failure to record.
    return false;
  }
}

/** A non-`Error` value: its type and its own `String()`, bounded. */
function renderValue(value: unknown, part: string): string {
  return `${typeof value}: ${readText(part, () => bounded(String(value)))}`;
}

/** `text` cut to {@link VALUE_TEXT_LIMIT}, with the cut counted. */
function bounded(text: string): string {
  return text.length > VALUE_TEXT_LIMIT
    ? `${text.slice(0, VALUE_TEXT_LIMIT)}… [${text.length - VALUE_TEXT_LIMIT} more characters]`
    : text;
}

function readPart<T>(read: () => T): Read<T> {
  try {
    return { ok: true, value: read() };
  } catch (failure) {
    return { ok: false, failure };
  }
}

function readText(part: string, read: () => string): string {
  try {
    return read();
  } catch (failure) {
    return unreadable(part, failure);
  }
}

/** Total: the reason a part could not be read, even when the failure itself cannot be read. */
function unreadable(part: string, failure: unknown): string {
  let reason: string;
  try {
    reason = isError(failure) ? bounded(String(failure.message)) : `${typeof failure}: ${bounded(String(failure))}`;
  } catch {
    reason = `${typeof failure} failure whose text cannot be read`;
  }
  return `[unreadable ${part}: ${reason}]`;
}
