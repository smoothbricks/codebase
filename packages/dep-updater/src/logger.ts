/**
 * Logging utilities with level control
 */

/**
 * Log levels in order of severity
 */
export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  SILENT = 4,
}

/**
 * Logger interface
 */
export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

/**
 * Console-based logger with level control
 */
export class ConsoleLogger implements Logger {
  constructor(private level: LogLevel = LogLevel.INFO) {}

  debug(message: string, ...args: unknown[]): void {
    if (this.level <= LogLevel.DEBUG) {
      console.debug(message, ...args);
    }
  }

  info(message: string, ...args: unknown[]): void {
    if (this.level <= LogLevel.INFO) {
      console.log(message, ...args);
    }
  }

  warn(message: string, ...args: unknown[]): void {
    if (this.level <= LogLevel.WARN) {
      console.warn(message, ...args);
    }
  }

  error(message: string, ...args: unknown[]): void {
    if (this.level <= LogLevel.ERROR) {
      console.error(message, ...args);
    }
  }

  /**
   * Set the log level
   */
  setLevel(level: LogLevel): void {
    this.level = level;
  }

  /**
   * Get the current log level
   */
  getLevel(): LogLevel {
    return this.level;
  }
}

/**
 * Silent logger that doesn't output anything
 * Useful for programmatic use or testing
 */
export class SilentLogger implements Logger {
  debug(): void {
    // no-op
  }

  info(): void {
    // no-op
  }

  warn(): void {
    // no-op
  }

  error(): void {
    // no-op
  }
}

/**
 * Create a logger based on configuration
 */
export function createLogger(verbose = false, silent = false): Logger {
  if (silent) {
    return new SilentLogger();
  }

  const level = verbose ? LogLevel.DEBUG : LogLevel.INFO;
  return new ConsoleLogger(level);
}
