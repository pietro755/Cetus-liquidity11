export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

class Logger {
  private level: LogLevel;
  private verbose: boolean;

  constructor(level: string = 'info', verbose: boolean = false) {
    this.level = this.parseLogLevel(level);
    this.verbose = verbose;
  }

  private parseLogLevel(level: string): LogLevel {
    switch (level.toLowerCase()) {
      case 'debug':
        return LogLevel.DEBUG;
      case 'info':
        return LogLevel.INFO;
      case 'warn':
        return LogLevel.WARN;
      case 'error':
        return LogLevel.ERROR;
      default:
        return LogLevel.INFO;
    }
  }

  private formatMessage(level: string, message: string, data?: any): string {
    const timestamp = new Date().toISOString();
    let formatted = `[${timestamp}] [${level}] ${message}`;
    
    if (data && this.verbose) {
      formatted += '\n' + JSON.stringify(data, null, 2);
    }
    
    return formatted;
  }

  debug(message: string, data?: any): void {
    if (this.level <= LogLevel.DEBUG) {
      console.log(this.formatMessage('DEBUG', message, data));
    }
  }

  info(message: string, data?: any): void {
    if (this.level <= LogLevel.INFO) {
      console.log(this.formatMessage('INFO', message, data));
    }
  }

  warn(message: string, data?: any): void {
    if (this.level <= LogLevel.WARN) {
      console.warn(this.formatMessage('WARN', message, data));
    }
  }

  error(message: string, error?: any): void {
    if (this.level <= LogLevel.ERROR) {
      // Always append the error message so callers can see why an operation
      // failed even when VERBOSE_LOGS is not enabled.  The full stack trace
      // (and any extra context object) is still gated behind verbose mode.
      const suffix = error instanceof Error ? `: ${error.message}` : '';
      const errorData = this.verbose
        ? (error instanceof Error ? { message: error.message, stack: error.stack } : error)
        : undefined;
      console.error(this.formatMessage('ERROR', `${message}${suffix}`, errorData));
    }
  }
}

export const logger = new Logger(
  process.env.LOG_LEVEL || 'info',
  process.env.VERBOSE_LOGS === 'true'
);
