export const LogLevel = {
  DEBUG: "DEBUG",
  INFO: "INFO",
  WARN: "WARN",
  ERROR: "ERROR",
} as const;

export type LogLevel = keyof typeof LogLevel;

class Logger {
  service: string;
  requestId: string;

  constructor(service: string, requestId: string) {
    this.service = service;
    this.requestId = requestId;
  }

  private log(
    level: LogLevel,
    message: string,
    context: Record<string, unknown> = {},
  ) {
    const log: Record<string, unknown> = {
      timestamp: new Date().toISOString(),
      level,
      service: this.service,
      message,
      requestId: this.requestId,
      ...context,
    };
    console.log(log);
  }

  public debug(message: string, context: Record<string, unknown> = {}): void {
    this.log(LogLevel.DEBUG, message, context);
  }
  public info(message: string, context: Record<string, unknown> = {}): void {
    this.log(LogLevel.INFO, message, context);
  }

  public warn(message: string, context: Record<string, unknown> = {}): void {
    this.log(LogLevel.WARN, message, context);
  }

  public error(message: string, context: Record<string, unknown> = {}): void {
    this.log(LogLevel.ERROR, message, context);
  }
}

export default Logger;
