export class RelayNodeServer {
  
  constructor()
  /** Start the server */
  start(callback: (...args: any[]) => any): void
  /** Stops the server */
  stop(): void
}
