/** One authoritative hydration per entity; all callers share its result. */
export class SingleflightHydrator {
  private readonly inFlight = new Map<string, Promise<unknown>>();

  hydrate<T>(key: string, load: () => Promise<T>): Promise<T> {
    const active = this.inFlight.get(key) as Promise<T> | undefined;
    if (active) return active;
    const promise = Promise.resolve().then(load);
    this.inFlight.set(key, promise);
    void promise.finally(() => {
      if (this.inFlight.get(key) === promise) this.inFlight.delete(key);
    }).catch(() => undefined);
    return promise;
  }
}
