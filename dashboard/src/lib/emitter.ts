/** Minimal pub/sub for passing live data between sibling components without re-rendering the parent. */
export function createEmitter<T>() {
  const listeners = new Set<(value: T) => void>();
  return {
    emit(value: T) {
      listeners.forEach((listener) => listener(value));
    },
    subscribe(listener: (value: T) => void): () => void {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

export type Emitter<T> = ReturnType<typeof createEmitter<T>>;
