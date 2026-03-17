import { tableFromIPC } from '@uwdata/flechette';

export interface StreamSourceConfig {
  /** WebSocket or SSE endpoint URL */
  url: string;
  /** Transport protocol */
  transport: 'websocket' | 'sse';
  /** Topic to subscribe to (e.g. 'traces:{groupId}') */
  topic: string;
}

export interface StreamSource {
  /** Open the connection and start receiving batches */
  connect(): void;
  /** Close the connection */
  disconnect(): void;
  /** Register a callback for incoming Arrow IPC batches */
  onBatch(callback: (ipcBytes: Uint8Array, parsed: ReturnType<typeof tableFromIPC>) => void): void;
}

/**
 * Create a live stream source that receives Arrow IPC frames via WebSocket or SSE.
 * Auto-reconnects on disconnect with exponential backoff (max 30s).
 */
export function createStreamSource(config: StreamSourceConfig): StreamSource {
  const batchCallbacks: Array<(ipcBytes: Uint8Array, parsed: ReturnType<typeof tableFromIPC>) => void> = [];
  let ws: WebSocket | null = null;
  let eventSource: EventSource | null = null;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let reconnectDelay = 1000;
  let shouldReconnect = false;

  function notifyBatch(ipcBytes: Uint8Array): void {
    const parsed = tableFromIPC(ipcBytes);
    for (const cb of batchCallbacks) {
      cb(ipcBytes, parsed);
    }
  }

  function scheduleReconnect(): void {
    if (!shouldReconnect) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connectInternal();
    }, reconnectDelay);
    // Exponential backoff, capped at 30s
    reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
  }

  function resetBackoff(): void {
    reconnectDelay = 1000;
  }

  function connectWebSocket(): void {
    const wsUrl = `${config.url}?topic=${encodeURIComponent(config.topic)}`;
    ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';

    ws.onopen = () => {
      resetBackoff();
    };

    ws.onmessage = (event: MessageEvent) => {
      if (event.data instanceof ArrayBuffer) {
        notifyBatch(new Uint8Array(event.data));
      }
    };

    ws.onclose = () => {
      ws = null;
      scheduleReconnect();
    };

    ws.onerror = () => {
      // onclose will fire after onerror, triggering reconnect
    };
  }

  function connectSSE(): void {
    const sseUrl = `${config.url}/subscribe?topic=${encodeURIComponent(config.topic)}`;
    eventSource = new EventSource(sseUrl);

    eventSource.onopen = () => {
      resetBackoff();
    };

    eventSource.onmessage = (event: MessageEvent) => {
      // SSE transport sends base64-encoded Arrow IPC in the data field
      const binary = atob(event.data);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
      }
      notifyBatch(bytes);
    };

    eventSource.onerror = () => {
      eventSource?.close();
      eventSource = null;
      scheduleReconnect();
    };
  }

  function connectInternal(): void {
    if (config.transport === 'websocket') {
      connectWebSocket();
    } else {
      connectSSE();
    }
  }

  return {
    connect(): void {
      shouldReconnect = true;
      connectInternal();
    },

    disconnect(): void {
      shouldReconnect = false;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (ws) {
        ws.close();
        ws = null;
      }
      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }
    },

    onBatch(callback): void {
      batchCallbacks.push(callback);
    },
  };
}
