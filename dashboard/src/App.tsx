import { useState } from "react";
import type { Connection } from "./api/client";
import { ConnectScreen } from "./components/ConnectScreen";
import { Dashboard } from "./components/Dashboard";
import { connectionStore } from "./config";

export function App() {
  const [conn, setConn] = useState<Connection | null>(() =>
    connectionStore.load(),
  );

  if (!conn) {
    return (
      <ConnectScreen
        onConnect={(c) => {
          connectionStore.save(c);
          setConn(c);
        }}
      />
    );
  }
  return (
    <Dashboard
      conn={conn}
      onDisconnect={() => {
        connectionStore.clear();
        setConn(null);
      }}
    />
  );
}
