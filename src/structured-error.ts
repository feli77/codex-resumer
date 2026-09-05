import { AppServerRpcError } from "./app-server.js";

export interface StructuredError {
  code: string;
  details?: unknown;
  message: string;
}

export function serializeOperationalError(error: unknown): StructuredError {
  if (error instanceof AppServerRpcError) {
    return {
      code: "app_server_rpc_error",
      details: {
        data: error.data,
        method: error.method,
        rpcCode: error.rpcCode,
      },
      message: error.message,
    };
  }
  return {
    code: "daemon_error",
    message: error instanceof Error ? error.message : String(error),
  };
}
