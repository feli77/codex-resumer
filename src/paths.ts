import { homedir, tmpdir } from "node:os";
import path from "node:path";

const APP_DIRECTORY = "codex-resumer";

export interface DaemonPaths {
  configDir: string;
  configPath: string;
  databasePath: string;
  lockPath: string;
  runtimeDir: string;
  socketPath: string;
  stateDir: string;
  statusPath: string;
}

export function resolvePaths(
  env: NodeJS.ProcessEnv = process.env,
  uid: number = process.getuid?.() ?? 0,
): DaemonPaths {
  const home = absoluteOr(env.HOME, homedir());
  const configBase = absoluteOr(env.XDG_CONFIG_HOME, path.join(home, ".config"));
  const stateBase = absoluteOr(
    env.XDG_STATE_HOME,
    path.join(home, ".local", "state"),
  );
  const runtimeDir = env.XDG_RUNTIME_DIR && path.isAbsolute(env.XDG_RUNTIME_DIR)
    ? path.join(env.XDG_RUNTIME_DIR, APP_DIRECTORY)
    : path.join(tmpdir(), `${APP_DIRECTORY}-${uid}`);

  return {
    configDir: path.join(configBase, APP_DIRECTORY),
    configPath: path.join(configBase, APP_DIRECTORY, "config.json"),
    databasePath: path.join(stateBase, APP_DIRECTORY, "state.sqlite3"),
    lockPath: path.join(runtimeDir, "daemon-start.lock"),
    runtimeDir,
    socketPath: path.join(runtimeDir, "daemon.sock"),
    stateDir: path.join(stateBase, APP_DIRECTORY),
    statusPath: path.join(stateBase, APP_DIRECTORY, "daemon-status.json"),
  };
}

function absoluteOr(value: string | undefined, fallback: string): string {
  return value && path.isAbsolute(value) ? value : fallback;
}
