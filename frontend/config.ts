// config.ts

const DEFAULT_BACKEND_PORT = 8000;
const envPort = Number(process.env.NEXT_PUBLIC_BACKEND_PORT);
const BACKEND_PORT = Number.isFinite(envPort) && envPort > 0 ? envPort : DEFAULT_BACKEND_PORT;
const BACKEND_HTTP_URL = process.env.NEXT_PUBLIC_BACKEND_HTTP_URL?.trim();
const BACKEND_WS_URL = process.env.NEXT_PUBLIC_BACKEND_WS_URL?.trim();

function stripTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

function getBaseHost(): string {
  if (typeof window !== "undefined") {
    return window.location.hostname;
  }
  return "127.0.0.1";
}

function getPageProtocol(): "http:" | "https:" {
  if (typeof window !== "undefined" && window.location.protocol === "https:") {
    return "https:";
  }
  return "http:";
}

function buildLocalHttpUrl(): string {
  return `${getPageProtocol()}//${getBaseHost()}:${BACKEND_PORT}`;
}

function buildLocalWsUrl(): string {
  const wsProto = getPageProtocol() === "https:" ? "wss:" : "ws:";
  return `${wsProto}//${getBaseHost()}:${BACKEND_PORT}/ws/live`;
}

function deriveWsUrlFromHttp(httpUrl: string): string {
  try {
    const parsed = new URL(httpUrl);
    parsed.protocol = parsed.protocol === "https:" ? "wss:" : "ws:";
    parsed.pathname = "/ws/live";
    parsed.search = "";
    parsed.hash = "";
    return stripTrailingSlash(parsed.toString());
  } catch {
    return buildLocalWsUrl();
  }
}

export const getBaseIp = (): string => getBaseHost();

export const getHttpUrl = (): string => {
  if (BACKEND_HTTP_URL) {
    return stripTrailingSlash(BACKEND_HTTP_URL);
  }
  return buildLocalHttpUrl();
};

export const getWsUrl = (): string => {
  if (BACKEND_WS_URL) {
    return stripTrailingSlash(BACKEND_WS_URL);
  }
  if (BACKEND_HTTP_URL) {
    return deriveWsUrlFromHttp(BACKEND_HTTP_URL);
  }
  return buildLocalWsUrl();
};
