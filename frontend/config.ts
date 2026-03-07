const PUBLIC_API_URL =
  process.env.NEXT_PUBLIC_API_URL?.trim() ||
  process.env.NEXT_PUBLIC_BACKEND_HTTP_URL?.trim();
const PUBLIC_WS_URL =
  process.env.NEXT_PUBLIC_WS_URL?.trim() ||
  process.env.NEXT_PUBLIC_BACKEND_WS_URL?.trim();

function stripTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

function normalizeHttpUrl(value: string): string {
  try {
    const parsed = new URL(value);
    if (parsed.protocol === "ws:") {
      parsed.protocol = "http:";
    } else if (parsed.protocol === "wss:") {
      parsed.protocol = "https:";
    }
    if (parsed.pathname.endsWith("/ws/live")) {
      parsed.pathname = parsed.pathname.slice(0, -"/ws/live".length) || "/";
    }
    parsed.search = "";
    parsed.hash = "";
    return stripTrailingSlash(parsed.toString());
  } catch {
    return stripTrailingSlash(value);
  }
}

function normalizeWsUrl(value: string): string {
  try {
    const parsed = new URL(value);
    if (parsed.protocol === "http:") {
      parsed.protocol = "ws:";
    } else if (parsed.protocol === "https:") {
      parsed.protocol = "wss:";
    }
    if (!parsed.pathname || parsed.pathname === "/") {
      parsed.pathname = "/ws/live";
    } else if (!parsed.pathname.endsWith("/ws/live")) {
      parsed.pathname = `${parsed.pathname.replace(/\/+$/, "")}/ws/live`;
    }
    parsed.search = "";
    parsed.hash = "";
    return stripTrailingSlash(parsed.toString());
  } catch {
    return stripTrailingSlash(value);
  }
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
    return "ws://localhost:8000/ws/live";
  }
}

function deriveHttpUrlFromWs(wsUrl: string): string {
  try {
    const parsed = new URL(wsUrl);
    parsed.protocol = parsed.protocol === "wss:" ? "https:" : "http:";
    parsed.pathname = "";
    parsed.search = "";
    parsed.hash = "";
    return stripTrailingSlash(parsed.toString());
  } catch {
    return "http://localhost:8000";
  }
}

export const getBaseIp = (): string => {
  try {
    const parsed = new URL(getHttpUrl());
    return parsed.hostname;
  } catch {
    return "localhost";
  }
};

export const getHttpUrl = (): string => {
  if (PUBLIC_API_URL) {
    return normalizeHttpUrl(PUBLIC_API_URL);
  }
  if (PUBLIC_WS_URL) {
    return deriveHttpUrlFromWs(PUBLIC_WS_URL);
  }
  return "http://localhost:8000";
};

export const getWsUrl = (): string => {
  if (PUBLIC_WS_URL) {
    return normalizeWsUrl(PUBLIC_WS_URL);
  }
  if (PUBLIC_API_URL) {
    return deriveWsUrlFromHttp(PUBLIC_API_URL);
  }
  return "ws://localhost:8000/ws/live";
};
