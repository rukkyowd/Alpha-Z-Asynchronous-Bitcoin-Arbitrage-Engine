const PUBLIC_API_URL =
  process.env.NEXT_PUBLIC_API_URL?.trim() ||
  process.env.NEXT_PUBLIC_BACKEND_HTTP_URL?.trim();
const PUBLIC_WS_URL =
  process.env.NEXT_PUBLIC_WS_URL?.trim() ||
  process.env.NEXT_PUBLIC_BACKEND_WS_URL?.trim();

function stripTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
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
    return stripTrailingSlash(PUBLIC_API_URL);
  }
  return "http://localhost:8000";
};

export const getWsUrl = (): string => {
  if (PUBLIC_WS_URL) {
    return stripTrailingSlash(PUBLIC_WS_URL);
  }
  if (PUBLIC_API_URL) {
    return deriveWsUrlFromHttp(PUBLIC_API_URL);
  }
  return "ws://localhost:8000/ws/live";
};
