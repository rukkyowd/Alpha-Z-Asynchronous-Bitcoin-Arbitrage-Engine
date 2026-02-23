// config.ts

const BACKEND_PORT = 8000;

export const getBaseIp = () => {
  // If we are in the browser, dynamically use whatever IP is in the URL bar
  if (typeof window !== "undefined") {
    return window.location.hostname;
  }
  // Safe fallback for server-side rendering
  return "127.0.0.1";
};

export const getWsUrl = () => {
  return `ws://${getBaseIp()}:${BACKEND_PORT}/ws/live`;
};

export const getHttpUrl = () => {
  return `http://${getBaseIp()}:${BACKEND_PORT}`;
};