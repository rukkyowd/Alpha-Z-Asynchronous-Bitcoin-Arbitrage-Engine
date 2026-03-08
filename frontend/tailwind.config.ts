import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        az: {
          bg: "#0b0e14",         // Nearly black background
          surface: "#11141d",    // Slightly elevated base panel
          "surface-2": "#1a1e29",// Secondary panel background or hover
          border: "#2b2f3a",     // Subtle divider
          text: "#f8f9fa",       // Primary white-ish text
          "text-muted": "#848e9c",// Dimmed secondary text
          accent: "#3b82f6",     // Action color
          profit: "#2ebc85",     // Sharp crypto green
          "profit-muted": "rgba(46, 188, 133, 0.15)",
          loss: "#f6465d",       // Sharp crypto red
          "loss-muted": "rgba(246, 70, 93, 0.15)",
          warning: "#f5b300",    // Warning amber
          chip: "#242835",       // Neutral chip background
          "chip-active": "#3b82f6",// Active chip outline/bg
        },
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "-apple-system", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "Cascadia Code", "Fira Code", "monospace"],
      },
      animation: {
        "flash-up": "flash-up 1s ease-out",
        "flash-down": "flash-down 1s ease-out",
        "live-ping": "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
      },
      keyframes: {
        "flash-up": {
          "0%": { backgroundColor: "rgba(46, 188, 133, 0.25)" },
          "100%": { backgroundColor: "transparent" },
        },
        "flash-down": {
          "0%": { backgroundColor: "rgba(246, 70, 93, 0.25)" },
          "100%": { backgroundColor: "transparent" },
        },
      },
    },
  },
  plugins: [],
};
export default config;