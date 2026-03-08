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
          base: "#06080d",
          surface: "#0c1018",
          elevated: "#141a26",
          hover: "#1a2233",
          accent: "#3b82f6",
          "accent-hover": "#60a5fa",
          profit: "#34d399",
          loss: "#f87171",
          warning: "#fbbf24",
        },
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "-apple-system", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "Cascadia Code", "Fira Code", "monospace"],
      },
      boxShadow: {
        "az-panel": "0 4px 24px rgba(0, 0, 0, 0.4), 0 0 1px rgba(255, 255, 255, 0.05)",
        "az-glow-blue": "0 0 20px rgba(59, 130, 246, 0.15), 0 0 60px rgba(59, 130, 246, 0.05)",
        "az-glow-green": "0 0 20px rgba(52, 211, 153, 0.15)",
        "az-glow-red": "0 0 20px rgba(248, 113, 113, 0.15)",
      },
      backdropBlur: {
        "az": "24px",
      },
      borderRadius: {
        "az-sm": "8px",
        "az-md": "12px",
        "az-lg": "16px",
        "az-xl": "20px",
      },
      animation: {
        "pulse-glow": "pulse-glow 2s ease-in-out infinite",
        "flash-green": "flash-green 0.6s ease-out",
        "flash-red": "flash-red 0.6s ease-out",
        "fade-in-up": "fade-in-up 0.4s ease-out forwards",
        "breathe": "breathe 3s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};
export default config;