import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{js,ts,jsx,tsx,mdx}", "./components/**/*.{js,ts,jsx,tsx,mdx}", "./lib/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      colors: {
        municipal: {
          50: "#eef6ff",
          100: "#d9ebff",
          500: "#1f6fb2",
          700: "#124a7c",
          800: "#0d355b",
          900: "#082640",
        },
      },
      boxShadow: {
        municipal: "0 16px 40px rgba(8, 38, 64, 0.12)",
      },
    },
  },
  plugins: [],
};

export default config;
