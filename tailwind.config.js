/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./views/**/*.ejs"],
  theme: {
    extend: {
      fontFamily: {
        display: ["Sora", "ui-sans-serif", "system-ui", "sans-serif"],
        body: ["Manrope", "ui-sans-serif", "system-ui", "sans-serif"],
      },
      colors: {
        ink: "#0B1221",
        mist: "#EEF6FF",
        electric: "#00AEEF",
        coral: "#FF6B4A",
        neon: "#25E89B",
      },
      boxShadow: {
        soft: "0 10px 30px rgba(10, 20, 40, 0.08)",
      },
    },
  },
  plugins: [],
}
