import { defineConfig } from "vite";
import solidPlugin from "vite-plugin-solid";

const ServerUrl = "http://127.0.0.1:5000";

export default defineConfig({
    plugins: [solidPlugin()],
    server: {
        port: 3000,
        proxy: {
            "/api": {
                target: ServerUrl,
                changeOrigin: true,
            },
        },
    },
    build: {
        target: "esnext",
    },
});
