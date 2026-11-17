import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) } },
  server: { proxy: { '/api': process.env.CHATTING_PLATFORM_URL || 'http://127.0.0.1:9466' } },
  build: { outDir: '../go/handler/internal/platform/web/dist', emptyOutDir: true },
})
