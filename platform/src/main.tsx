import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import '@fontsource-variable/inter'
import '@fontsource-variable/jetbrains-mono'
import App from './App'
import { applyTheme } from './components/theme'
import { ApiError } from './lib/api'
import './index.css'

applyTheme()
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // A 4xx (not found, bad request) will not change on retry; only retry network and 5xx.
      retry: (failures, error) =>
        failures < 1 && !(error instanceof ApiError && error.status >= 400 && error.status < 500),
      staleTime: 2000,
      refetchOnWindowFocus: true,
    },
  },
})
ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>,
)
