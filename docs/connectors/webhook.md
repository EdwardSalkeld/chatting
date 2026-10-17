# Webhooks

There is currently no webhook ingress listener.

If webhook support is needed, add a dedicated route to the main message handler
and expose only that route through the firewall or reverse proxy. Normalize
requests into the existing task envelope and use the handler's task publication
path; a separate ingress process and additional BBMB queues are unnecessary.
