# Open-weight model server for Render (private service). Pulls the model on first start into the persistent disk.
FROM ollama/ollama:latest
ENV OLLAMA_HOST=0.0.0.0:11434
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 11434
ENTRYPOINT ["/entrypoint.sh"]
