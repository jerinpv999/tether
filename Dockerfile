FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev --ignore-scripts
COPY . .
EXPOSE 3000 50051 3101
# Same image for both applications:
#   master: node bin/master.js --config config/master-1.yaml
#   worker: node bin/worker.js --config config/worker-1.yaml
CMD ["node", "bin/master.js", "--config", "config/single-node.yaml"]
