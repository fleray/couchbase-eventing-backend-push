# Contexte de build = racine du projet (le backend sert aussi ../client).
FROM node:22-alpine

WORKDIR /app/server
COPY server/package.json server/package-lock.json ./
RUN npm ci --omit=dev
COPY server/ ./
COPY client/ /app/client/

ENV NODE_ENV=production
EXPOSE 3000
USER node
CMD ["node", "server.js"]
