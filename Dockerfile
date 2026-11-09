FROM node:20-alpine
WORKDIR /app
COPY package.json server.mjs ./
COPY public ./public
COPY index.html standalone-offline-index.html ./
RUN mkdir -p /app/data/attachments /app/data/backups
VOLUME ["/app/data"]
ENV NODE_ENV=production
ENV PORT=3000
EXPOSE 3000
CMD ["node", "server.mjs"]
