# syntax=docker/dockerfile:1
FROM node:20-alpine AS build
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --ignore-scripts
COPY dashboard/ ./
COPY api /api
ARG VITE_QUERY_URL=http://localhost:8081
ENV VITE_QUERY_URL=${VITE_QUERY_URL}
RUN npm run build

FROM nginx:1.31-alpine
ENV CSP_CONNECT_SRC="'self' http://localhost:8081"
COPY dashboard/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
