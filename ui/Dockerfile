FROM registry.access.redhat.com/ubi9/nodejs-20 AS build
WORKDIR /opt/app-root/src
COPY --chown=1001:0 package.json ./
RUN npm install
COPY --chown=1001:0 . .
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY nginx.conf /etc/nginx/nginx.conf
COPY --from=build /opt/app-root/src/dist /usr/share/nginx/html
EXPOSE 8080
