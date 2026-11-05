# syntax=docker/dockerfile:1
FROM node:24-alpine AS frontend
WORKDIR /src/frontend
COPY src/frontend/package*.json ./
RUN npm ci
COPY src/frontend/ ./
RUN npm run build

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS backend
WORKDIR /src
COPY . .
RUN dotnet restore Kafdeck.slnx --locked-mode
RUN dotnet publish src/backend/Kafdeck.Api/Kafdeck.Api.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
# USN-8996-1 / CVE-2026-109518: upgrade both vulnerable APT packages.
# Disable retries while using the older bootstrap client and require HTTPS.
RUN sed -i 's|http://|https://|g' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get -o Acquire::Retries=0 update \
    && apt-get -o Acquire::Retries=0 install -y --no-install-recommends --only-upgrade apt libapt-pkg6.0t64 \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' apt)" ge 2.8.4ubuntu0.1 \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libapt-pkg6.0t64)" ge 2.8.4ubuntu0.1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=backend /app/publish ./
COPY --from=frontend /src/frontend/dist ./wwwroot
ENV ASPNETCORE_ENVIRONMENT=Production
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "Kafdeck.Api.dll"]
