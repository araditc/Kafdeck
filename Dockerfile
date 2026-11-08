# syntax=docker/dockerfile:1
FROM node:24.22.2-alpine@sha256:a34ab6d7f6f4813f4d0c5469d74685604b962fbdbf5793a7bc7b4998ecb867c8 AS frontend
WORKDIR /src/frontend
COPY src/frontend/package*.json ./
RUN npm ci
COPY src/frontend/ ./
RUN npm run build

FROM mcr.microsoft.com/dotnet/sdk:10.0.403-noble@sha256:212fe4cb74627d5adf9817a4088906ae3edff1bd853bb27565d4b46731efe61e AS backend
WORKDIR /src
COPY . .
RUN dotnet restore Kafdeck.slnx --locked-mode
RUN dotnet publish src/backend/Kafdeck.Api/Kafdeck.Api.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0.14-noble@sha256:1a48394b6f29d3a32c79d9edecd4abda0ecbe8776d567dbfc4590ed6979c740c AS runtime
# Runtime never installs packages. Remove unused APT and libapt entirely
# (CVE-2026-109518); purge performs no package download or upgrade.
RUN apt-get -o Acquire::Retries=0 purge -y --allow-remove-essential apt libapt-pkg6.0t64 \
    && test ! -e /usr/bin/apt-get \
    && test -z "$(dpkg-query -W -f='${db:Status-Status}\n' apt libapt-pkg6.0t64 2>/dev/null | grep '^installed$')" \
    && test -z "$(find /usr/lib -name 'libapt-pkg.so*' -print -quit)" \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=backend /app/publish ./
COPY --from=frontend /src/frontend/dist ./wwwroot
ENV ASPNETCORE_ENVIRONMENT=Production
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "Kafdeck.Api.dll"]
