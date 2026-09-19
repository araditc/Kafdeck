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
RUN dotnet restore Kafdeck.slnx --locked-mode || dotnet restore Kafdeck.slnx
RUN dotnet publish src/backend/Kafdeck.Api/Kafdeck.Api.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=backend /app/publish ./
COPY --from=frontend /src/frontend/dist ./wwwroot
ENV ASPNETCORE_ENVIRONMENT=Production
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "Kafdeck.Api.dll"]
