FROM mcr.microsoft.com/dotnet/sdk:10.0@sha256:212fe4cb74627d5adf9817a4088906ae3edff1bd853bb27565d4b46731efe61e AS dotnet
FROM mcr.microsoft.com/powershell:7.5-ubuntu-24.04@sha256:042240d57ec9e47e511033b92625a8d95875ee5860af3015992c248b58a8be81 AS powershell
FROM postgres:18.6-trixie@sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git libgssapi-krb5-2 libicu76 libssl3t64 \
    openjdk-21-jre-headless procps python3 unzip \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 w82
COPY --from=dotnet /usr/share/dotnet /usr/share/dotnet
COPY --from=powershell /opt/microsoft/powershell/7 /opt/microsoft/powershell/7
ENV DOTNET_ROOT=/usr/share/dotnet
ENV PATH="/usr/share/dotnet:/opt/microsoft/powershell/7:/usr/lib/postgresql/18/bin:${PATH}"
USER w82
ENV HOME=/home/w82
RUN dotnet --version && pwsh -NoProfile -Command 'exit 0' && postgres --version && java -version
ENTRYPOINT ["/bin/bash"]
