# Build from the repository ROOT so the build context can see both the library and the sample:
#   docker build -f samples/Hangfire.Raft.K8sSample/Dockerfile -t hangfire-raft-sample .
#
# The projects target net10.0, so the 10.0 SDK builds them and the 10.0 ASP.NET runtime runs them.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Directory.Build.props carries repo-wide build settings, including a NuGet audit suppression for a
# transitive advisory. Copy it before restore so the in-container restore matches the host build;
# without it, audit fails the restore under TreatWarningsAsErrors.
COPY Directory.Build.props .

# Restore against just the project files first for better layer caching.
COPY src/Hangfire.Raft/Hangfire.Raft.csproj src/Hangfire.Raft/
COPY samples/Hangfire.Raft.K8sSample/Hangfire.Raft.K8sSample.csproj samples/Hangfire.Raft.K8sSample/
RUN dotnet restore samples/Hangfire.Raft.K8sSample/Hangfire.Raft.K8sSample.csproj

COPY src/Hangfire.Raft/ src/Hangfire.Raft/
COPY samples/Hangfire.Raft.K8sSample/ samples/Hangfire.Raft.K8sSample/
RUN dotnet publish samples/Hangfire.Raft.K8sSample/Hangfire.Raft.K8sSample.csproj -c Release --no-restore -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .

# 5000 Raft consensus, 5001 leader-forwarding (Raft + RpcPortOffset), 8080 HTTP (dashboard + health).
EXPOSE 5000 5001 8080

# Run unprivileged under a fixed numeric UID that matches runAsUser/fsGroup in the Kubernetes manifests,
# so the PVC-backed WAL at /data/wal stays writable. The app needs no /etc/passwd entry for this UID.
USER 64198

ENTRYPOINT ["dotnet", "Hangfire.Raft.K8sSample.dll"]
