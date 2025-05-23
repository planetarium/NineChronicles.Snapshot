FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build-env
WORKDIR /app
ARG COMMIT

COPY ./libplanet/Directory.Build.props ./
COPY ./libplanet/Menees.Analyzers.Settings.xml ./
COPY ./libplanet/stylecop.json ./
COPY ./libplanet/src/Directory.Build.props ./src/
COPY ./libplanet/src/Libplanet/Libplanet.csproj ./Libplanet/
COPY ./libplanet/src/Libplanet.RocksDBStore/Libplanet.RocksDBStore.csproj ./Libplanet.RocksDBStore/
COPY ./NineChronicles.Snapshot/NineChronicles.Snapshot.csproj ./NineChronicles.Snapshot/
RUN dotnet restore Libplanet
RUN dotnet restore Libplanet.RocksDBStore
RUN dotnet restore NineChronicles.Snapshot

# Copy everything else and build
COPY . ./
RUN dotnet publish NineChronicles.Snapshot/NineChronicles.Snapshot.csproj \
    -c Release \
    -r linux-x64 \
    -o out \
    --self-contained \
    --version-suffix $COMMIT

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:6.0
WORKDIR /app
RUN apt-get update && apt-get install -y \
    libc6-dev \
    librocksdb-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev
COPY --from=build-env /app/out .

VOLUME /data

ENTRYPOINT ["dotnet", "NineChronicles.Snapshot.dll"]
