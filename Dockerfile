FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build-env
WORKDIR /app
ARG COMMIT

COPY ./NineChronicles.Snapshot.csproj ./NineChronicles.Snapshot/
RUN dotnet restore NineChronicles.Snapshot

# Copy everything else and build
COPY . ./
RUN dotnet publish NineChronicles.Snapshot.csproj \
    -c Release \
    -r linux-x64 \
    -o out \
    --self-contained \
    --version-suffix $COMMIT

# Build runtime image
FROM mcr.microsoft.com/dotnet/core/aspnet:3.1
WORKDIR /app
RUN apt-get update && apt-get install -y libc6-dev
COPY --from=build-env /app/out .

VOLUME /data

ENTRYPOINT ["dotnet", "NineChronicles.Snapshot.dll"]
