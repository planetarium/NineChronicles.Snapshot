using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Formats.Tar;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Bencodex.Types;
using Cocona;
using EasyCompressor;
using Libplanet.Action;
using Libplanet.Action.Loader;
using Libplanet.Types.Blocks;
using Libplanet.RocksDBStore;
using Libplanet.Store;
using Libplanet.Blockchain;
using Libplanet.Blockchain.Policies;
using Libplanet.Common;
using Libplanet.Crypto;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using ILogger = Serilog.ILogger;

namespace NineChronicles.Snapshot
{
    class Program
    {
        public enum SnapshotType { Full, Partition, All }

        private enum ArchiveType { Zip, TarZstd }

        private static readonly IReadOnlyDictionary<ArchiveType, string> _archiveExtensions = new Dictionary<ArchiveType, string>
        {
            { ArchiveType.Zip, "zip" },
            { ArchiveType.TarZstd, "tar.zst" }
        };

        private int _compressionLevel;

        private RocksDBStore _store;
        private TrieStateStore _stateStore;
        private ILogger _logger;

        private ArchiveType _archiveType = ArchiveType.Zip;
        private string ArchiveExtension { get => _archiveExtensions[_archiveType]; }

        static void Main(string[] args)
        {
            CoconaLiteApp.Run<Program>(args);
        }

        [Command]
        public void Snapshot(
            string apv,
            [Option('o')]
            string outputDirectory,
            [Option("bypass-copystates")]
            bool bypassCopyStates = false,
            bool zstd = false,
            int compressionLevel = 0,
            string storePath = null,
            int blockBefore = 1,
            SnapshotType snapshotType = SnapshotType.Partition)
        {
            try
            {
                var configurationBuilder = new ConfigurationBuilder();
                configurationBuilder.AddJsonFile("appsettings.json");
                var configuration = configurationBuilder.Build();
                var loggerConf = new LoggerConfiguration()
                    .ReadFrom.Configuration(configuration);
                _logger = loggerConf.CreateLogger();

                if (zstd)
                {
                    _logger.Debug("Compression method: Zstd (Tar)");
                    _archiveType = ArchiveType.TarZstd;
                }
                else
                {
                    _logger.Debug("Compression method: Zip (Default)");
                }
                _logger.Debug("Compression level: {CompressionLevel}", compressionLevel);
                _compressionLevel = compressionLevel;

                var snapshotStart = DateTimeOffset.Now;
                _logger.Debug($"Create Snapshot-{snapshotType.ToString()} start.");

                // If store changed epoch unit seconds, this will be changed too
                const int epochUnitSeconds = 86400;
                string defaultStorePath = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "planetarium",
                    "9c"
                );

                if (blockBefore < 0)
                {
                    throw new CommandExitedException("The --block-before option must be greater than or equal to 0.", -1);
                }

                Directory.CreateDirectory(outputDirectory);
                Directory.CreateDirectory(Path.Combine(outputDirectory, "partition"));
                Directory.CreateDirectory(Path.Combine(outputDirectory, "state"));
                Directory.CreateDirectory(Path.Combine(outputDirectory, "metadata"));
                Directory.CreateDirectory(Path.Combine(outputDirectory, "full"));

                outputDirectory = string.IsNullOrEmpty(outputDirectory)
                    ? Environment.CurrentDirectory
                    : outputDirectory;

                var metadataDirectory = Path.Combine(outputDirectory, "metadata");
                int currentMetadataBlockEpoch = GetMetaDataEpoch(metadataDirectory, "BlockEpoch");
                int currentMetadataTxEpoch = GetMetaDataEpoch(metadataDirectory, "TxEpoch");
                int previousMetadataBlockEpoch = GetMetaDataEpoch(metadataDirectory, "PreviousBlockEpoch");

                storePath = string.IsNullOrEmpty(storePath) ? defaultStorePath : storePath;
                if (!Directory.Exists(storePath))
                {
                    throw new CommandExitedException("Invalid store path. Please check --store-path is valid.", -1);
                }

                var statesPath = Path.Combine(storePath, "states");
                var mainPath = Path.Combine(storePath, "9c-main");
                var stateRefPath = Path.Combine(storePath, "stateref");
                var statePath = Path.Combine(storePath, "state");
                var newStatesPath = Path.Combine(storePath, "new_states");
                var stateHashesPath = Path.Combine(storePath, "state_hashes");

                var staleDirectories = new[] { mainPath, statePath, stateRefPath, stateHashesPath, newStatesPath };
                foreach (var staleDirectory in staleDirectories)
                {
                    if (Directory.Exists(staleDirectory))
                    {
                        Directory.Delete(staleDirectory, true);
                    }
                }

                if (RocksDBStore.MigrateChainDBFromColumnFamilies(Path.Combine(storePath, "chain")))
                {
                    _logger.Debug("Successfully migrated IndexDB.");
                }
                else
                {
                    _logger.Debug("Migration not required.");
                }

                _store = new RocksDBStore(storePath);
                var stateKeyValueStore = new RocksDBKeyValueStore(statesPath);
                _stateStore = new TrieStateStore(stateKeyValueStore);

                var canonicalChainId = _store.GetCanonicalChainId();
                if (!(canonicalChainId is { } chainId))
                {
                    throw new CommandExitedException("Canonical chain doesn't exist.", -1);
                }

                var genesisHash = _store.IterateIndexes(chainId, 0, 1).First();
                var tipHash = _store.IndexBlockHash(chainId, -1)
                    ?? throw new CommandExitedException("The given chain seems empty.", -1);
                if (!(_store.GetBlockIndex(tipHash) is { } tipIndex))
                {
                    throw new CommandExitedException(
                        $"The index of {tipHash} doesn't exist.",
                        -1);
                }

                IStagePolicy stagePolicy = new VolatileStagePolicy();
                IBlockPolicy blockPolicy =
                    new BlockPolicy();
                var blockChainStates = new BlockChainStates(_store, _stateStore);
                var actionEvaluator = new ActionEvaluator(
                    blockPolicy.PolicyActionsRegistry,
                    _stateStore,
                    new NCActionLoader()
                    );
                var tip = _store.GetBlock(tipHash);

                var potentialSnapshotTipIndex = tipIndex - blockBefore;
                var potentialSnapshotTipHash = (BlockHash)_store.IndexBlockHash(chainId, potentialSnapshotTipIndex)!;
                var snapshotTip = _store.GetBlock(potentialSnapshotTipHash);

                _logger.Debug("Original Store Tip: #{0}\n1. LastCommit: {1}\n2. BlockCommit in Chain: {2}\n3. BlockCommit in Store: {3}",
                    tip.Index, tip.LastCommit, GetChainBlockCommit(tipHash, chainId), _store.GetBlockCommit(tipHash));
                _logger.Debug("Potential Snapshot Tip: #{0}\n1. LastCommit: {1}\n2. BlockCommit in Chain: {2}\n3. BlockCommit in Store: {3}",
                    potentialSnapshotTipIndex, snapshotTip.LastCommit, GetChainBlockCommit(potentialSnapshotTipHash, chainId), _store.GetBlockCommit(potentialSnapshotTipHash));

                var tipBlockCommit = _store.GetBlockCommit(tipHash) ??
                    GetChainBlockCommit(tipHash, chainId);
                var potentialSnapshotTipBlockCommit = _store.GetBlockCommit(potentialSnapshotTipHash) ??
                    GetChainBlockCommit(potentialSnapshotTipHash, chainId);

                // Add tip and the snapshot tip's block commit to store to avoid block validation during preloading
                if (potentialSnapshotTipBlockCommit != null)
                {
                    _logger.Debug("Adding the tip(#{0}) and the snapshot tip(#{1})'s block commit to the store", tipIndex, snapshotTip.Index);
                    _store.PutBlockCommit(tipBlockCommit);
                    _store.PutChainBlockCommit(chainId, tipBlockCommit);
                    _store.PutBlockCommit(potentialSnapshotTipBlockCommit);
                    _store.PutChainBlockCommit(chainId, potentialSnapshotTipBlockCommit);
                }
                else
                {
                    _logger.Debug("There is no block commit associated with the potential snapshot tip: #{0}. Snapshot will automatically truncate 1 more block from the original chain tip.",
                        potentialSnapshotTipIndex);
                    blockBefore += 1;
                    potentialSnapshotTipBlockCommit = _store.GetBlock((BlockHash)_store.IndexBlockHash(chainId, tip.Index - blockBefore + 1)!).LastCommit;
                    _store.PutBlockCommit(tipBlockCommit);
                    _store.PutChainBlockCommit(chainId, tipBlockCommit);
                    _store.PutBlockCommit(potentialSnapshotTipBlockCommit);
                    _store.PutChainBlockCommit(chainId, potentialSnapshotTipBlockCommit);
                }

                var blockCommitBlock = _store.GetBlock(tipHash);

                // Add last block commits to store from tip until --block-before + 5 for buffer
                for (var i = 0; i < blockBefore + 5; i++)
                {
                    _logger.Debug("Adding block #{0}'s block commit to the store", blockCommitBlock.Index - 1);
                    _store.PutBlockCommit(blockCommitBlock.LastCommit);
                    _store.PutChainBlockCommit(chainId, blockCommitBlock.LastCommit);
                    blockCommitBlock = _store.GetBlock((BlockHash)blockCommitBlock.PreviousHash!);
                }

                var snapshotTipIndex = Math.Max(tipIndex - (blockBefore + 1), 0);
                BlockHash snapshotTipHash;

                do
                {
                    snapshotTipIndex++;

                    if (!(_store.IndexBlockHash(chainId, snapshotTipIndex) is { } hash))
                    {
                        throw new CommandExitedException(
                            $"The index {snapshotTipIndex} doesn't exist on ${chainId}.",
                            -1);
                    }

                    snapshotTipHash = hash;
                } while (!_stateStore.GetStateRoot(_store.GetBlock(snapshotTipHash).StateRootHash).Recorded);

                var forkedId = Guid.NewGuid();
                Fork(chainId, forkedId, snapshotTipHash, tip);

                _store.SetCanonicalChainId(forkedId);
                foreach (var id in _store.ListChainIds().Where(id => !id.Equals(forkedId)))
                {
                    _store.DeleteChainId(id);
                }

                var snapshotTipDigest = _store.GetBlockDigest(snapshotTipHash);
                var snapshotTipStateRootHash = _store.GetStateRootHash(snapshotTipHash);
                ImmutableHashSet<HashDigest<SHA256>> stateHashes = ImmutableHashSet<HashDigest<SHA256>>.Empty.Add((HashDigest<SHA256>)snapshotTipStateRootHash!);

                // Get 2 block digest before snapshot tip using snapshot previous block hash.
                BlockHash? previousBlockHash = snapshotTipDigest?.Hash;
                int count = 0;
                const int maxStateDepth = 2;

                while (previousBlockHash is { } pbh &&
                       _store.GetBlockDigest(pbh) is { } previousBlockDigest &&
                       count < maxStateDepth)
                {
                    stateHashes = stateHashes.Add(previousBlockDigest.StateRootHash);
                    previousBlockHash = previousBlockDigest.PreviousHash;
                    count++;
                }

                var newTipHash = _store.IndexBlockHash(forkedId, -1)
                    ?? throw new CommandExitedException("The given chain seems empty.", -1);
                var newTip = _store.GetBlock(newTipHash);
                var latestEpoch = (int)(newTip.Timestamp.ToUnixTimeSeconds() / epochUnitSeconds);
                _logger.Debug("Official Snapshot Tip: #{0}\n1. Timestamp: {1}\n2. Latest Epoch: {2}\n3. BlockCommit in Chain: {3}\n4. BlockCommit in Store: {4}",
                    newTip.Index, newTip.Timestamp.UtcDateTime, latestEpoch, GetChainBlockCommit(newTip.Hash, forkedId), _store.GetBlockCommit(newTip.Hash));

                DateTimeOffset start;

                if (bypassCopyStates)
                {
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} CopyStates Skipped.");
                }
                else
                {
                    var newStateKeyValueStore = new RocksDBKeyValueStore(newStatesPath);
                    var newStateStore = new TrieStateStore(newStateKeyValueStore);

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} CopyStates Start.");
                    start = DateTimeOffset.Now;
                    _stateStore.CopyStates(stateHashes, newStateStore);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} CopyStates Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                    newStateStore.Dispose();
                    newStateKeyValueStore.Dispose();
                }

                _store.Dispose();
                _stateStore.Dispose();
                stateKeyValueStore.Dispose();

                if (Directory.Exists(newStatesPath))
                {
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Determining State Sizes Start.");
                    var statesPathSize = Directory.GetFiles(statesPath, "*", SearchOption.AllDirectories).Sum(file => new FileInfo(file).Length);
                    var newStatesPathSize = Directory.GetFiles(newStatesPath, "*", SearchOption.AllDirectories).Sum(file => new FileInfo(file).Length);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Previous States Size: {(float)statesPathSize / 1024 / 1024 / 1024} GiB");
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} New States Size: {(float)newStatesPathSize / 1024 / 1024 / 1024} GiB");

                    // _logger.Debug($"Snapshot-{snapshotType.ToString()} Move States Start.");
                    // start = DateTimeOffset.Now;
                    // Directory.Delete(statesPath, recursive: true);
                    // Directory.Move(newStatesPath, statesPath);
                    // _logger.Debug($"Snapshot-{snapshotType.ToString()} Move States Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min");
                }

                // var partitionBaseFilename = GetPartitionBaseFileName(
                //     currentMetadataBlockEpoch,
                //     currentMetadataTxEpoch,
                //     latestEpoch);
                // var stateBaseFilename = "state_latest";
                //
                // var fullSnapshotDirectory = Path.Combine(outputDirectory, "full");
                // var genesisHashHex = ByteUtil.Hex(genesisHash.ToByteArray());
                // var snapshotTipHashHex = ByteUtil.Hex(snapshotTipHash.ToByteArray());
                // var fullSnapshotFilename = $"{genesisHashHex}-snapshot-{snapshotTipHashHex}-{snapshotTipIndex}.{ArchiveExtension}";
                // var fullSnapshotPath = Path.Combine(fullSnapshotDirectory, fullSnapshotFilename);
                //
                // var partitionSnapshotFilename = $"{partitionBaseFilename}.{ArchiveExtension}";
                // var partitionSnapshotPath = Path.Combine(outputDirectory, "partition", partitionSnapshotFilename);
                // var stateSnapshotFilename = $"{stateBaseFilename}.{ArchiveExtension}";
                // var stateSnapshotPath = Path.Combine(outputDirectory, "state", stateSnapshotFilename);
                //
                // _logger.Debug($"Snapshot-{snapshotType.ToString()} Clean Store Start.");
                // start = DateTimeOffset.Now;
                // CleanStore(
                //     partitionSnapshotPath,
                //     stateSnapshotPath,
                //     fullSnapshotPath,
                //     storePath);
                // _logger.Debug($"Snapshot-{snapshotType.ToString()} Clean Store Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");
                //
                // if (snapshotType == SnapshotType.Full || snapshotType == SnapshotType.All)
                // {
                //     _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Full ZipFile Start.");
                //     start = DateTimeOffset.Now;
                //     ArchiveDirectory(fullSnapshotPath, storePath);
                //     _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Full ZipFile Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");
                // }
                //
                // if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                // {
                //     var epochLimit = GetEpochLimit(
                //         latestEpoch,
                //         currentMetadataBlockEpoch,
                //         previousMetadataBlockEpoch);
                //
                //     _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Partition Archive Start.");
                //     start = DateTimeOffset.Now;
                //     ArchiveDirectory(partitionSnapshotPath, storePath, epochLimit, new[] { "block", "tx" }, new[] { "blockindex", "txindex" });
                //     _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Partition Archive Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");
                //
                //     _logger.Debug($"Snapshot-{snapshotType.ToString()} Create State Archive Start.");
                //     start = DateTimeOffset.Now;
                //     ArchiveDirectory(stateSnapshotPath, storePath, subDirs: new[] {
                //         "block/blockindex",
                //         "tx/txindex",
                //         "txbindex",
                //         "states",
                //         "chain",
                //         "blockcommit",
                //         "txexec"
                //     });
                //     _logger.Debug($"Snapshot-{snapshotType.ToString()} Create State Archive Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");
                //
                //     if (snapshotTipDigest is null)
                //     {
                //         throw new CommandExitedException("Tip does not exist.", -1);
                //     }
                //
                //     string stringfyMetadata = CreateMetadata(
                //         snapshotTipDigest.Value,
                //         apv,
                //         currentMetadataBlockEpoch,
                //         currentMetadataTxEpoch,
                //         previousMetadataBlockEpoch,
                //         latestEpoch);
                //     var metadataFilename = $"{partitionBaseFilename}.json";
                //     var metadataPath = Path.Combine(metadataDirectory, metadataFilename);
                //
                //     if (File.Exists(metadataPath))
                //     {
                //         File.Delete(metadataPath);
                //     }
                //
                //     File.WriteAllText(metadataPath, stringfyMetadata);
                // }
                //
                // _logger.Debug($"Create Snapshot-{snapshotType.ToString()} Complete. Time Taken: {(DateTimeOffset.Now - snapshotStart).TotalMinutes} min.");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message);
                _logger.Error(ex.StackTrace);
            }
        }

        private string GetPartitionBaseFileName(
            int currentMetadataBlockEpoch,
            int currentMetadataTxEpoch,
            int latestEpoch
        )
        {
            // decrease latest epochs by 1 when creating genesis snapshot
            if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
            {
                return $"snapshot-{latestEpoch - 1}-{latestEpoch - 1}";
            }
            else
            {
                return $"snapshot-{latestEpoch}-{latestEpoch}";
            }
        }

        private int GetEpochLimit(
            int latestEpoch,
            int currentMetadataEpoch,
            int previousMetadataEpoch
        )
        {
            if (latestEpoch == currentMetadataEpoch)
            {
                // case when all epochs are the same
                if (latestEpoch == previousMetadataEpoch)
                {
                    // return previousMetadataEpoch - 1
                    // to save previous epoch in snapshot
                    return previousMetadataEpoch - 1;
                }

                // case when metadata points to genesis snapshot
                if (previousMetadataEpoch == 0)
                {
                    return currentMetadataEpoch - 1;
                }

                return previousMetadataEpoch;
            }

            return currentMetadataEpoch;
        }

        private string CreateMetadata(
            BlockDigest snapshotTipDigest,
            string apv,
            int currentMetadataBlockEpoch,
            int currentMetadataTxEpoch,
            int previousMetadataBlockEpoch,
            int latestEpoch)
        {
            BlockHeader snapshotTipHeader = snapshotTipDigest.GetHeader();
            JObject jsonObject = JObject.FromObject(snapshotTipHeader);
            jsonObject.Add("APV", apv);

            jsonObject = AddPreviousEpochs(
                jsonObject,
                currentMetadataBlockEpoch,
                previousMetadataBlockEpoch,
                latestEpoch,
                "PreviousBlockEpoch",
                "PreviousTxEpoch");

            // decrease latest epochs by 1 for genesis snapshot
            if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
            {
                jsonObject.Add("BlockEpoch", latestEpoch - 1);
                jsonObject.Add("TxEpoch", latestEpoch - 1);
            }
            else
            {
                jsonObject.Add("BlockEpoch", latestEpoch);
                jsonObject.Add("TxEpoch", latestEpoch);
            }

            return JsonConvert.SerializeObject(jsonObject);
        }

        private void CleanStore(
            string partitionSnapshotPath,
            string stateSnapshotPath,
            string fullSnapshotPath,
            string storePath)
        {
            if (File.Exists(partitionSnapshotPath))
            {
                File.Delete(partitionSnapshotPath);
            }

            if (File.Exists(stateSnapshotPath))
            {
                File.Delete(stateSnapshotPath);
            }

            if (File.Exists(fullSnapshotPath))
            {
                File.Delete(fullSnapshotPath);
            }

            var cleanDirectories = new[]
            {
                Path.Combine(storePath, "blockpercept"),
                Path.Combine(storePath, "stagedtx")
            };

            foreach (var path in cleanDirectories)
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, true);
                }
            }
        }

        private void CleanPartitionStore(string partitionDirectory)
        {
            var cleanDirectories = new[]
            {
                Path.Combine(partitionDirectory, "block", "blockindex"),
                Path.Combine(partitionDirectory, "tx", "txindex"),
            };

            foreach (var path in cleanDirectories)
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, true);
                }
            }
        }

        private void Fork(
            Guid src,
            Guid dest,
            BlockHash branchpointHash,
            Block tip)
        {
            _store.ForkBlockIndexes(src, dest, branchpointHash);
            if (_store.GetBlockCommit(branchpointHash) is { })
            {
                _store.PutChainBlockCommit(dest, _store.GetBlockCommit(branchpointHash));
            }
            _store.ForkTxNonces(src, dest);

            for (
                Block block = tip;
                block.PreviousHash is { } hash
                && !block.Hash.Equals(branchpointHash);
                block = _store.GetBlock(hash))
            {
                IEnumerable<(Address, int)> signers = block
                    .Transactions
                    .GroupBy(tx => tx.Signer)
                    .Select(g => (g.Key, g.Count()));

                foreach ((Address address, int txCount) in signers)
                {
                    _store.IncreaseTxNonce(dest, address, -txCount);
                }
            }
        }

        private int GetMetaDataEpoch(
            string outputDirectory,
            string epochType)
        {
            try
            {
                string previousMetadata = Directory.GetFiles(outputDirectory)
                    .Where(x => Path.GetExtension(x) == ".json")
                    .OrderByDescending(x => File.GetLastWriteTime(x))
                    .First();
                var jsonObject = JObject.Parse(File.ReadAllText(previousMetadata));
                return (int)jsonObject[epochType];
            }
            catch (InvalidOperationException ex)
            {
                _logger.Error(ex.Message);
                _logger.Error(ex.StackTrace);
                return 0;
            }
        }

        private void ArchiveDirectory(
            string destPath,
            string srcDirPath,
            int? epochLimit = null,
            string[] subDirs = null,
            string[] excludeDirs = null)
        {
            var archiveEntries = ArchivePaths(srcDirPath, epochLimit, subDirs, excludeDirs)
                .Select(path => Path.GetRelativePath(srcDirPath, path));

            try
            {
                using FileStream destStream = File.Create(destPath);
                if (_archiveType == ArchiveType.TarZstd)
                {
                    var compressor = new ZstdSharpCompressor(this._compressionLevel);

                    using FileStream destTarStream = File.Create(Path.GetTempFileName(), 4096, FileOptions.DeleteOnClose);
                    using TarWriter tarWriter = new TarWriter(destTarStream, TarEntryFormat.Pax);
                    foreach (var entry in archiveEntries)
                    {
                        using var input = File.OpenRead(Path.Combine(srcDirPath, entry));
                        tarWriter.WriteEntry(new PaxTarEntry(TarEntryType.RegularFile, entry) { DataStream = input });
                    }
                    destTarStream.Seek(0, SeekOrigin.Begin);

                    compressor.Compress(destTarStream, destStream);
                }
                else if (_archiveType == ArchiveType.Zip)
                {
                    using ZipArchive zipArchive = new ZipArchive(destStream, ZipArchiveMode.Create);
                    foreach (var entry in archiveEntries)
                    {
                        zipArchive.CreateEntryFromFile(Path.Combine(srcDirPath, entry), entry, (CompressionLevel)_compressionLevel);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message);
                _logger.Error(ex.StackTrace);
            }
        }

        private string[] ArchivePaths(
            string dirPath,
            int? epochLimit = null,
            string[] subDirs = null,
            string[] excludeDirs = null)
        {
            if (subDirs != null)
            {
                return subDirs.SelectMany(subDir => ArchivePaths(
                    Path.Combine(dirPath, subDir),
                    epochLimit: epochLimit,
                    excludeDirs: excludeDirs)
                ).ToArray();
            }

            var dirName = dirPath.Split("/").Last();

            if ((excludeDirs is { } && excludeDirs.Contains(dirName))
                || (epochLimit.HasValue
                    && int.TryParse(Regex.Match(dirName, @"^epoch(\d+)$").Groups[1].Value, out int epoch)
                    && epoch < epochLimit.Value))
            {
                return new[] { "" };
            }

            var files = Directory.GetFiles(dirPath);
            var directories = Directory.GetDirectories(dirPath)
                .SelectMany(subdir => ArchivePaths(subdir, epochLimit, excludeDirs: excludeDirs))
                .Where(path => path != "");

            return files.Concat(directories).ToArray();
        }

        private JObject AddPreviousEpochs(
            JObject jsonObject,
            int currentMetadataEpoch,
            int previousMetadataEpoch,
            int latestEpoch,
            string blockEpochName,
            string txEpochName)
        {
            if (currentMetadataEpoch == latestEpoch)
            {
                jsonObject.Add(blockEpochName, previousMetadataEpoch);
                jsonObject.Add(txEpochName, previousMetadataEpoch);
            }
            else
            {
                jsonObject.Add(blockEpochName, currentMetadataEpoch);
                jsonObject.Add(txEpochName, currentMetadataEpoch);
            }

            return jsonObject;
        }

        private BlockCommit GetChainBlockCommit(BlockHash blockHash, Guid chainId)
        {
            var tipHash = _store.IndexBlockHash(chainId, -1)
                ?? throw new CommandExitedException("The given chain seems empty.", -1);
            if (!(_store.GetBlockIndex(tipHash) is { } tipIndex))
            {
                throw new CommandExitedException(
                    $"The index of {tipHash} doesn't exist.",
                    -1);
            }

            if (!(_store.GetBlockIndex(blockHash) is { } blockIndex))
            {
                throw new CommandExitedException(
                    $"The index of {blockHash} doesn't exist.",
                    -1);
            }

            if (blockIndex == tipIndex)
            {
                return _store.GetChainBlockCommit(chainId);
            }

            if (!(_store.IndexBlockHash(chainId, blockIndex + 1) is { } nextHash))
            {
                throw new CommandExitedException(
                    $"The hash of index {blockIndex + 1} doesn't exist.",
                    -1);
            }

            return _store.GetBlock(nextHash).LastCommit;
        }

        public class NCActionLoader : IActionLoader
        {
            private readonly IActionLoader _actionLoader;
            public IAction LoadAction(long index, IValue value) => _actionLoader.LoadAction(index, value);
        }
    }
}
