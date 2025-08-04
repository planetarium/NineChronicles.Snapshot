using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using Bencodex.Types;
using Cocona;
using Libplanet.Action;
using Libplanet.Action.Loader;
using Libplanet.Types.Blocks;
using Libplanet.RocksDBStore;
using Libplanet.Store;
using Libplanet.Store.Trie;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Libplanet.Blockchain;
using Libplanet.Blockchain.Policies;
using Libplanet.Common;
using Libplanet.Crypto;
using Microsoft.Extensions.Configuration;
using Serilog;
using ILogger = Serilog.ILogger;

namespace NineChronicles.Snapshot
{
    class Program
    {
        private RocksDBStore _store;
        private TrieStateStore _stateStore;
        private ILogger _logger;

        public enum SnapshotType { Full, Partition, All }

        private static readonly List<string> STATE_PATHS = new List<string>() {
            Path.Combine("block", "blockindex"),
            Path.Combine("tx", "txindex"),
            Path.Combine("txbindex"),
            Path.Combine("states"),
            Path.Combine("chain"),
            Path.Combine("blockcommit"),
            Path.Combine("txexec")
        };

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
                Directory.CreateDirectory(Path.Combine(outputDirectory, "temp"));

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

                var staleDirectories =
                new[] { mainPath, statePath, stateRefPath, stateHashesPath };
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
                IKeyValueStore stateKeyValueStore = new RocksDBKeyValueStore(statesPath);
                IKeyValueStore newStateKeyValueStore = new RocksDBKeyValueStore(newStatesPath);
                _stateStore = new TrieStateStore(stateKeyValueStore);
                var newStateStore = new TrieStateStore(newStateKeyValueStore);

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
                    _ => blockPolicy.BlockAction,
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
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} CopyStates Start.");
                    start = DateTimeOffset.Now;
                    _stateStore.CopyStates(stateHashes, newStateStore);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} CopyStates Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Determining State Sizes Start.");
                    var statesPathSize = Directory.GetFiles(statesPath, "*", SearchOption.AllDirectories).Sum(file => new FileInfo(file).Length);
                    var newStatesPathSize = Directory.GetFiles(newStatesPath, "*", SearchOption.AllDirectories).Sum(file => new FileInfo(file).Length);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Previous States Size: {(float)statesPathSize / 1024 / 1024 / 1024} GiB");
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} New States Size: {(float)newStatesPathSize / 1024 / 1024 / 1024} GiB");

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Move States Start.");
                    start = DateTimeOffset.Now;
                    Directory.Delete(statesPath, recursive: true);
                    Directory.Move(newStatesPath, statesPath);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Move States Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min");
                }

                _store.Dispose();
                _stateStore.Dispose();
                stateKeyValueStore.Dispose();
                newStateStore.Dispose();
                newStateKeyValueStore.Dispose();

                var partitionBaseFilename = GetPartitionBaseFileName(
                    currentMetadataBlockEpoch,
                    currentMetadataTxEpoch,
                    latestEpoch);
                var stateBaseFilename = "state_latest";

                var fullSnapshotDirectory = Path.Combine(outputDirectory, "full");
                var genesisHashHex = ByteUtil.Hex(genesisHash.ToByteArray());
                var snapshotTipHashHex = ByteUtil.Hex(snapshotTipHash.ToByteArray());
                var fullSnapshotFilename = $"{genesisHashHex}-snapshot-{snapshotTipHashHex}-{snapshotTipIndex}.zip";
                var fullSnapshotPath = Path.Combine(fullSnapshotDirectory, fullSnapshotFilename);

                var partitionSnapshotFilename = $"{partitionBaseFilename}.zip";
                var partitionSnapshotPath = Path.Combine(outputDirectory, "partition", partitionSnapshotFilename);
                var stateSnapshotFilename = $"{stateBaseFilename}.zip";
                var stateSnapshotPath = Path.Combine(outputDirectory, "state", stateSnapshotFilename);

                var tempDirectory = Path.Combine(storePath, "temp");
                var partitionDirectory = Path.Combine(tempDirectory, "snapshot");
                var stateDirectory = Path.Combine(tempDirectory, "state");

                if (Directory.Exists(partitionDirectory))
                {
                    Directory.Delete(partitionDirectory, true);
                }

                if (Directory.Exists(stateDirectory))
                {
                    Directory.Delete(stateDirectory, true);
                }

                _logger.Debug($"Snapshot-{snapshotType.ToString()} Clean Store Start.");
                start = DateTimeOffset.Now;
                CleanStore(
                    partitionSnapshotPath,
                    stateSnapshotPath,
                    fullSnapshotPath,
                    storePath);
                _logger.Debug($"Snapshot-{snapshotType.ToString()} Clean Store Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                if (snapshotType == SnapshotType.Full || snapshotType == SnapshotType.All)
                {
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Full ZipFile Start.");
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(storePath, fullSnapshotPath);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Full ZipFile Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");
                }

                if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                {
                    var storeBlockPath = Path.Combine(storePath, "block");
                    var storeTxPath = Path.Combine(storePath, "tx");
                    var partitionDirBlockPath = Path.Combine(partitionDirectory, "block");
                    var partitionDirTxPath = Path.Combine(partitionDirectory, "tx");

                    var epochLimit = GetEpochLimit(
                        latestEpoch,
                        currentMetadataBlockEpoch,
                        previousMetadataBlockEpoch);

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Clone Partition Directory Start.");
                    start = DateTimeOffset.Now;
                    CopyDirectory(storeBlockPath, partitionDirBlockPath, epochLimit: epochLimit);
                    CopyDirectory(storeTxPath, partitionDirTxPath, epochLimit: epochLimit);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Clone Partition Directory Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Partition ZipFile Start.");
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(partitionDirectory, partitionSnapshotPath);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Create Partition ZipFile Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                    Directory.Delete(partitionDirectory, true);

                    STATE_PATHS.ForEach(path => File.Create(Path.Combine(storePath, path, "LOCK")));

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Move State Directory Start.");
                    start = DateTimeOffset.Now;
                    MoveStateStore(storePath, stateDirectory);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Move State Directory Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                    STATE_PATHS.ForEach(path => File.Delete(Path.Combine(stateDirectory, path, "LOCK")));

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Create State ZipFile Start.");
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(stateDirectory, stateSnapshotPath);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Create State ZipFile Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Restore State Directory Start.");
                    start = DateTimeOffset.Now;
                    MoveStateStore(stateDirectory, storePath);
                    _logger.Debug($"Snapshot-{snapshotType.ToString()} Restore State Directory Done. Time Taken: {(DateTimeOffset.Now - start).TotalMinutes} min.");

                    Directory.Delete(stateDirectory, true);

                    if (snapshotTipDigest is null)
                    {
                        throw new CommandExitedException("Tip does not exist.", -1);
                    }

                    string stringfyMetadata = CreateMetadata(
                        snapshotTipDigest.Value,
                        apv,
                        currentMetadataBlockEpoch,
                        currentMetadataTxEpoch,
                        previousMetadataBlockEpoch,
                        latestEpoch);
                    var metadataFilename = $"{partitionBaseFilename}.json";
                    var metadataPath = Path.Combine(metadataDirectory, metadataFilename);

                    if (File.Exists(metadataPath))
                    {
                        File.Delete(metadataPath);
                    }

                    File.WriteAllText(metadataPath, stringfyMetadata);
                }

                _logger.Debug($"Create Snapshot-{snapshotType.ToString()} Complete. Time Taken: {(DateTimeOffset.Now - snapshotStart).TotalMinutes} min.");
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

        private void MoveStateStore(string src, string dst)
        {
            STATE_PATHS.ForEach(path => CopyDirectory(Path.Combine(src, path), Path.Combine(dst, path), moveInsteadOfCopy: true));
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

        private void CopyDirectory(string sourceDir, string destinationDir, bool recursive = true, bool moveInsteadOfCopy = false, int? epochLimit = null)
        {
            try
            {
                // Get information about the source directory
                var dir = new DirectoryInfo(sourceDir);

                // Check if the source directory exists
                if (!dir.Exists)
                    throw new DirectoryNotFoundException($"Source directory not found: {dir.FullName}");

                // Cache directories before we start copying
                DirectoryInfo[] dirs = dir.GetDirectories();

                // Create the destination directory
                Directory.CreateDirectory(destinationDir);

                // Get the files in the source directory and copy to the destination directory
                foreach (FileInfo file in dir.GetFiles())
                {
                    string targetFilePath = Path.Combine(destinationDir, file.Name);
                    if (moveInsteadOfCopy)
                    {
                        file.MoveTo(targetFilePath);
                    }
                    else
                    {
                        file.CopyTo(targetFilePath);
                    }
                }

                // If recursive and copying subdirectories, recursively call this method
                if (recursive)
                {
                    foreach (DirectoryInfo subDir in dirs)
                    {
                        if (epochLimit.HasValue
                            && subDir.Name.StartsWith("epoch")
                            && int.TryParse(subDir.Name.Substring(5), out int epoch)
                            && epoch < epochLimit.Value)
                            continue;

                        string newDestinationDir = Path.Combine(destinationDir, subDir.Name);
                        CopyDirectory(subDir.FullName, newDestinationDir, true, moveInsteadOfCopy, epochLimit);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message);
                _logger.Error(ex.StackTrace);
            }
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
