using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Bencodex.Types;
using Cocona;
using Libplanet;
using Libplanet.Action;
using Libplanet.Blocks;
using Libplanet.RocksDBStore;
using Libplanet.Store;
using Libplanet.Store.Trie;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Reflection;
using Libplanet.Blockchain;
using Libplanet.Blockchain.Policies;
using Serilog;
using Serilog.Events;

namespace NineChronicles.Snapshot
{
    class Program
    {
        private RocksDBStore _store;
        private TrieStateStore _stateStore;

        public enum SnapshotType { Full, Partition, All }

        static void Main(string[] args)
        {
            CoconaLiteApp.Run<Program>(args);
        }

        [Command]
        public void Snapshot(
            string apv,
            [Option('o')]
            string outputDirectory,
            string storePath = null,
            int blockBefore = 1,
            SnapshotType snapshotType = SnapshotType.Partition)
        {
            try
            {
                var data = String.Format("Create Snapshot-{0} start.", snapshotType.ToString());
                Console.WriteLine(data);
                // If store changed epoch unit seconds, this will be changed too
                const int epochUnitSeconds = 86400;
                
                string defaultStorePath = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "planetarium",
                    "9c"
                );

                if (blockBefore < 1)
                {
                    throw new CommandExitedException("The --block-before option must be greater than 0.", -1);
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
                var tempDirectory = Path.Combine(outputDirectory, "temp");
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
                new [] { mainPath, statePath, stateRefPath, stateHashesPath};
                foreach (var staleDirectory in staleDirectories)
                {
                    if (Directory.Exists(staleDirectory))
                    {
                        Directory.Delete(staleDirectory, true);
                    }
                }

                if (RocksDBStore.MigrateChainDBFromColumnFamilies(Path.Combine(storePath, "chain")))
                {
                    Console.WriteLine("Successfully migrated IndexDB.");
                }
                else
                {
                    Console.WriteLine("Migration not required.");
                }

                _store = new RocksDBStore(storePath);
                IKeyValueStore stateKeyValueStore = new RocksDBKeyValueStore(statesPath);
                IKeyValueStore newStateKeyValueStore = new RocksDBKeyValueStore(newStatesPath);
                _stateStore = new TrieStateStore(stateKeyValueStore);
                var newStateStore = new TrieStateStore(newStateKeyValueStore);

                var canonicalChainId = _store.GetCanonicalChainId();
                if (!(canonicalChainId is Guid chainId))
                {
                    throw new CommandExitedException("Canonical chain doesn't exist.", -1);
                }

                var genesisHash = _store.IterateIndexes(chainId,0, 1).First();
                var tipHash = _store.IndexBlockHash(chainId, -1) 
                    ?? throw new CommandExitedException("The given chain seems empty.", -1);
                if (!(_store.GetBlockIndex(tipHash) is long tipIndex))
                {
                    throw new CommandExitedException(
                        $"The index of {tipHash} doesn't exist.",
                        -1);
                }

                var tip = _store.GetBlock<DummyAction>(tipHash);
                var tipBlockCommit = _store.GetBlockCommit(tipHash);
                IStagePolicy<DummyAction> stagePolicy = new VolatileStagePolicy<DummyAction>();
                IBlockPolicy<DummyAction> blockPolicy =
                    new BlockPolicy<DummyAction>();
                var originalChain = new BlockChain<DummyAction>(blockPolicy, stagePolicy, _store, _stateStore, _store.GetBlock<DummyAction>(genesisHash));
                Console.WriteLine("*** FROM CHAIN: This is the block commit of original chain tip #{0} Tip.LastCommit: {1} TipGetBlockCommit: {2}", tip.Index, tip.LastCommit, originalChain.GetBlockCommit(tipHash));
                Console.WriteLine("*** FROM STORE: This is the block commit of original chain tip #{0} Tip.LastCommit: {1} TipGetBlockCommit: {2}", tip.Index, tip.LastCommit, tipBlockCommit);
                var snapshotTipIndex = Math.Max(tipIndex - (blockBefore + 1), 0);
                BlockHash snapshotTipHash;

                do
                {
                    snapshotTipIndex++;

                    if (!(_store.IndexBlockHash(chainId, snapshotTipIndex) is BlockHash hash))
                    {
                        throw new CommandExitedException(
                            $"The index {snapshotTipIndex} doesn't exist on ${chainId}.",
                            -1);
                    }

                    snapshotTipHash = hash;
                } while (!_stateStore.ContainsStateRoot(_store.GetBlock<DummyAction>(snapshotTipHash).StateRootHash));

                var forkedId = Guid.NewGuid();

                tipBlockCommit = _store.GetBlockCommit(tipHash);
                Console.WriteLine("*** Pre-Fork: This is the block commit of original chain tip #{0} Tip.LastCommit: {1} TipGetBlockCommit: {2}", tip.Index, tip.LastCommit, tipBlockCommit);
                var snapshotTip = _store.GetBlock<DummyAction>(snapshotTipHash);
                var snapshotTipCommit = _store.GetBlockCommit(snapshotTipHash);
                Console.WriteLine("*** Pre-Fork & Before PutBlockCommit: This is the block commit of snapshot tip #{0} snapshotTip.LastCommit: {1} snapshotTipGetBlockCommit: {2}", snapshotTip.Index, snapshotTip.LastCommit, snapshotTipCommit);
                _store.PutBlockCommit(tip.LastCommit);

                Fork(chainId, forkedId, snapshotTipHash, tip);

                _store.SetCanonicalChainId(forkedId);
                foreach (var id in _store.ListChainIds().Where(id => !id.Equals(forkedId)))
                {
                    _store.DeleteChainId(id);
                }

                snapshotTipCommit = _store.GetBlockCommit(snapshotTipHash);
                Console.WriteLine("*** Post-Fork & After PutBlockCommit: This is the block commit of snapshot tip #{0} snapshotTip.LastCommit: {1} snapshotTipGetBlockCommit: {2}", snapshotTip.Index, snapshotTip.LastCommit, snapshotTipCommit);
                var newChain = new BlockChain<DummyAction>(blockPolicy, stagePolicy, _store, _stateStore, _store.GetBlock<DummyAction>(genesisHash));
                var newTip = newChain.Tip;
                Console.WriteLine("*** FROM STORE New Tip Index: {0} Tip Timestamp: {1} Tip LastCommit: {2} Tip Block Commit: {3}", newTip.Index, newTip.Timestamp.UtcDateTime, newTip.LastCommit, snapshotTipCommit);
                Console.WriteLine("*** FROM CHAIN New Tip Index: {0} Tip Timestamp: {1} Tip LastCommit: {2} Tip Block Commit: {3}", newTip.Index, newTip.Timestamp.UtcDateTime, newTip.LastCommit, newChain.GetBlockCommit(newTip.Hash));
                var snapshotTipDigest = _store.GetBlockDigest(snapshotTipHash);
                ImmutableHashSet<HashDigest<SHA256>> stateHashes = ImmutableHashSet<HashDigest<SHA256>>.Empty;

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

                Console.WriteLine("CopyStates Start.");
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "CopyStates Start");
                Console.WriteLine(data);
                var start = DateTimeOffset.Now;
                _stateStore.CopyStates(stateHashes, newStateStore);
                var end = DateTimeOffset.Now;
                var stringdata = String.Format("CopyStates Done. Time Taken: {0} min", (end - start).TotalMinutes);
                Console.WriteLine(stringdata);
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                Console.WriteLine(data);

                var latestEpoch = (int) (tip.Timestamp.ToUnixTimeSeconds() / epochUnitSeconds);
                Console.WriteLine("*** Original Tip Index: {0} Tip Timestamp: {1} Tip LastCommit: {2} Latest Epoch: {3} Tip Block Commit: {4}", tip.Index, tip.Timestamp.UtcDateTime, tip.LastCommit, latestEpoch, _store.GetBlockCommit(tip.Hash));
                Console.WriteLine("*** FROM STORE New Tip Index: {0} Tip Timestamp: {1} Tip LastCommit: {2} Tip Block Commit: {3}", newTip.Index, newTip.Timestamp.UtcDateTime, newTip.LastCommit, _store.GetBlockCommit(newTip.Hash));
                Console.WriteLine("*** FROM CHAIN New Tip Index: {0} Tip Timestamp: {1} Tip LastCommit: {2} Tip Block Commit: {3}", newTip.Index, newTip.Timestamp.UtcDateTime, newTip.LastCommit, newChain.GetBlockCommit(newTip.Hash));

                _store.Dispose();
                _stateStore.Dispose();
                newStateKeyValueStore.Dispose();

                Console.WriteLine("Move States Start.");
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Move States Start");
                Console.WriteLine(data);
                start = DateTimeOffset.Now;
                Directory.Delete(statesPath, recursive: true);
                Directory.Move(newStatesPath, statesPath);
                end = DateTimeOffset.Now;
                stringdata = String.Format("Move States Done. Time Taken: {0} min", (end - start).TotalMinutes);
                Console.WriteLine(stringdata);
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                Console.WriteLine(data);

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
                string partitionDirectory = Path.Combine(tempDirectory, "snapshot");
                string stateDirectory = Path.Combine(tempDirectory, "state");

                if (Directory.Exists(partitionDirectory))
                {
                    Directory.Delete(partitionDirectory, true);
                }

                if (Directory.Exists(stateDirectory))
                {
                    Directory.Delete(stateDirectory, true);
                }

                Console.WriteLine("Clean Store Start.");
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Clean Store Start");
                Console.WriteLine(data);
                start = DateTimeOffset.Now;
                CleanStore(
                    partitionSnapshotPath,
                    stateSnapshotPath,
                    fullSnapshotPath,
                    storePath);
                end = DateTimeOffset.Now;
                stringdata = String.Format("Clean Store Done. Time Taken: {0} min", (end - start).TotalMinutes);
                Console.WriteLine(stringdata);
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                Console.WriteLine(data);

                if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                {
                    var storeBlockPath = Path.Combine(storePath, "block");
                    var storeTxPath = Path.Combine(storePath, "tx");
                    var partitionDirBlockPath = Path.Combine(partitionDirectory, "block");
                    var partitionDirTxPath = Path.Combine(partitionDirectory, "tx");
                    Console.WriteLine("Clone Partition Directory Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Clone Partition Directory Start");
                    Console.WriteLine(data);
                    start = DateTimeOffset.Now;
                    CopyDirectory(storeBlockPath, partitionDirBlockPath, true);
                    CopyDirectory(storeTxPath, partitionDirTxPath, true);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Clone Partition Directory Done. Time Taken: {0} min", (end - start).TotalMinutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    Console.WriteLine(data);

                    // get epoch limit for block & tx
                    var epochLimit = GetEpochLimit(
                        latestEpoch,
                        currentMetadataBlockEpoch,
                        previousMetadataBlockEpoch);

                    Console.WriteLine("Clean Partition Store Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Clean Partition Store Start");
                    Console.WriteLine(data);
                    start = DateTimeOffset.Now;
                    // clean epoch directories in block & tx
                    CleanEpoch(partitionDirBlockPath, epochLimit);
                    CleanEpoch(partitionDirTxPath, epochLimit);

                    CleanPartitionStore(partitionDirectory);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Clean Partition Store Done. Time Taken: {0} min", (end - start).TotalMinutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    Console.WriteLine(data);

                    Console.WriteLine("Clone State Directory Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Clone State Directory Start");
                    Console.WriteLine(data);
                    start = DateTimeOffset.Now;
                    CopyStateStore(storePath, stateDirectory);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Clone State Directory Done. Time Taken: {0} min", (end - start).TotalMinutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    Console.WriteLine(data);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    Console.WriteLine(data);
                }
                
                if (snapshotType == SnapshotType.Full || snapshotType == SnapshotType.All)
                {
                    Console.WriteLine("Create Full ZipFile Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Create Full ZipFile Start");
                    Console.WriteLine(data);
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(storePath, fullSnapshotPath);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Create Full ZipFile Done. Time Taken: {0} min", (end - start).TotalMinutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    Console.WriteLine(data);
                }

                if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                {
                    Console.WriteLine("Create Partition ZipFile Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Create Partition ZipFile Start");
                    Console.WriteLine(data);
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(partitionDirectory, partitionSnapshotPath);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Create Partition ZipFile Done. Time Taken: {0} min", (end - start).TotalMinutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    Console.WriteLine(data);
                    Console.WriteLine("Create State ZipFile Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Create State ZipFile Start");
                    Console.WriteLine(data);
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(stateDirectory, stateSnapshotPath);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Create State Zipfile Done. Time Taken: {0} min", (end - start).TotalMinutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    Console.WriteLine(data);

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
                    Directory.Delete(partitionDirectory, true);
                    Directory.Delete(stateDirectory, true);
                }

                data = String.Format("Create Snapshot-{0} Complete.", snapshotType.ToString());
                Console.WriteLine(data);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
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

        private void CopyStateStore(string storePath,string stateDirectory)
        {
            var storeBlockIndexPath = Path.Combine(storePath, "block", "blockindex");
            var storeTxIndexPath = Path.Combine(storePath, "tx", "txindex");
            var storeTxBIndexPath = Path.Combine(storePath, "txbindex");
            var storeStatesPath = Path.Combine(storePath, "states");
            var storeChainPath = Path.Combine(storePath, "chain");
            var storeBlockCommitPath = Path.Combine(storePath, "blockcommit");
            var stateDirBlockIndexPath = Path.Combine(stateDirectory, "block", "blockindex");
            var stateDirTxIndexPath = Path.Combine(stateDirectory, "tx", "txindex");
            var stateDirTxBIndexPath = Path.Combine(stateDirectory, "txbindex");
            var stateDirStatesPath = Path.Combine(stateDirectory, "states");
            var stateDirChainPath = Path.Combine(stateDirectory, "chain");
            var stateDirBlockCommitPath = Path.Combine(stateDirectory, "blockcommit");
            CopyDirectory(storeBlockIndexPath, stateDirBlockIndexPath, true);
            CopyDirectory(storeTxIndexPath, stateDirTxIndexPath, true);
            CopyDirectory(storeTxBIndexPath, stateDirTxBIndexPath, true);
            CopyDirectory(storeStatesPath, stateDirStatesPath, true);
            CopyDirectory(storeChainPath, stateDirChainPath, true);
            CopyDirectory(storeBlockCommitPath, stateDirBlockCommitPath, true);
        }

        private void Fork(
            Guid src,
            Guid dest,
            BlockHash branchpointHash,
            Block<DummyAction> tip)
        {
            var branchPoint = _store.GetBlock<DummyAction>(branchpointHash);
            _store.ForkBlockIndexes(src, dest, branchpointHash);
            if (_store.GetBlockCommit(branchpointHash) is { } p)
            {
                _store.PutChainBlockCommit(dest, _store.GetBlockCommit(branchpointHash));
            }
            _store.ForkTxNonces(src, dest);

            for (
                Block<DummyAction> block = tip;
                block.PreviousHash is BlockHash hash
                && !block.Hash.Equals(branchpointHash);
                block = _store.GetBlock<DummyAction>(hash))
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
            catch (InvalidOperationException e)
            {
                Console.Error.WriteLine(e.Message);
                return 0;
            }
        }

        private Block<T> GetLatestBlockWithTransaction<T>(Block<T> tip, RocksDBStore store)
            where T : DummyAction, new()
        {
            var block = tip;
            while(!block.Transactions.Any())
            {
                if (block.PreviousHash is BlockHash newHash)
                {
                    block = store.GetBlock<T>(newHash);
                }
            }
            return block;
        }

        private void CopyDirectory(string sourceDir, string destinationDir, bool recursive)
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
                    file.CopyTo(targetFilePath);
                }

                // If recursive and copying subdirectories, recursively call this method
                if (recursive)
                {
                    foreach (DirectoryInfo subDir in dirs)
                    {
                        string newDestinationDir = Path.Combine(destinationDir, subDir.Name);
                        CopyDirectory(subDir.FullName, newDestinationDir, true);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        
        private void CleanEpoch(string path, int epochLimit)
        {
            string[] directories = Directory.GetDirectories(
                path,
                "epoch*",
                SearchOption.AllDirectories);
            try
            {
                foreach (string dir in directories)
                {
                    string dirName = new DirectoryInfo(dir).Name;
                    int epoch = Int32.Parse(dirName.Substring(5));
                    if (epoch < epochLimit)
                    {
                        Directory.Delete(dir, true);
                    }
                }
            }
            catch (FormatException)
            {
                throw new FormatException("Epoch value is not numeric.");
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

        internal class DummyAction : IAction
        {
            public IValue PlainValue { get; private set; }
            public void LoadPlainValue(IValue plainValue) { PlainValue = plainValue; }
            public IAccountStateDelta Execute(IActionContext context) => context.PreviousStates;
        }
    }
}
