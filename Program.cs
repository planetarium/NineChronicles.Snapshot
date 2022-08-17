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
            string outputDirectory = null,
            string storePath = null,
            int blockBefore = 10,
            SnapshotType snapshotType = SnapshotType.Partition)
        {
            try
            {
                var wb = new WebClient();
                var data = String.Format("Create Snapshot-{0} start.", snapshotType.ToString());
                string url =
                    "https://planetariumhq.slack.com/services/hooks/slackbot?token=4hBLriaHECDGHlNNbOnwjkfk&channel=%23snapshot-monitoring";
                var response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);
                // If store changed epoch unit seconds, this will be changed too
                const int blockEpochUnitSeconds = 86400;
                const int txEpochUnitSeconds = 86400;
                
                string defaultStorePath = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "planetarium",
                    "9c"
                );

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
                int previousMetadataTxEpoch = GetMetaDataEpoch(metadataDirectory, "PreviousTxEpoch");

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

                _store = new RocksDBStore(
                    storePath,
                    blockEpochUnitSeconds: blockEpochUnitSeconds,
                    txEpochUnitSeconds: txEpochUnitSeconds);
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

                Fork(chainId, forkedId, snapshotTipHash, tip);

                _store.SetCanonicalChainId(forkedId);
                foreach (var id in _store.ListChainIds().Where(id => !id.Equals(forkedId)))
                {
                    _store.DeleteChainId(id);
                }

                var snapshotTipDigest = _store.GetBlockDigest(snapshotTipHash);
                var snapshotTipStateRootHash = _store.GetStateRootHash(snapshotTipHash);

                Console.WriteLine("CopyStates Start.");
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "CopyStates Start");
                response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);
                var start = DateTimeOffset.Now;
                _stateStore.CopyStates(ImmutableHashSet<HashDigest<SHA256>>.Empty
                    .Add((HashDigest<SHA256>)snapshotTipStateRootHash), newStateStore);
                var end = DateTimeOffset.Now;
                var stringdata = String.Format("CopyStates Done. Time Taken: {0} min", (end - start).Minutes);
                Console.WriteLine(stringdata);
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);

                var latestBlockEpoch = (int) (tip.Timestamp.ToUnixTimeSeconds() / blockEpochUnitSeconds);
                var latestBlockWithTx = GetLatestBlockWithTransaction<DummyAction>(tip, _store);
                var txTimeSecond = latestBlockWithTx.Transactions.Max(tx => tx.Timestamp.ToUnixTimeSeconds());
                var latestTxEpoch = (int) (txTimeSecond / txEpochUnitSeconds);

                _store.Dispose();
                _stateStore.Dispose();
                newStateKeyValueStore.Dispose();

                Console.WriteLine("Move States Start.");
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Move States Start");
                response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);
                start = DateTimeOffset.Now;
                Directory.Delete(statesPath, recursive: true);
                Directory.Move(newStatesPath, statesPath);
                end = DateTimeOffset.Now;
                stringdata = String.Format("Move States Done. Time Taken: {0} min", (end - start).Minutes);
                Console.WriteLine(stringdata);
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);

                var partitionBaseFilename = GetPartitionBaseFileName(
                    currentMetadataBlockEpoch,
                    currentMetadataTxEpoch,
                    latestBlockEpoch,
                    latestTxEpoch);
                var stateBaseFilename = $"state_latest";

                var fullSnapshotDirectory = Path.Combine(outputDirectory, "full");
                var genesisHashHex = ByteUtil.Hex(genesisHash.ToByteArray());
                var snapshotTipHashHex = ByteUtil.Hex(snapshotTipHash.ToByteArray());
                var fullSnapshotFilename = $"{genesisHashHex}-snapshot-{snapshotTipHashHex}.zip";
                var fullSnapshotPath = Path.Combine(fullSnapshotDirectory, fullSnapshotFilename);

                var partitionSnapshotFilename = $"{partitionBaseFilename}.zip";
                var partitionSnapshotPath = Path.Combine(outputDirectory, "partition", partitionSnapshotFilename);
                var stateSnapshotFilename = $"{stateBaseFilename}.zip";
                var stateSnapshotPath = Path.Combine(outputDirectory, "state", stateSnapshotFilename);
                string partitionDirectory = Path.Combine(Path.GetTempPath(), "snapshot");
                string stateDirectory = Path.Combine(Path.GetTempPath(), "state");

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
                response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);
                start = DateTimeOffset.Now;
                CleanStore(
                    partitionSnapshotPath,
                    stateSnapshotPath,
                    fullSnapshotPath,
                    storePath);
                end = DateTimeOffset.Now;
                stringdata = String.Format("Clean Store Done. Time Taken: {0} min", (end - start).Minutes);
                Console.WriteLine(stringdata);
                data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);

                if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                {
                    var storeBlockPath = Path.Combine(storePath, "block");
                    var storeTxPath = Path.Combine(storePath, "tx");
                    var partitionDirBlockPath = Path.Combine(partitionDirectory, "block");
                    var partitionDirTxPath = Path.Combine(partitionDirectory, "tx");
                    Console.WriteLine("Clone Partition Directory Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Clone Partition Directory Start");
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    start = DateTimeOffset.Now;
                    CopyDirectory(storeBlockPath, partitionDirBlockPath, true);
                    CopyDirectory(storeTxPath, partitionDirTxPath, true);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Clone Partition Directory Done. Time Taken: {0} min", (end - start).Minutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);

                    // get epoch limit for block & tx
                    var blockEpochLimit = GetEpochLimit(
                        latestBlockEpoch,
                        currentMetadataBlockEpoch,
                        previousMetadataBlockEpoch);
                    var txEpochLimit = GetEpochLimit(
                        latestTxEpoch,
                        currentMetadataTxEpoch,
                        previousMetadataTxEpoch);

                    Console.WriteLine("Clean Partition Store Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Clean Partition Store Start");
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    start = DateTimeOffset.Now;
                    // clean epoch directories in block & tx
                    CleanEpoch(partitionDirBlockPath, blockEpochLimit);
                    CleanEpoch(partitionDirTxPath, txEpochLimit);

                    CleanPartitionStore(partitionDirectory);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Clean Partition Store Done. Time Taken: {0} min", (end - start).Minutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);

                    Console.WriteLine("Clone State Directory Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Clone State Directory Start");
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    start = DateTimeOffset.Now;
                    CopyStateStore(storePath, stateDirectory);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Clone State Directory Done. Time Taken: {0} min", (end - start).Minutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                }
                
                if (snapshotType == SnapshotType.Full || snapshotType == SnapshotType.All)
                {
                    Console.WriteLine("Create Full ZipFile Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Create Full ZipFile Start");
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(storePath, fullSnapshotPath);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Create Full ZipFile Done. Time Taken: {0} min", (end - start).Minutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                }

                if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                {
                    Console.WriteLine("Create Partition ZipFile Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Create Partition ZipFile Start");
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(partitionDirectory, partitionSnapshotPath);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Create Partition ZipFile Done. Time Taken: {0} min", (end - start).Minutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    Console.WriteLine("Create State ZipFile Start.");
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), "Create State ZipFile Start");
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(stateDirectory, stateSnapshotPath);
                    end = DateTimeOffset.Now;
                    stringdata = String.Format("Create State Zipfile Done. Time Taken: {0} min", (end - start).Minutes);
                    Console.WriteLine(stringdata);
                    data = String.Format("Snapshot-{0} {1}.", snapshotType.ToString(), stringdata);
                    response = wb.UploadString(url, "POST", data);
                    Console.WriteLine(response);

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
                        latestBlockEpoch);
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
                response = wb.UploadString(url, "POST", data);
                Console.WriteLine(response);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private string GetPartitionBaseFileName(
            int currentMetadataBlockEpoch,
            int currentMetadataTxEpoch,
            int latestBlockEpoch,
            int latestTxEpoch
        )
        {
            // decrease latest epochs by 1 when creating genesis snapshot
            if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
            {
                return $"snapshot-{latestBlockEpoch - 1}-{latestBlockEpoch - 1}";
            }
            else
            {
                return $"snapshot-{latestBlockEpoch}-{latestBlockEpoch}";
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
                else if (previousMetadataEpoch == 0)
                {
                    return currentMetadataEpoch - 1;
                }
                else
                {
                    return previousMetadataEpoch;
                }
            }
            else
            {
                return currentMetadataEpoch;
            }
        }

        private string CreateMetadata(
            BlockDigest snapshotTipDigest,
            string apv,
            int currentMetadataBlockEpoch,
            int currentMetadataTxEpoch,
            int previousMetadataBlockEpoch,
            int latestBlockEpoch)
        {
            BlockHeader snapshotTipHeader = snapshotTipDigest.GetHeader();
            JObject jsonObject = JObject.FromObject(snapshotTipHeader);
            jsonObject.Add("APV", apv);

            jsonObject = AddPreviousEpochs(
                jsonObject,
                currentMetadataBlockEpoch,
                previousMetadataBlockEpoch,
                latestBlockEpoch,
                "PreviousBlockEpoch",
                "PreviousTxEpoch");

            // decrease latest epochs by 1 for genesis snapshot
            if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
            {
                jsonObject.Add("BlockEpoch", latestBlockEpoch - 1);
                jsonObject.Add("TxEpoch", latestBlockEpoch - 1);
            }
            else
            {
                jsonObject.Add("BlockEpoch", latestBlockEpoch);
                jsonObject.Add("TxEpoch", latestBlockEpoch);
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
            var stateDirBlockIndexPath = Path.Combine(stateDirectory, "block", "blockindex");
            var stateDirTxIndexPath = Path.Combine(stateDirectory, "tx", "txindex");
            var stateDirTxBIndexPath = Path.Combine(stateDirectory, "txbindex");
            var stateDirStatesPath = Path.Combine(stateDirectory, "states");
            var stateDirChainPath = Path.Combine(stateDirectory, "chain");
            CopyDirectory(storeBlockIndexPath, stateDirBlockIndexPath, true);
            CopyDirectory(storeTxIndexPath, stateDirTxIndexPath, true);
            CopyDirectory(storeTxBIndexPath, stateDirTxBIndexPath, true);
            CopyDirectory(storeStatesPath, stateDirStatesPath, true);
            CopyDirectory(storeChainPath, stateDirChainPath, true);
        }

        private void Fork(
            Guid src,
            Guid dest,
            BlockHash branchpointHash,
            Block<DummyAction> tip)
        {
            var branchPoint = _store.GetBlock<DummyAction>(branchpointHash);
            _store.ForkBlockIndexes(src, dest, branchpointHash);
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

        private void CloneDirectory(string source, string dest)
        {
            foreach (var directory in Directory.GetDirectories(source))
            {
                string dirName = Path.GetFileName(directory);
                if (!Directory.Exists(Path.Combine(dest, dirName)))
                {
                    Directory.CreateDirectory(Path.Combine(dest, dirName));
                }
                CloneDirectory(directory, Path.Combine(dest, dirName));
            }

            foreach (var file in Directory.GetFiles(source))
            {
                File.Copy(file, Path.Combine(dest, Path.GetFileName(file)));
            }
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

        private JObject AddPreviousTxEpoch(JObject jsonObject,
            int currentMetadataBlockEpoch,
            int currentMetadataTxEpoch,
            int previousMetadataTxEpoch,
            int latestBlockEpoch,
            int latestTxEpoch,
            string txEpochName)
        {
                if (currentMetadataTxEpoch == latestTxEpoch)
                {
                    if (currentMetadataBlockEpoch != latestBlockEpoch)
                    {
                        jsonObject.Add(txEpochName, currentMetadataTxEpoch);
                    }
                    else
                    {
                        jsonObject.Add(txEpochName, previousMetadataTxEpoch);
                    }
                }
                else
                {
                    jsonObject.Add(txEpochName, currentMetadataTxEpoch);
                }

                return jsonObject;
        }

        private JObject AddPreviousBlockEpoch(JObject jsonObject,
            int currentMetadataBlockEpoch,
            int currentMetadataTxEpoch,
            int previousMetadataBlockEpoch,
            int latestBlockEpoch,
            int latestTxEpoch,
            string blockEpochName)
        {
                if (currentMetadataBlockEpoch == latestBlockEpoch)
                {
                    if (currentMetadataTxEpoch != latestTxEpoch)
                    {
                        jsonObject.Add(blockEpochName, currentMetadataBlockEpoch);
                    }
                    else
                    {
                        jsonObject.Add(blockEpochName, previousMetadataBlockEpoch);
                    }
                }
                else
                {
                    jsonObject.Add(blockEpochName, currentMetadataBlockEpoch);
                }

                return jsonObject;
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

        private class DummyAction : IAction
        {
            public IValue PlainValue { get; private set; }
            public void LoadPlainValue(IValue plainValue) { PlainValue = plainValue; }
            public IAccountStateDelta Execute(IActionContext context) => context.PreviousStates;
            public void Render(IActionContext context, IAccountStateDelta nextStates) { }
            public void RenderError(IActionContext context, Exception exception) { }
            public void Unrender(IActionContext context, IAccountStateDelta nextStates) { }
            public void UnrenderError(IActionContext context, Exception exception) { }
        }

        internal static class RocksDBStoreBitConverter
        {
            /// <summary>
            /// Get <c>long</c> representation of the <paramref name="value"/>.
            /// </summary>
            /// <param name="value">The Big-endian byte-array value to convert to <c>long</c>.</param>
            /// <returns>The <c>long</c> representation of the <paramref name="value"/>.</returns>
            public static long ToInt64(byte[] value)
            {
                byte[] bytes = new byte[sizeof(long)];
                value.CopyTo(bytes, 0);

                // Use Big-endian to order index lexicographically.
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(bytes);
                }

                return BitConverter.ToInt64(bytes, 0);
            }

            /// <summary>
            /// Get <c>string</c> representation of the <paramref name="value"/>.
            /// </summary>
            /// <param name="value">The byte-array value to convert to <c>string</c>.</param>
            /// <returns>The <c>string</c> representation of the <paramref name="value"/>.</returns>
            public static string GetString(byte[] value)
            {
                return Encoding.UTF8.GetString(value);
            }

            /// <summary>
            /// Get Big-endian byte-array representation of the <paramref name="value"/>.
            /// </summary>
            /// <param name="value">The <c>long</c> value to convert to byte-array.</param>
            /// <returns>The Big-endian byte-array representation of the <paramref name="value"/>.
            /// </returns>
            public static byte[] GetBytes(long value)
            {
                byte[] bytes = BitConverter.GetBytes(value);

                // Use Big-endian to order index lexicographically.
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(bytes);
                }

                return bytes;
            }

            /// <summary>
            /// Get encoded byte-array representation of the <paramref name="value"/>.
            /// </summary>
            /// <param name="value">The <c>string</c> to convert to byte-array.</param>
            /// <returns>The encoded representation of the <paramref name="value"/>.</returns>
            public static byte[] GetBytes(string value)
            {
                return Encoding.UTF8.GetBytes(value);
            }
        }
    }
}
