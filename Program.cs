using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
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
using RocksDbSharp;

namespace NineChronicles.Snapshot
{
    class Program
    {
        private RocksDBStore _store;
        private TrieStateStore _stateStore;
        private HashAlgorithmGetter _hashAlgorithmGetter;

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
                var txexecPath = Path.Combine(storePath, "txexec");

                var staleDirectories =
                new [] { mainPath, statePath, stateRefPath, stateHashesPath, txexecPath };
                foreach (var staleDirectory in staleDirectories)
                {
                    if (Directory.Exists(staleDirectory))
                    {
                        Directory.Delete(staleDirectory, true);
                    }
                }

                PruneOutdatedColumnFamilies(Path.Combine(storePath, "chain"));

                _hashAlgorithmGetter = (_ => HashAlgorithmType.Of<SHA256>());
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

                var tip = _store.GetBlock<DummyAction>(_hashAlgorithmGetter, tipHash);
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
                } while (!_stateStore.ContainsStateRoot(_store.GetBlock<DummyAction>(_hashAlgorithmGetter, snapshotTipHash).StateRootHash));

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
                var start = DateTimeOffset.Now;
                _stateStore.CopyStates(ImmutableHashSet<HashDigest<SHA256>>.Empty
                    .Add((HashDigest<SHA256>)snapshotTipStateRootHash), newStateStore);
                var end = DateTimeOffset.Now;
                Console.WriteLine("CopyStates Done. Time Taken: {0}", (end - start).Minutes);

                var latestBlockEpoch = (int) (tip.Timestamp.ToUnixTimeSeconds() / blockEpochUnitSeconds);
                var latestBlockWithTx = GetLatestBlockWithTransaction<DummyAction>(tip, _store);
                var txTimeSecond = latestBlockWithTx.Transactions.Max(tx => tx.Timestamp.ToUnixTimeSeconds());
                var latestTxEpoch = (int) (txTimeSecond / txEpochUnitSeconds);

                _store.Dispose();
                _stateStore.Dispose();

                Console.WriteLine("Move States Start.");
                start = DateTimeOffset.Now;
                Directory.Delete(statesPath, recursive: true);
                Directory.Move(newStatesPath, statesPath);
                end = DateTimeOffset.Now;
                Console.WriteLine("Move States Done. Time Taken: {0}", (end - start).Minutes);

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
                Console.WriteLine("Clean Store Start.");
                start = DateTimeOffset.Now;
                CleanStore(
                    partitionSnapshotPath,
                    stateSnapshotPath,
                    fullSnapshotPath,
                    storePath);
                end = DateTimeOffset.Now;
                Console.WriteLine("Clean Store Done. Time Taken: {0}", (end - start).Minutes);
                if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                {
                    Console.WriteLine("Clone Directory Start.");
                    start = DateTimeOffset.Now;
                    CloneDirectory(storePath, partitionDirectory);
                    CloneDirectory(storePath, stateDirectory);
                    end = DateTimeOffset.Now;
                    Console.WriteLine("Clone Directory Done. Time Taken: {0}", (end - start).Minutes);
                    var blockPath = Path.Combine(partitionDirectory, "block");
                    var txPath = Path.Combine(partitionDirectory, "tx");

                    // get epoch limit for block & tx
                    var blockEpochLimit = GetEpochLimit(
                        latestBlockEpoch,
                        currentMetadataBlockEpoch,
                        previousMetadataBlockEpoch);
                    var txEpochLimit = GetEpochLimit(
                        latestTxEpoch,
                        currentMetadataTxEpoch,
                        previousMetadataTxEpoch);

                    Console.WriteLine("Clean Stores Start.");
                    start = DateTimeOffset.Now;
                    // clean epoch directories in block & tx
                    CleanEpoch(blockPath, blockEpochLimit);
                    CleanEpoch(txPath, txEpochLimit);

                    CleanPartitionStore(partitionDirectory);
                    CleanStateStore(stateDirectory);
                    end = DateTimeOffset.Now;
                    Console.WriteLine("Clean Stores Done. Time Taken: {0}", (end - start).Minutes);
                }
                
                if (snapshotType == SnapshotType.Full || snapshotType == SnapshotType.All)
                {
                    ZipFile.CreateFromDirectory(storePath, fullSnapshotPath);
                }

                if (snapshotType == SnapshotType.Partition || snapshotType == SnapshotType.All)
                {
                    Console.WriteLine("Create Partition ZipFile Start.");
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(partitionDirectory, partitionSnapshotPath);
                    end = DateTimeOffset.Now;
                    Console.WriteLine("Create Partition ZipFile Done. Time Taken: {0}", (end - start).Minutes);
                    Console.WriteLine("Create State ZipFile Start.");
                    start = DateTimeOffset.Now;
                    ZipFile.CreateFromDirectory(stateDirectory, stateSnapshotPath);
                    end = DateTimeOffset.Now;
                    Console.WriteLine("Create State Zipfile Done. Time Taken: {0}", (end - start).Minutes);
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
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private void PruneOutdatedColumnFamilies(string path)
        {
            var opt = new DbOptions();
            List<string> cfns = RocksDb.ListColumnFamilies(opt, path).ToList();
            var cfs = new ColumnFamilies();
            foreach (string name in cfns)
            {
                cfs.Add(name, opt);
            }

            using RocksDb db = RocksDb.Open(opt, path, cfs);
            var ccid = new Guid(db.Get(new[] { (byte)'C' }));
            ColumnFamilyHandle ccf = db.GetColumnFamily(ccid.ToString());

            (ColumnFamilyHandle, long)? PreviousColumnFamily(ColumnFamilyHandle cfh)
            {
                byte[] cid = db.Get(new[] { (byte)'P' }, cfh);

                if (cid is null)
                {
                    return null;
                }
                else
                {
                    return (
                        db.GetColumnFamily(new Guid(cid).ToString()),
                        RocksDBStoreBitConverter.ToInt64(db.Get(new[] { (byte)'p' }, cfh))
                    );
                }
            }

            var cfInfo = PreviousColumnFamily(ccf);
            var ip = new[] { (byte)'I' };
            var batch = new WriteBatch();
            while (cfInfo is { } cfInfoNotNull)
            {
                ColumnFamilyHandle cf = cfInfoNotNull.Item1;
                long cfi = cfInfoNotNull.Item2;
                
                Iterator it = db.NewIterator(cf);
                for (it.Seek(ip); it.Valid() && it.Key().StartsWith(ip); it.Next())
                {
                    long index = RocksDBStoreBitConverter.ToInt64(it.Key().Skip(1).ToArray());
                    if (index > cfi)
                    {
                        continue;
                    }
                    batch.Put(it.Key(), it.Value(), ccf);

                    if (batch.Count() > 10000)
                    {
                        db.Write(batch);
                        batch.Clear();
                    }
                }

                cfInfo = PreviousColumnFamily(cf);
            }

            db.Write(batch);

            foreach (string name in cfns)
            {
                if (name == ccid.ToString() || name == "default")
                {
                    continue;
                }
                db.DropColumnFamily(name);
            }

            db.Remove(new[] { (byte)'P' }, ccf);
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
            BlockHeader snapshotTipHeader = snapshotTipDigest.GetHeader(_hashAlgorithmGetter);
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
                Path.Combine(partitionDirectory, "state"),
                Path.Combine(partitionDirectory, "state_hashes"),
                Path.Combine(partitionDirectory, "stateref"),
                Path.Combine(partitionDirectory, "states"),
                Path.Combine(partitionDirectory, "chain"),
                Path.Combine(partitionDirectory, "txexec"),
                Path.Combine(partitionDirectory, "txbindex"),
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

        private void CleanStateStore(string stateDirectory)
        {
            var blockPath = Path.Combine(stateDirectory, "block");
            var txPath = Path.Combine(stateDirectory, "tx");
            var txExecPath = Path.Combine(stateDirectory, "txexec");
            string[] blockDirectories = Directory.GetDirectories(
                blockPath,
                "epoch*",
                SearchOption.AllDirectories);
            string[] txDirectories = Directory.GetDirectories(
                txPath,
                "epoch*",
                SearchOption.AllDirectories);

            foreach (string dir in blockDirectories)
            {
                Directory.Delete(dir, true);
            }

            foreach (string dir in txDirectories)
            {
                Directory.Delete(dir, true);
            }

            Directory.Delete(txExecPath, true);
        }

        private void Fork(
            Guid src,
            Guid dest,
            BlockHash branchpointHash,
            Block<DummyAction> tip)
        {
            var branchPoint = _store.GetBlock<DummyAction>(_hashAlgorithmGetter, branchpointHash);
            _store.ForkBlockIndexes(src, dest, branchpointHash);
            _store.ForkTxNonces(src, dest);

            for (
                Block<DummyAction> block = tip;
                block.PreviousHash is BlockHash hash
                && !block.Hash.Equals(branchpointHash);
                block = _store.GetBlock<DummyAction>(_hashAlgorithmGetter, hash))
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
                    block = store.GetBlock<T>(_hashAlgorithmGetter, newHash);
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
