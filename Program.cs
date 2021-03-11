using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
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

namespace NineChronicles.Snapshot
{
    class Program
    {
        private RocksDBStore _store;
        private TrieStateStore _stateStore;

        static void Main(string[] args)
        {
            CoconaLiteApp.Run<Program>(args);
        }

        [Command]
        public void Snapshot(
            string apv,
            string workingDirectory,
            [Option('o')]
            string outputDirectory = null,
            string storePath = null,
            int blockBefore = 10)
        {
            string defaultStorePath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "planetarium",
                "9c"
            );
            outputDirectory = string.IsNullOrEmpty(outputDirectory)
                ? Environment.CurrentDirectory
                : outputDirectory;

            int metadataBlockEpoch = 0;
            int metadataTxEpoch = 0;
            metadataBlockEpoch = GetPreviousMetadataEpoch(outputDirectory, "BlockEpoch");
            metadataTxEpoch = GetPreviousMetadataEpoch(outputDirectory, "TxEpoch");

            storePath = string.IsNullOrEmpty(storePath) ? defaultStorePath : storePath;
            if (!Directory.Exists(storePath))
            {
                throw new CommandExitedException("Invalid store path. Please check --store-path is valid.", -1);
            }

            if (!Directory.Exists(workingDirectory))
            {
                throw new CommandExitedException("Invalid working directory path. Please check --working-directory is valid.", -1);

            }
            else
            {
                CleanDirectory(workingDirectory);
                CloneDirectory(storePath, workingDirectory);
            }

            var statesPath = Path.Combine(workingDirectory, "states");
            var stateHashesPath = Path.Combine(workingDirectory, "state_hashes");

            _store = new RocksDBStore(workingDirectory);
            IKeyValueStore stateKeyValueStore = new RocksDBKeyValueStore(statesPath);
            IKeyValueStore stateHashKeyValueStore = new RocksDBKeyValueStore(stateHashesPath);
            _stateStore = new TrieStateStore(stateKeyValueStore, stateHashKeyValueStore);

            var canonicalChainId = _store.GetCanonicalChainId();
            if (canonicalChainId is Guid chainId)
            {
                var genesisHash = _store.IterateIndexes(chainId, 0, 1).First();
                var tipHash = _store.IterateIndexes(chainId, 0, null).Last();
                if (!(_store.GetBlockIndex(tipHash) is long tipIndex))
                {
                    throw new CommandExitedException(
                        $"The index of {tipHash} doesn't exist.",
                        -1);
                }
                var tip = _store.GetBlock<DummyAction>(tipHash);
                var snapshotTipIndex = Math.Max(tipIndex - (blockBefore + 1), 0);
                HashDigest<SHA256> snapshotTipHash;

                var latestBlockEpoch = (int)(tip.Timestamp.ToUnixTimeSeconds() / 86400);
                var latestBlockWithTx = GetLastestBlockWithTransaction<DummyAction>(tip, _store);
                var txTimeSecond = latestBlockWithTx.Transactions.Max(tx => tx.Timestamp.ToUnixTimeSeconds());
                var latestTxEpoch = (int)(txTimeSecond / 86400);

                do
                {
                    snapshotTipIndex++;

                    if (!(_store.IndexBlockHash(chainId, snapshotTipIndex) is HashDigest<SHA256> hash))
                    {
                        throw new CommandExitedException(
                            $"The index {snapshotTipIndex} doesn't exist on ${chainId}.",
                            -1);
                    }
                    snapshotTipHash = hash;
                } while (!_stateStore.ContainsBlockStates(snapshotTipHash));

                var forkedId = Guid.NewGuid();

                Fork(chainId, forkedId, genesisHash, snapshotTipHash, tip);

                _store.SetCanonicalChainId(forkedId);
                foreach (var id in _store.ListChainIds().Where(id => !id.Equals(forkedId)))
                {
                    _store.DeleteChainId(id);
                }

                var snapshotTipDigest = _store.GetBlockDigest(snapshotTipHash);

                _stateStore.PruneStates(new []{ snapshotTipHash }.ToImmutableHashSet());

                _store.Dispose();
                _stateStore.Dispose();

                var genesisHashHex = ByteUtil.Hex(genesisHash.ToByteArray());
                var snapshotTipHashHex = ByteUtil.Hex(snapshotTipHash.ToByteArray());
                var snapshotFilename = $"{genesisHashHex}-snapshot-{latestBlockEpoch}-{latestTxEpoch}.zip";
                var snapshotPath = Path.Combine(outputDirectory, snapshotFilename);
                if (File.Exists(snapshotPath))
                {
                    File.Delete(snapshotPath);
                }

                var blockPerceptPath = Path.Combine(workingDirectory, "blockpercept");
                if (Directory.Exists(blockPerceptPath))
                {
                    Directory.Delete(blockPerceptPath, true);
                }

                var stagedTxPath = Path.Combine(workingDirectory, "stagedtx");
                if (Directory.Exists(stagedTxPath))
                {
                    Directory.Delete(stagedTxPath, true);
                }

                var blockPath = Path.Combine(workingDirectory, "block");
                var txPath = Path.Combine(workingDirectory, "tx");
                CleanEpoch(blockPath, metadataBlockEpoch, latestBlockEpoch);
                CleanEpoch(txPath, metadataTxEpoch, latestTxEpoch);

                ZipFile.CreateFromDirectory(workingDirectory, snapshotPath);
                if (snapshotTipDigest is null)
                {
                    throw new CommandExitedException("Tip does not exists.", -1);
                }

                var snapshotTipHeader = snapshotTipDigest.Value.Header;
                JObject jsonObject = JObject.FromObject(snapshotTipHeader);
                jsonObject.Add("APV", apv);
                jsonObject.Add("BlockEpoch", latestBlockEpoch);
                jsonObject.Add("TxEpoch", latestTxEpoch);
                var jsonString = JsonConvert.SerializeObject(jsonObject);

                var metadataFilename = $"{genesisHashHex}-snapshot-{latestBlockEpoch}-{latestTxEpoch}.json";
                var metadataPath = Path.Combine(outputDirectory, metadataFilename);
                if (File.Exists(metadataPath))
                {
                    File.Delete(metadataPath);
                }

                File.WriteAllText(metadataPath, jsonString);
            }
            else
            {
                throw new CommandExitedException("Canonical chain doesn't exists.", -1);
            }
        }

        private void Fork(
            Guid src,
            Guid dest,
            HashDigest<SHA256> genesisHash,
            HashDigest<SHA256> branchpointHash,
            Block<DummyAction> tip)
        {
            var branchPoint = _store.GetBlock<DummyAction>(branchpointHash);
            _store.AppendIndex(dest, genesisHash);
            _store.ForkBlockIndexes(src, dest, branchpointHash);

            var signersToStrip = new Dictionary<Address, int>();

            for (
                Block<DummyAction> block = tip;
                block.PreviousHash is HashDigest<SHA256> hash
                && !block.Hash.Equals(branchpointHash);
                block = _store.GetBlock<DummyAction>(hash))
            {
                IEnumerable<(Address, int)> signers = block
                    .Transactions
                    .GroupBy(tx => tx.Signer)
                    .Select(g => (g.Key, g.Count()));

                foreach ((Address address, int txCount) in signers)
                {
                    signersToStrip.TryGetValue(address, out var existingValue);
                    signersToStrip[address] = existingValue + txCount;
                }
            }


            foreach (KeyValuePair<Address, long> pair in _store.ListTxNonces(src))
            {
                Address address = pair.Key;
                long existingNonce = pair.Value;
                long txNonce = existingNonce;
                if (signersToStrip.TryGetValue(address, out var staleTxCount))
                {
                    txNonce -= staleTxCount;
                }

                if (txNonce < 0)
                {
                    throw new InvalidOperationException(
                        $"A tx nonce for {address} in the store seems broken.\n" +
                        $"Existing tx nonce: {existingNonce}\n" +
                        $"# of stale transactions: {staleTxCount}\n"
                    );
                }

                _store.IncreaseTxNonce(dest, address, txNonce);
            }
        }

        private static int GetPreviousMetadataEpoch(
            string outputDirectory,
            string epochType)
        {
            try
            {
                string previousMetadata = Directory.GetFiles(outputDirectory)
                    .Where(x => Path.GetExtension(x) == ".json")
                    .OrderByDescending(x => File.GetLastWriteTime(x))
                    .First();
                using (StreamReader reader = new StreamReader(previousMetadata))
                {
                    var jsonString = reader.ReadToEnd();
                    var jsonObject = JObject.Parse(jsonString); 
                    return (int)jsonObject[epochType];            
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static Block<T> GetLastestBlockWithTransaction<T>(Block<T> tip, RocksDBStore store)
            where T : DummyAction, new()
        {
            var block = tip;
            while(!block.Transactions.Any())
            {
                if (block.PreviousHash is HashDigest<SHA256> newHash)
                {
                    block = store.GetBlock<T>(newHash);
                }
            }
            return block;
        }

        private static void CloneDirectory(string source, string dest)
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

        private static void CleanDirectory(string path)
        {
            System.IO.DirectoryInfo di = new DirectoryInfo(path);
            foreach (FileInfo file in di.GetFiles())
            {
                file.Delete(); 
            }
            foreach (DirectoryInfo dir in di.GetDirectories())
            {
                dir.Delete(true); 
            }
        }

        private static void CleanEpoch(string path, int metadataEpoch, int latestEpoch)
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
                    if (epoch < metadataEpoch)
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
    }
}
