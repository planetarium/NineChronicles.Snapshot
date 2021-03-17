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
            [Option('o')]
            string outputDirectory = null,
            string storePath = null,
            int blockBefore = 10)
        {
            // If store changed epoch unit seconds, this will be changed too
            const int blockEpochUnitSeconds = 86400;
            const int txEpochUnitSeconds = 86400;
            
            string defaultStorePath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "planetarium",
                "9c"
            );
            outputDirectory = string.IsNullOrEmpty(outputDirectory)
                ? Environment.CurrentDirectory
                : outputDirectory;

            int currentMetadataBlockEpoch = GetMetaDataEpoch(outputDirectory, "BlockEpoch");
            int currentMetadataTxEpoch = GetMetaDataEpoch(outputDirectory, "TxEpoch");
            int previousMetadataBlockEpoch = GetMetaDataEpoch(outputDirectory, "PreviousBlockEpoch");
            int previousMetadataTxEpoch = GetMetaDataEpoch(outputDirectory, "PreviousTxEpoch");

            storePath = string.IsNullOrEmpty(storePath) ? defaultStorePath : storePath;
            if (!Directory.Exists(storePath))
            {
                throw new CommandExitedException("Invalid store path. Please check --store-path is valid.", -1);
            }

            var statesPath = Path.Combine(storePath, "states");
            var stateHashesPath = Path.Combine(storePath, "state_hashes");

            _store = new RocksDBStore(
                storePath,
                blockEpochUnitSeconds: blockEpochUnitSeconds,
                txEpochUnitSeconds: txEpochUnitSeconds);
            IKeyValueStore stateKeyValueStore = new RocksDBKeyValueStore(statesPath);
            IKeyValueStore stateHashKeyValueStore = new RocksDBKeyValueStore(stateHashesPath);
            _stateStore = new TrieStateStore(stateKeyValueStore, stateHashKeyValueStore);

            var canonicalChainId = _store.GetCanonicalChainId();
            if (!(canonicalChainId is Guid chainId))
            {
                throw new CommandExitedException("Canonical chain doesn't exists.", -1);
            }

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

            _stateStore.PruneStates(new[] {snapshotTipHash}.ToImmutableHashSet());


            var latestBlockEpoch = (int) (tip.Timestamp.ToUnixTimeSeconds() / blockEpochUnitSeconds);
            var latestBlockWithTx = GetLatestBlockWithTransaction<DummyAction>(tip, _store);
            var txTimeSecond = latestBlockWithTx.Transactions.Max(tx => tx.Timestamp.ToUnixTimeSeconds());
            var latestTxEpoch = (int) (txTimeSecond / txEpochUnitSeconds);

            _store.Dispose();
            _stateStore.Dispose();

            var snapshotFilename = $"snapshot-{latestBlockEpoch}-{latestTxEpoch}.zip";
            var snapshotPath = Path.Combine(outputDirectory, snapshotFilename);
            string workingDirectory = Path.Combine(Path.GetTempPath(), "snapshot");
            CleanStore(snapshotPath, storePath, workingDirectory);
            CloneDirectory(storePath, workingDirectory);

            var blockPath = Path.Combine(workingDirectory, "block");
            var txPath = Path.Combine(workingDirectory, "tx");
            CleanEpoch(blockPath, currentMetadataBlockEpoch);
            CleanEpoch(txPath, currentMetadataTxEpoch);

            ZipFile.CreateFromDirectory(workingDirectory, snapshotPath);
            if (snapshotTipDigest is null)
            {
                throw new CommandExitedException("Tip does not exists.", -1);
            }

            var snapshotTipHeader = snapshotTipDigest.Value.Header;
            JObject jsonObject = JObject.FromObject(snapshotTipHeader);
            jsonObject.Add("APV", apv);
            jsonObject = AddPreviousBlockEpoch(
                jsonObject,
                currentMetadataBlockEpoch,
                currentMetadataTxEpoch,
                previousMetadataBlockEpoch,
                latestBlockEpoch,
                latestTxEpoch,
                "PreviousBlockEpoch");
            jsonObject = AddPreviousTxEpoch(
                jsonObject,
                currentMetadataBlockEpoch,
                currentMetadataTxEpoch,
                previousMetadataTxEpoch,
                latestBlockEpoch,
                latestTxEpoch,
                "PreviousTxEpoch");
            jsonObject.Add("BlockEpoch", latestBlockEpoch);
            jsonObject.Add("TxEpoch", latestTxEpoch);
            var jsonString = JsonConvert.SerializeObject(jsonObject);

            var metadataFilename = $"snapshot-{latestBlockEpoch}-{latestTxEpoch}.json";
            var metadataPath = Path.Combine(outputDirectory, metadataFilename);
            if (File.Exists(metadataPath))
            {
                File.Delete(metadataPath);
            }

            File.WriteAllText(metadataPath, jsonString);
            Directory.Delete(workingDirectory, true);
        }

        private void CleanStore(string snapshotPath, string storePath, string workingDirectory)
        {
             if (File.Exists(snapshotPath))
             {
                 File.Delete(snapshotPath);
             }
 
             var blockPerceptPath = Path.Combine(storePath, "blockpercept");
             if (Directory.Exists(blockPerceptPath))
             {
                 Directory.Delete(blockPerceptPath, true);
             }
 
             var stagedTxPath = Path.Combine(storePath, "stagedtx");
             if (Directory.Exists(stagedTxPath))
             {
                 Directory.Delete(stagedTxPath, true);
             }
 
             if (Directory.Exists(workingDirectory))
             {
                 Directory.Delete(workingDirectory, true);
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
                if (block.PreviousHash is HashDigest<SHA256> newHash)
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

        private void CleanEpoch(string path, int currentMetadataEpoch)
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
                    if (epoch <= currentMetadataEpoch)
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
