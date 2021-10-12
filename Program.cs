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
        private MonoRocksDBStore _store;
        private TrieStateStore _stateStore;
        private HashAlgorithmGetter _hashAlgorithmGetter;

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
            string defaultStorePath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "planetarium",
                "9c"
            );
            outputDirectory = string.IsNullOrEmpty(outputDirectory)
                ? Environment.CurrentDirectory
                : outputDirectory;
            storePath = string.IsNullOrEmpty(storePath) ? defaultStorePath : storePath;
            if (!Directory.Exists(storePath))
            {
                throw new CommandExitedException("Invalid store path. Please check --store-path is valid.", -1);
            }

            var statesPath = Path.Combine(storePath, "states");
            var stateHashesPath = Path.Combine(storePath, "state_hashes");
            var txexecPath = Path.Combine(storePath, "txexec");

            _hashAlgorithmGetter = (_ => HashAlgorithmType.Of<SHA256>());
            _store = new MonoRocksDBStore(storePath);
            IKeyValueStore stateKeyValueStore = new RocksDBKeyValueStore(statesPath);
            _stateStore = new TrieStateStore(stateKeyValueStore);

            var canonicalChainId = _store.GetCanonicalChainId();
            if (canonicalChainId is Guid chainId)
            {
                var genesisHash = _store.IterateIndexes(chainId,0, 1).First();
                var tipHash = _store.IterateIndexes(chainId, 0, null).Last();
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

                Fork(chainId, forkedId, genesisHash, snapshotTipHash, tip);

                _store.SetCanonicalChainId(forkedId);
                foreach (var id in _store.ListChainIds().Where(id => !id.Equals(forkedId)))
                {
                    _store.DeleteChainId(id);
                }

                var snapshotTipDigest = _store.GetBlockDigest(snapshotTipHash);
                var snapshotTipStateRootHash = _store.GetStateRootHash(snapshotTipHash);

                _stateStore.PruneStates(
                    ImmutableHashSet<HashDigest<SHA256>>.Empty
                        .Add((HashDigest<SHA256>)snapshotTipStateRootHash));

                _store.Dispose();
                _stateStore.Dispose();

                var genesisHashHex = ByteUtil.Hex(genesisHash.ToByteArray());
                var snapshotTipHashHex = ByteUtil.Hex(snapshotTipHash.ToByteArray());
                var snapshotFilename = $"{genesisHashHex}-snapshot-{snapshotTipHashHex}.zip";
                var snapshotPath = Path.Combine(outputDirectory, snapshotFilename);
                if (File.Exists(snapshotPath))
                {
                    File.Delete(snapshotPath);
                }

                var blockPerceptPath = Path.Combine(storePath, "blockpercept");
                if (Directory.Exists(blockPerceptPath))
                {
                    Directory.Delete(blockPerceptPath, true);
                }

                if (Directory.Exists(txexecPath))
                {
                    Directory.Delete(txexecPath, true);
                }

                ZipFile.CreateFromDirectory(storePath, snapshotPath);

                if (snapshotTipDigest is null)
                {
                    throw new CommandExitedException("Tip does not exists.", -1);
                }

                var snapshotTipHeader = snapshotTipDigest.Value.GetHeader(_hashAlgorithmGetter);
                
                JObject jsonObject = JObject.FromObject(snapshotTipHeader);
                jsonObject.Add("APV", apv);
                var jsonString = JsonConvert.SerializeObject(jsonObject);

                var metadataFilename = $"{genesisHashHex}-snapshot-{snapshotTipHashHex}.json";
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
            BlockHash genesisHash,
            BlockHash branchpointHash,
            Block<DummyAction> tip)
        {
            var branchPoint = _store.GetBlock<DummyAction>(_hashAlgorithmGetter, branchpointHash);
            _store.AppendIndex(dest, genesisHash);
            _store.ForkBlockIndexes(src, dest, branchpointHash);

            var signersToStrip = new Dictionary<Address, int>();

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
