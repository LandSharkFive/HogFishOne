using HogFishOne;
using UnitTestOne;

namespace UnitTestFour
{

    [TestClass]
    public sealed class UnitThree
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;


        [TestMethod]
        public void IntegrityCheckOne()
        {
            string path = "carrot.db";
            File.Delete(path);

            using (var tree = new BTree(path))
            {
                // 1. Build a specific structure: Root with two children
                // Inserting 10, 20, 30. With Order 4, this may stay in one node.
                // We insert more to force a split.
                int[] keys = { 10, 20, 30, 40, 50, 60 };
                foreach (var k in keys) tree.Insert(k, k * 100);

                // 2. Perform a deletion that triggers MergeChildren
                // We delete keys until a node hits t-1 and its sibling is also thin.
                tree.Delete(60, 6000);
                tree.Delete(50, 5000);

                int count = tree.CountKeys();
                Assert.AreEqual(4, count, "Missing Keys.");

                // 3. Run Integrity Check prior to compaction
                // This ensures Case 3 didn't leave orphaned IDs in Kids[]
                tree.ValidateIntegrity();

                // 4. Verify physical space management
                // Ensure deleted node IDs were pushed to FreeList.
                tree.Compact();
                Assert.IsTrue(tree.Header.NodeCount < 5, "Compaction failed.");

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }

        [TestMethod]
        public void CompactTestOne()
        {
            string path = "delta.db";
            File.Delete(path);

            int count = 1000;
            int deleteCount = 800;

            using (var tree = new BTree(path, 256))
            {
                var random = new Random();
                var keys = new HashSet<int>();

                while (keys.Count < count)
                {
                    int k = random.Next(1, 100000);
                    if (keys.Add(k)) tree.Insert(k, k * 2);
                }

                // Delete a significant portion to create major fragmentation holes in the file.
                var keysToDelete = keys.Take(deleteCount).ToList();
                foreach (var k in keysToDelete)
                {
                    tree.Delete(k, k * 2);
                    keys.Remove(k);
                }

                tree.ValidateIntegrity();

                long sizeBefore = new FileInfo(path).Length;

                // COMPACT
                tree.Compact();

                long sizeAfter = new FileInfo(path).Length;

                // 1. PHYSICAL ASSERT: File must be smaller.
                Assert.IsTrue(sizeAfter < sizeBefore, $"Compaction failed. Before: {sizeBefore}, After: {sizeAfter}");

                // 2. INTEGRITY ASSERT: Root must be valid.
                Assert.IsFalse(tree.Header.RootId < 0, "Root lost");

                // 3. DATA ASSERT: Every remaining key must still be searchable and correct.
                foreach (var k in keys)
                {
                    Element result;
                    Assert.IsTrue(tree.TrySearch(k, out result), $"Key {k} missing");
                    Assert.AreEqual(k * 2, result.Data, "Corrupted");
                }

                // 4. STRUCTURE ASSERT: Ensure the B+ Tree logic still holds
                tree.CheckGhost();
                count = tree.CountKeys();
                Assert.AreEqual(keys.Count, count, "Missing Keys");
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");

                // 5. LEAF CHAIN ASSERT: Verify horizontal integrity
                // If NextLeafId mapping failed, this will crash or return a wrong count.
                int leafLinkCount = tree.GetLeafChainCount();
                Assert.IsTrue(leafLinkCount > 0, "Leaf chain broken");

                // 6. FREELIST ASSERT: Ensure the holes were truly welded shut.
                Assert.AreEqual(0, tree.Header.FreeListCount, "FreeList not cleared");
                Assert.AreEqual(0, tree.Header.FreeListOffset, "FreeListOffset should be 0.");

                // 7. SORTED ORDER ASSERT: Walk the leaf chain and ensure keys are strictly increasing.
                int? lastKey = null;
                // This assumes you have a way to iterate through all keys in order, 
                // or you can traverse the leaf chain manually.
                var sortedList = tree.GetKeys();
                foreach (var currentKey in sortedList)
                {
                    if (lastKey.HasValue)
                    {
                        Assert.IsTrue(currentKey > lastKey.Value,
                            $"Sort Violation: Key {currentKey} is not greater than preceding key {lastKey.Value}");
                    }
                    lastKey = currentKey;
                }
            }

            File.Delete(path);
        }

        [TestMethod]
        public void CompactPreservesData()
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            string path = "sam.db";
            File.Delete(path);

            int count = 1000;
            int deleteCount = 100;

            var rnd = new Random();
            var keys = new HashSet<int>();

            using (var tree = new BTree(path))
            {
                while (keys.Count < count)
                {
                    int k = rnd.Next(1, 1000000);
                    if (keys.Add(k)) tree.Insert(k, k * 2);
                }

                // Delete some keys
                var toDelete = keys.Take(deleteCount).ToList();
                foreach (var k in toDelete)
                {
                    tree.Delete(k, k * 2);
                    keys.Remove(k);
                }

                tree.ValidateIntegrity();
                long before = new FileInfo(path).Length;

                tree.Compact();

                long after = new FileInfo(path).Length;

                // File should shrink and data preserved
                Assert.IsTrue(after <= before, $"Compact did not shrink file: before={before} after={after}");

                // All remaining keys must still be searchable
                foreach (var k in keys)
                {
                    Element e;
                    Assert.IsTrue(tree.TrySearch(k, out e), $"Missing key after compact: {k}");
                    Assert.AreEqual(k * 2, e.Data);
                }

                tree.ValidateIntegrity();
                Assert.AreEqual(0, tree.CountZombies(), "Zombies present after compact");
            }

            File.Delete(path);
        }


    }
}
