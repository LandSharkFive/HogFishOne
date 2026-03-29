using HogFishOne;
using UnitTestOne;

namespace UnitTestFive
{

    [TestClass]
    public sealed class UnitFive
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

        [TestMethod]
        public void StressTestRandomThousand()
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            string path = "kilo.db";
            File.Delete(path);

            int count = 1000;
            using (var tree = new BTree(path, 1024))
            {
                var random = new Random();
                var keys = new HashSet<int>();

                // 1. Bulk Insertion of Random Keys
                while (keys.Count < count)
                {
                    int k = random.Next(1, 100000);
                    if (keys.Add(k))
                    {
                        tree.Insert(k, k * 2);
                    }
                }

                // 2. Integrity Check
                tree.CheckGhost(); // Ensure no internal nodes were emptied incorrectly
                var sortedKeys = tree.GetKeys();
                Assert.AreEqual(count, sortedKeys.Count, "Missing Keys.");
                Assert.IsTrue(Util.IsSorted(sortedKeys), "Not sorted.");

                // 3. Random Search Verification
                foreach (var k in keys.Take(100))
                {
                    Element result;
                    Assert.IsTrue(tree.TrySearch(k, out result), $"Missing key {k}");
                    Assert.AreEqual(k * 2, result.Data);
                }

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }

        [TestMethod]
        public void MergeTestOne()
        {
            string path = "pickle.db";
            File.Delete(path);

            using (var tree = new BTree(path, 1024))
            {
                List<int> keys = new List<int>();
                for (int i = 1; i <= 20; i++) keys.Add(i * 10);

                foreach (var k in keys) tree.Insert(k, k);

                // 1. Verify keys exist.    
                int count = tree.CountKeys();
                Assert.AreEqual(keys.Count, count, "Missing Keys.");

                // 2. Delete to trigger a merge that propagates upward.
                // Deleting 90 should thin the right side, 80 continues the collapse.
                tree.Delete(90, 90);
                tree.Delete(80, 80);
                tree.Delete(70, 70);

                // 3. Verify the root wasn't orphaned and height updated.
                tree.CheckGhost();
                Element item;
                Assert.IsTrue(tree.TrySearch(10, out item));
                Assert.IsTrue(tree.TrySearch(20, out item));
                Assert.IsTrue(tree.TrySearch(30, out item));
                count = tree.CountKeys();
                Assert.AreEqual(keys.Count - 3, count, "Missing Keys.");

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }

        [TestMethod]
        public void BorrowFromInternalNodeOne()
        {
            string path = "skunk.db";
            File.Delete(path);

            using (var tree = new BTree(path, 256))
            {
                // Build a balanced tree
                for (int i = 1; i <= 15; i++) tree.Insert(i, i);

                // Delete keys to force an internal node to borrow from a sibling
                // This ensures Kids[0] of the sibling correctly becomes Kids[N] of the borrower
                tree.Delete(1, 1);
                tree.Delete(2, 2);

                // Ensure integrity check passes (no orphaned keys).
                var keys = tree.GetKeys();
                Assert.IsTrue(Util.IsSorted(keys));
                Assert.IsFalse(Util.HasDuplicate(keys));
                int count = tree.CountKeys();
                Assert.AreEqual(13, count, "Missing Keys.");

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }

        [TestMethod]
        public void OddEvenTestOne()
        {
            string path = "pear.db";
            File.Delete(path);

            using (var tree = new BTree(path, 256))
            {
                var range = Enumerable.Range(1, 100).ToList();
                foreach (var i in range) tree.Insert(i, i);

                // Verify the numbers exist.
                int count = tree.CountKeys();
                Assert.AreEqual(range.Count, count, "Missing Keys.");

                // Delete even numbers
                foreach (var i in range.Where(n => n % 2 == 0))
                {
                    tree.Delete(i, i);
                    Element item;
                    Assert.IsFalse(tree.TrySearch(i, out item), $"Deleted key {i} found.");
                }

                // Verify odd numbers still exist
                foreach (var i in range.Where(n => n % 2 != 0))
                {
                    Element item;
                    Assert.IsTrue(tree.TrySearch(i, out item), $"Valid key {i} missing.");
                }

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }

        [TestMethod]
        public void HardTestOne()
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            string path = "hard.db";
            File.Delete(path);

            using (var tree = new BTree(path, 2048))
            {
                Random rng = new Random();
                HashSet<int> tracker = new HashSet<int>();

                // Phase 1: Heavy Churn
                for (int i = 0; i < 2000; i++)
                {
                    int val = rng.Next(1, 10000);

                    // 70% chance to insert, 30% chance to delete
                    if (rng.NextDouble() < 0.7)
                    {
                        if (!tracker.Contains(val))
                        {
                            tree.Insert(val, val);
                            tracker.Add(val);
                        }
                    }
                    else if (tracker.Count > 0)
                    {
                        int toDelete = tracker.ElementAt(rng.Next(tracker.Count));
                        tree.Delete(toDelete, toDelete);
                        tracker.Remove(toDelete);
                    }

                    // Verify integrity every 100 ops.
                    if (i > 0 && i % 100 == 0)
                    {
                        tree.CheckGhost();
                        int keyCount = tree.CountKeys();
                        Assert.AreEqual(tracker.Count, keyCount, "Missing Keys");
                    }
                }

                // Phase 2: Total Liquidation
                foreach (var remaining in tracker.ToList())
                {
                    tree.Delete(remaining, remaining);
                }

                int count = tree.CountKeys();
                Assert.AreEqual(0, count, "Tree should be empty.");
                int rootId = tree.Header.RootId;
                Assert.IsTrue(rootId == -1 || tree.DiskRead(rootId).NumKeys == 0);

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }


    }
}
