using HogFishOne;
using UnitTestOne;

namespace UnitTestTwo
{
    [TestClass]
    public sealed class UnitTwo
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

        [TestMethod]
        public void SimpleDeleteTest()
        {
            string outFile = "jim.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // 1. Create keys.
                int totalCount = 20;
                int[] data = Enumerable.Range(1, totalCount).ToArray();

                // 2. Insert keys.
                foreach (int k in data)
                {
                    tree.Insert(new Element { Key = k, Data = k });
                }

                // 3. Verify Root Exists.
                Assert.IsTrue(tree.Header.RootId >= 0, "Root");

                // 4. Count Keys
                int keyCount = tree.CountKeys();
                Assert.AreEqual(totalCount, keyCount);

                // 5. Delete three keys.
                tree.Delete(5, 0);
                tree.Delete(10, 0);
                tree.Delete(15, 0);

                // 6. Verify count after deletions.
                keyCount = tree.CountKeys();
                Assert.AreEqual(totalCount - 3, keyCount);

                // 7. Verify the keys have been deleted.
                Element e;
                Assert.IsFalse(tree.TrySearch(5, out e), "Key 5 should be deleted");
                Assert.IsFalse(tree.TrySearch(10, out e), "Key 10 should be deleted");
                Assert.IsFalse(tree.TrySearch(15, out e), "Key 15 should be deleted");

                // 8. Full Audit.
                var report = tree.PerformFullAudit();
                Assert.IsTrue(report.Height <= 3, "Height must be 3 or less.");
                Assert.IsTrue(report.ReachableNodes < 150);
                Assert.IsTrue(report.TotalKeys < 150);
                Assert.AreEqual(0, report.ZombieCount, "Zombies");
                Assert.AreEqual(0, report.GhostCount, "Ghosts");
                if (tree.GetNodeCount() > 10)
                   Assert.IsTrue(report.AverageDensity > 25.0, "Low density");

                // 9. Integrity Check.
                tree.ValidateIntegrity();

                // 10. Delete all keys.
                for (int k = 1; k <= totalCount; k++)
                {
                    tree.Delete(k, 0);
                }

                Assert.AreEqual(0, tree.CountKeys(), "Tree should be empty");
            }
            File.Delete(outFile);
        }


        [TestMethod]
        public void SimpleDeleteFirstTest()
        {
            string outFile = "beta.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // 1. Create keys.
                int totalCount = 20;
                int[] data = Enumerable.Range(1, totalCount).ToArray();

                // 2. Insert keys.
                foreach (int i in data)
                {
                    tree.Insert(new Element { Key = i, Data = i });
                }

                // 3. Verify Root Exists.
                Assert.IsTrue(tree.Header.RootId >= 0, "Root");

                // 4. Count Keys
                int keyCount = tree.CountKeys();
                Assert.AreEqual(totalCount, keyCount);

                // 5. Delete all keys.
                for (int i = 0; i < data.Length; i++)
                {
                    tree.DeleteFirst();
                }

                Assert.AreEqual(0, tree.CountKeys(), "Tree should be empty");

                // 6. Integrity Check.
                tree.ValidateIntegrity();
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void SimpleDeleteLastTest()
        {
            string outFile = "charlie.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // 1. Create keys.
                int totalCount = 20;
                int[] data = Enumerable.Range(1, totalCount).ToArray();

                // 2. Insert keys.
                foreach (int k in data)
                {
                    tree.Insert(new Element { Key = k, Data = k });
                }

                // 3. Verify Root Exists.
                Assert.IsTrue(tree.Header.RootId >= 0, "Root");

                // 4. Count Keys
                int keyCount = tree.CountKeys();
                Assert.AreEqual(totalCount, keyCount);

                // 5. Delete all keys.
                for (int k = 0; k < totalCount; k++)
                {
                    tree.DeleteLast();
                }

                Assert.AreEqual(0, tree.CountKeys(), "Tree should be empty");

                // 6. Integrity Check.
                tree.ValidateIntegrity();
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestDeleteMiddleOut()
        {
            string outFile = "middle.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                int total = 100;
                for (int i = 1; i <= total; i++) tree.Insert(new Element { Key = i, Data = i });

                // Delete from the middle moving outwards
                int mid = total / 2;
                for (int offset = 0; offset < mid; offset++)
                {
                    tree.Delete(mid - offset, 0);
                    tree.Delete(mid + offset + 1, 0);
                    tree.ValidateIntegrity(); 
                }

                Assert.AreEqual(0, tree.CountKeys(), "Tree should be empty.");
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestDeleteRandomShuffle()
        {
            string outFile = "random.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                var keys = Enumerable.Range(1, 100).ToList();
                foreach (var k in keys) tree.Insert(k,k);

                var random = new Random(); 
                var shuffle = keys.OrderBy(x => random.Next()).ToList();

                foreach (var k in shuffle)
                {
                    tree.Delete(k, 0);

                    Element e;
                    Assert.IsFalse(tree.TrySearch(k, out e), $"Key {k} should be gone.");
                }

                Assert.AreEqual(0, tree.CountKeys());
                tree.ValidateIntegrity();
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestDeleteNonExistentKey()
        {
            string outFile = "missing.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                tree.Insert(10, 100);
                tree.Insert(20, 200);

                // Assuming Delete returns a bool or just doesn't crash
                tree.Delete(15, 0);
                tree.Delete(5, 0);
                tree.Delete(25, 0);

                Assert.AreEqual(2, tree.CountKeys(), "Tree count should remain unchanged.");
                tree.ValidateIntegrity();
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestSequentialDeleteThousand()
        {
            string outFile = "stress.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                int count = 1000;

                // 1. Bulk Insert
                for (int i = 1; i <= count; i++)
                {
                    tree.Insert(i, i);
                }
                Assert.AreEqual(count, tree.CountKeys(), "Failed initial insert count.");

                // 2. Sequential Delete
                for (int i = 1; i <= count; i++)
                {
                    tree.Delete(i, 0);

                    // Check integrity every 1000 deletes to save time but ensure safety
                    if (i % 100 == 0) tree.ValidateIntegrity();
                }

                Assert.AreEqual(0, tree.CountKeys(), "Tree should be empty.");
                tree.ValidateIntegrity();
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestRandomDeleteThousand()
        {
            string outFile = "bilbo.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                int count = 1000;
                var keys = Enumerable.Range(1, count).ToList();
                var random = new Random(); 

                // 1. Insert in random order
                var insertOrder = keys.OrderBy(x => random.Next()).ToList();
                foreach (var k in insertOrder)
                {
                    tree.AddOrUpdate(k, k); 
                }

                // 2. Delete in a different random order
                var deleteOrder = keys.OrderBy(x => random.Next()).ToList();
                foreach (var k in deleteOrder)
                {
                    tree.Delete(k, 0);
                }

                Assert.AreEqual(0, tree.CountKeys(), "Tree should be empty.");

                // Final Audit
                var report = tree.PerformFullAudit();
                Assert.AreEqual(0, report.TotalKeys, "Missing Keys");
                Assert.AreEqual(0, report.ZombieCount, "Zombies");
                Assert.AreEqual(0, report.GhostCount, "Ghosts");
            }
            File.Delete(outFile);
        }


    }
}
