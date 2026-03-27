using HogFishOne;

namespace UnitTestOne
{
    [TestClass]
    public sealed class UnitOne
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

        [TestMethod]
        public void SimpleInsertTest()
        {
            string outFile = "data.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // Insert keys in an order that forces splits.
                int[] keysToInsert = { 10, 20, 30, 40, 50, 25 };
                int totalCount = keysToInsert.Length;
                int[] sortedKeys = keysToInsert.OrderBy(k => k).ToArray();  

                foreach (int k in keysToInsert)
                {
                    tree.Insert(new Element { Key = k, Data = k });
                }

                // 1. Verify Root Exists.
                Assert.IsTrue(tree.Header.RootId >= 0, "Root");

                // 2. Count Keys
                int keyCount = tree.CountKeys();
                Assert.AreEqual(totalCount, keyCount);

                // 3. Get Keys and Verify Order & Uniqueness.
                var allKeys = tree.GetKeys();
                Assert.AreEqual(totalCount, allKeys.Count);
                Assert.IsTrue(allKeys.SequenceEqual(sortedKeys));
                Assert.IsTrue(Util.IsSorted(allKeys));
                Assert.IsFalse(Util.HasDuplicate(allKeys));

                // 4. Test Search
                int searchKey = 30;
                Assert.IsTrue(tree.TrySearch(searchKey, out Element found));
                Assert.AreEqual(searchKey, found.Key);

                // 5. Search every key.
                bool pass = allKeys.All(k => tree.TrySearch(k, out var e) && e.Data == k);
                Assert.IsTrue(pass, "Missing Keys");
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestSequential()
        {
            string outFile = "oak.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // 1. Insert sequential keys.
                int count = 100;
                for (int i = 1; i <= count; i++)
                {
                    tree.Insert(new Element(i, i));
                }

                // 2. Verify Root Exists.
                Assert.IsTrue(tree.Header.RootId >= 0, "Root");

                // 3. Count Keys.
                int keyCount = tree.CountKeys();
                Assert.AreEqual(count, keyCount);

                // 4. Get Keys and Verify Order & Uniqueness.
                var allKeys = tree.GetKeys();
                Assert.AreEqual(count, allKeys.Count);
                Assert.IsTrue(Util.IsSorted(allKeys));
                Assert.IsFalse(Util.HasDuplicate(allKeys));

                // 5. Verify Tree Height.
                // For Order 4, 100 items should be about 4 levels deep.
                int height = tree.GetHeight();
                Assert.IsTrue(height <= 3, "Height must be 3 or less.");

                // 6. Spot Check Search.
                int searchKey = 50;
                Assert.IsTrue(tree.TrySearch(searchKey, out Element result));
                Assert.IsTrue(result.Key == searchKey);

                // 7. Search every key.
                bool pass = allKeys.All(k => tree.TrySearch(k, out var e) && e.Data == k);
                Assert.IsTrue(pass, "Missing Keys");

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
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void RandomInsertionTest()
        {
            string outFile = "pine.db";
            File.Delete(outFile); 

            Random rnd = new Random();
            HashSet<int> insertedKeys = new HashSet<int>();

            using (var tree = new BTree(outFile, 256))
            {
                // 1. Insert random keys.
                int count = 100;
                while (insertedKeys.Count < count)
                {
                    int nextKey = rnd.Next(1, 10000);
                    if (insertedKeys.Add(nextKey))
                    {
                        tree.Insert(nextKey, nextKey);
                    }
                }

                // 2. Verify Root Exists.
                Assert.IsTrue(tree.Header.RootId >= 0, "Root");

                // 3. Count Keys
                int keyCount = tree.CountKeys();
                Assert.AreEqual(count, keyCount);

                // 4. Get Keys and Verify Order & Uniqueness.
                var allKeys = tree.GetKeys();
                Assert.AreEqual(count, allKeys.Count);
                Assert.IsTrue(Util.IsSorted(allKeys));
                Assert.IsFalse(Util.HasDuplicate(allKeys));

                // 5. Verify Tree Height.
                int height = tree.GetHeight();
                Assert.IsTrue(height <= 3, "Height must be 3 or less.");

                // 6. Search every key. 
                bool pass = allKeys.All(k => tree.TrySearch(k, out var e) && e.Data == k);
                Assert.IsTrue(pass, "Missing Keys");

                // 7. Full Audit.
                var report = tree.PerformFullAudit();
                Assert.IsTrue(report.Height <= 3, "Height must be 3 or less.");
                Assert.IsTrue(report.ReachableNodes < 150);
                Assert.IsTrue(report.TotalKeys < 150);
                Assert.AreEqual(0, report.ZombieCount, "Zombies");
                Assert.AreEqual(0, report.GhostCount, "Ghosts");
                if (tree.GetNodeCount() > 10)
                   Assert.IsTrue(report.AverageDensity > 25.0, "Low density");


                // 8. Integrity Check.
                tree.ValidateIntegrity();
            }
            File.Delete(outFile); // You've already mastered this!
        }

        [TestMethod]
        public void TestPersistence()
        {
            string outFile = "needle.db";
            File.Delete(outFile);

            int[] data = { 5, 15, 25, 35, 45 };

            // Phase 1: Write and Close
            using (var tree = new BTree(outFile, 256))
            {
                foreach (int x in data) tree.Insert(x, x);
            } 

            // Phase 2: Re-open and Verify
            using (var tree = new BTree(outFile))
            {
                foreach (int x in data)
                {
                    Assert.IsTrue(tree.TrySearch(x, out var result), $"Key {x} lost after reload!");
                    Assert.AreEqual(x, result.Data);
                }
            }
        }

        [TestMethod]
        public void TestRangeScanStandard()
        {
            string outFile = "cone.db";
            File.Delete(outFile);

            // Use a small order to ensure multiple leaves and a linked chain
            using (var tree = new BTree(outFile, 256))
            {
                // Insert 10, 20, 30, 40, 50, 60
                for (int i = 10; i <= 60; i += 10)
                    tree.Insert(i, i);

                // Test: Range 25 to 55
                // Should find: 30, 40, 50
                var results = tree.GetKeyRange(25, 55);

                Assert.AreEqual(3, results.Count);
                Assert.AreEqual(30, results[0]);
                Assert.AreEqual(40, results[1]);
                Assert.AreEqual(50, results[2]);
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestRangeScanEdgeCases()
        {
            string outFile = "john.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                for (int i = 10; i <= 30; i += 10)
                    tree.Insert(i, i);

                // 1. Range entirely below existing keys
                var below = tree.GetKeyRange(0, 5);
                Assert.AreEqual(0, below.Count);

                // 2. Range entirely above existing keys
                var above = tree.GetKeyRange(40, 50);
                Assert.AreEqual(0, above.Count);

                // 3. Range that covers everything
                var all = tree.GetKeyRange(0, 100);
                Assert.AreEqual(3, all.Count);

                // 4. Single item range
                var single = tree.GetKeyRange(20, 20);
                Assert.AreEqual(1, single.Count);
                Assert.AreEqual(20, single[0]);
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestBoundaries()
        {
            string outFile = "jane.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                int[] keys = { 50, 10, 30, 20, 40 };
                foreach (var k in keys) tree.Insert(k, k);

                Assert.AreEqual(10, tree.SelectFirst().Key);
                Assert.AreEqual(50, tree.SelectLast().Key);
            }
            File.Delete(outFile);
        }


        [TestMethod]
        public void TestUpdateValue()
        {
            string outFile = "craig.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // 1. Setup: Insert initial data
                tree.Insert(10, 100); // Key: 10, Data: 100
                tree.Insert(20, 200);
                tree.Insert(30, 300);

                // 2. Execute: Update the value for an existing key
                bool wasUpdated = tree.UpdateValue(20, 999);
                Assert.IsTrue(wasUpdated, "UpdateValue should return true for existing key.");

                // 3. Verify: Search to confirm the data changed
                tree.TrySearch(20, out var result);
                Assert.AreEqual(999, result.Data, "The data for key 20 was not updated correctly.");

                // 4. Persistence Check: Close and reopen to ensure DiskWrite happened
            }

            using (var tree = new BTree(outFile))
            {
                tree.TrySearch(20, out var result);
                Assert.AreEqual(999, result.Data, "Updated value did not persist to disk.");

                // 5. Edge Case: Attempt to update a non-existent key
                bool failedUpdate = tree.UpdateValue(99, 123);
                Assert.IsFalse(failedUpdate, "UpdateValue should return false for missing key.");
            }
            File.Delete(outFile);
        }

        [TestMethod]
        public void TestSave()
        {
            string outFile = "dog.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // 1. Setup: Initial data
                tree.Insert(10, 100);
                tree.Insert(20, 200);

                // 2. Save: Update existing key (20)
                tree.AddOrUpdate(20, 999);

                // 3. Save: Insert brand new key (30)
                tree.AddOrUpdate(30, 300);

                // Verify intermediate state
                bool found20 = tree.TrySearch(20, out var res20);
                bool found30 = tree.TrySearch(30, out var res30);

                Assert.IsTrue(found20, "Key 20 should exist.");
                Assert.AreEqual(999, res20.Data, "Save failed to update existing key 20.");
                Assert.IsTrue(found30, "Key 30 should have been inserted by Save.");
                Assert.AreEqual(300, res30.Data, "Save failed to insert new key 30.");
            }

            // 4. Persistence Check: Re-open the file
            using (var tree = new BTree(outFile))
            {
                bool found20 = tree.TrySearch(20, out var res20);
                bool found30 = tree.TrySearch(30, out var res30);

                Assert.AreEqual(999, res20.Data, "Updated value 999 did not persist.");
                Assert.AreEqual(300, res30.Data, "Inserted value 300 did not persist.");
            }

            File.Delete(outFile);
        }

    }
}
