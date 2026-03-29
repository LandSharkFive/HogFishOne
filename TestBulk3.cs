using HogFishOne;
using UnitTestOne;

namespace UnitTestThree
{

    [TestClass]
    public sealed class UnitThree
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

        [TestMethod]
        public void SimpleBulkLoadTest()
        {
            // 1. Setup Data
            string path = "ArcTest.dat";
            var builder = new TreeBuilder(1024, leafFill: 0.75); // Small order to force multiple levels
            var elements = Enumerable.Range(1, 100)
                                     .Select(i => new Element { Key = i, Data = i * 10 })
                                     .ToList();

            // 2. Execute Bulk Load
            builder.CreateFromSorted(path, elements);

            // 3. Verify with BTree Class (The "Second Opinion")
            using (var tree = new BTree(path))
            {
                // Check structural integrity via your existing audit
                int horizontalCount = tree.GetLeafChainCount();

                // Assert: Do we have all 100 keys in the leaf chain?
                Assert.AreEqual(100, horizontalCount, "Leaf chain count does not match input size.");

                // Assert: Is the RootId actually set in the header?
                Assert.AreNotEqual(-1, tree.Header.RootId, "Root ID was not saved to header.");

                // Assert: Range Scan - Can we walk the chain from 20 to 50?
                var range = tree.GetElementRange(20, 50);
                Assert.AreEqual(31, range.Count, "Range scan failed to return correct number of elements.");
                Assert.AreEqual(20, range.First().Key);
                Assert.AreEqual(50, range.Last().Key);
            }

            // Cleanup
            File.Delete(path);
        }

        [TestMethod]
        [DataRow(0.7)]
        [DataRow(0.8)]
        [DataRow(0.9)]
        [DataRow(1.0)]
        public void BulkLoadTestByFill(double fill)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            Console.WriteLine($"--- Leaf Fill {fill} ---");
            string path = TestHelper.GetTempDb();
            File.Delete(path);
            int elementCount = 101;

            var builder = new TreeBuilder(1024, leafFill: fill);
            var elements = Enumerable.Range(1, elementCount)
                                     .Select(i => new Element { Key = i, Data = i * 7 })
                                     .ToList();

            // 1. Build the tree
            builder.CreateFromSorted(path, elements);

            // 2. Audit the result
            using (var tree = new BTree(path))
            {
                int horizontalCount = tree.GetLeafChainCount();

                // If this fails, we've found a specific order that breaks the chain.
                Assert.AreEqual(elementCount, horizontalCount,
                    $"Expected {elementCount} keys in chain, Found {horizontalCount}");

                var report = tree.PerformFullAudit();
                Assert.IsTrue(report.Height <= 5, "Height must be 5 or less.");
                Assert.IsTrue(report.ReachableNodes < 150);
                Assert.IsTrue(report.TotalKeys < 180);
                Assert.AreEqual(0, report.ZombieCount, "Zombies");
                Assert.AreEqual(0, report.GhostCount, "Ghosts");
                if (tree.GetNodeCount() > 10)
                    Assert.IsTrue(report.AverageDensity > 25.0, "Low density");
            }

            File.Delete(path);
        }


        [TestMethod]
        [DataRow(1024, 0.9, 5000)]
        [DataRow(2048, 1.0, 10000)]
        [DataRow(4096, 0.9, 20000)]
        [DataRow(4086, 1.0, 10000)]
        [DataRow(4096, 0.9, 20000)]
        public void LargeBulkLoad(int pageSize, double fill, int totalCount)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            string path = TestHelper.GetTempDb();
            File.Delete(path);

            // 1. Prepare Large Dataset
            var elements = Enumerable.Range(1, totalCount)
                                     .Select(i => new Element { Key = i, Data = i })
                                     .ToList();

            var builder = new TreeBuilder(pageSize, leafFill: fill);

            // 2. Measure Performance
            var sw = System.Diagnostics.Stopwatch.StartNew();
            builder.CreateFromSorted(path, elements);
            sw.Stop();

            Console.WriteLine($"Bulk Loaded {totalCount} elements in {sw.ElapsedMilliseconds} ms");

            // 3. Audit for Integrity
            using (var tree = new BTree(path))
            {
                var report = tree.PerformFullAudit();
                Console.WriteLine($"Height: {report.Height}");
                Console.WriteLine($"Leaf Chain Count: {report.LeafChainCount}");
                Assert.AreEqual(totalCount, report.LeafChainCount, "Leaf chain is broken.");
                Assert.AreEqual(0, report.ZombieCount, "Zombies");
                Assert.AreEqual(0, report.GhostCount, "Ghosts");
                if (tree.GetNodeCount() > 10)
                   Assert.IsTrue(report.AverageDensity > 25.0, "Low density");
            }

            File.Delete(path);
        }

        /// <summary>
        /// An insertion test for a small number of items.  Ten items or less.
        /// Test searches. Test one delete. Test one min and one max.
        /// This is a general purpose sanity check for the code.
        /// </summary>
        [TestMethod]
        public void BulkTestHundredOne()
        {
            string myPath = "bacon.db";
            File.Delete(myPath);

            // 1. Generate 100 sorted keys
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 100; i++) data.Add(new Element(i * 10, i + 5));

            // 2. Run Bulk Loader.  Default Capacity (80%).
            var builder = new TreeBuilder(1024);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Verify results using your existing BTree methods
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // 6. Check for ghosts
                tree.CheckGhost();

                // 7. Verify keys.
                var list = tree.GetKeys();
                Assert.AreEqual(data.Count, list.Count);
                Assert.IsTrue(Util.IsSorted(list), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(list), "Duplicate found");

                // 8. Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }

        [TestMethod]
        public void BulkTestHundredTwo()
        {
            string myPath = "apple.db";
            File.Delete(myPath);

            // 1. Generate 100 sorted keys
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 100; i++) data.Add(new Element(i * 10, i * 100));

            // 2. Run Bulk Loader.  Default Capacity (80%).
            var builder = new TreeBuilder(1024);
            builder.CreateFromSorted(myPath, data);

            // 3. Open File.
            using (var tree = new BTree(myPath, 1024))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");


                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Search for keys.
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // 6. Check for ghosts
                tree.CheckGhost();

                // 7. Verify keys.
                var list = tree.GetKeys();
                Assert.AreEqual(data.Count, list.Count);
                Assert.IsTrue(Util.IsSorted(list), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(list), "Duplicate found");

                // 8. Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }

        [TestMethod]
        public void BulkTestFullCapacityFive()
        {
            string myPath = "blue.db";
            File.Delete(myPath);

            // 1. Generate 100 sorted keys
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 100; i++) data.Add(new Element(i * 10, i * 10));

            // 2. Run Bulk Loader.  Default Capacity (80%).
            var builder = new TreeBuilder(512);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Verify results using your existing BTree methods
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // 6. Verify keys.
                var list = tree.GetKeys();
                Assert.AreEqual(data.Count, list.Count);
                Assert.IsTrue(Util.IsSorted(list), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(list), "Duplicate key found.");

                // 7. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }

        [TestMethod]
        public void BulkLoadFullCapacityOne()
        {
            string myPath = "toast.db";
            File.Delete(myPath);

            // 1. Generate 16 sorted keys. 
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 16; i++) data.Add(new Element(i, i));

            // 2. Run Bulk Loader.  Full Capacity (80%).
            var builder = new TreeBuilder(512, leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Search for keys.
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // 6. Verify keys.  
                var sortedKeys = tree.GetKeys();
                Assert.IsTrue(Util.IsSorted(sortedKeys), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(sortedKeys), "Duplicate key found.");

                // 7. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }

        [TestMethod]
        public void BulkLoadFullCapacityTwo()
        {
            string myPath = "bear.db";
            File.Delete(myPath);

            // 1. Generate 100 sorted keys.  
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 100; i++) data.Add(new Element(i, i));

            // 2. Run Bulk Loader.  Full Capacity (80%).
            var builder = new TreeBuilder(512, leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Search for keys.
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // 6. Verify keys.
                var sortedKeys = tree.GetKeys();
                Assert.IsTrue(Util.IsSorted(sortedKeys), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(sortedKeys), "Duplicate key found.");

                // 7. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }

        [TestMethod]
        public void BulkLoadFullCapacityThree()
        {
            string myPath = "jam.db";
            File.Delete(myPath);

            // 1. Generate 50 sorted keys.
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 50; i++) data.Add(new Element(i, i));
            List<int> extra = new List<int>();
            for (int i = 51; i <= 60; i++) extra.Add(i);

            // 2. Run Bulk Loader.  Full Capacity (80%).
            var builder = new TreeBuilder(512,leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Search for keys.
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // Insert keys.
                foreach (int i in extra) tree.Insert(i, i * 10);

                // 7. Search for keys.
                foreach (int i in extra)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(i, out pair), $"Missing Key {i}");
                }

                // 8. Verify keys.
                var sortedKeys = tree.GetKeys();
                Assert.IsTrue(Util.IsSorted(sortedKeys), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(sortedKeys), "Duplicate keys found.");


                // 9. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }

        [TestMethod]
        public void BulkLoadFullCapacityThirtyOne()
        {
            string myPath = "sugar.db";
            File.Delete(myPath);

            // 1. Generate 24 sorted keys. 
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 24; i++) data.Add(new Element(i, i));
            List<int> extra = new List<int>();
            for (int i = 25; i <= 30; i++) extra.Add(i);

            // 2. Run Bulk Loader.  Full Capacity (100%).
            var builder = new TreeBuilder(512,leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Search for keys.
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // 6. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");

                // Insert keys.
                foreach (int i in extra) tree.Insert(i, i * 10);

                // 7. Search for keys.
                foreach (int i in extra)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(i, out pair), $"Missing Key {i}");
                }

                // 8. Check for zombies again.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }

        [TestMethod]
        public void BulkLoadFullCapacityFour()
        {
            string myPath = "beef.db";
            File.Delete(myPath);

            // 1. Generate 80 sorted keys. 
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 80; i++) data.Add(new Element(i, i));
            List<int> extra = new List<int>();
            for (int i = 81; i <= 100; i++) extra.Add(i);

            // 2. Run Bulk Loader.  Full Capacity (100%).
            var builder = new TreeBuilder(512,leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. All keys must exist.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Bulk Load Failed.");

                // 5. Search for keys.
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                }

                // 6. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");

                // 7. Insert keys. 
                foreach (int i in extra) tree.Insert(i, i * 10);

                // 8. Count the keys.
                count = tree.CountKeys();
                int totalKeys = data.Count + extra.Count;
                Assert.AreEqual(totalKeys, count, "Key counts must match.");

                // 9. Search for keys.
                foreach (int i in extra)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(i, out pair), $"Missing Key {i}");
                }

                // 10. Check the keys.
                var sortedKeys = tree.GetKeys();
                Assert.IsTrue(Util.IsSorted(sortedKeys), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(sortedKeys), "Duplicate keys found.");

                // 11. Check for zombies again.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(myPath);
        }


        [TestMethod]
        [DataRow(1024, 0.9, 500)]
        [DataRow(1024, 0.8, 1000)]
        [DataRow(1024, 0.8, 1000)]
        [DataRow(4096, 0.7, 1000)]
        [DataRow(4096, 0.6, 1000)]

        public void StressTestDelta(int pageSize, double fill, int totalKeys)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            string testPath = TestHelper.GetTempDb();   
            File.Delete(testPath);

            // 1. Setup: Generate sorted keys
            var testKeys = Enumerable.Range(1, totalKeys)
                .Select(i => new Element { Key = i, Data = i * 10 }).ToList();

            var builder = new TreeBuilder(leafFill: fill);

            builder.CreateFromSorted(testPath, testKeys);

            using (var tree = new BTree(testPath))
            {

                // 1. Count the keys.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");
                int count = tree.CountKeys();
                Assert.AreEqual(totalKeys, count, "Key Count must match.");

                // 2. Search every key.
                var allKeys = tree.GetKeys();
                bool pass = allKeys.All(k => tree.TrySearch(k, out var e) && e.Data == k * 10);
                Assert.IsTrue(pass, "Missing Keys");
                Assert.IsTrue(tree.GetHeight() < 10, "Height must be be 10 or less.");

                // 3. Run the high-speed single-pass audit.
                var report = tree.PerformFullAudit();
                Assert.AreEqual(0, report.ZombieCount, "Zombies");
                Assert.AreEqual(0, report.GhostCount, "Ghosts");
                if (tree.GetNodeCount() > 10)
                    Assert.IsTrue(report.AverageDensity > 25.0, "Density must be 25% or more.");
            }
            File.Delete(testPath);
        }



    }
}
