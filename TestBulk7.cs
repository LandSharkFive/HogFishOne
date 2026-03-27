using HogFishOne;
using UnitTestOne;

namespace UnitTestSeven
{

    [TestClass]
    public sealed class UnitSeven
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

        /// <summary>Bulk Load with Low Volume.</summary>
        [TestMethod]
        public void TestBulkLoadFullKeys16()
        {
            string myPath = "celery.db";
            File.Delete(myPath);

            // 1. Generate sorted data.
            var data = Enumerable.Range(1, 16).Select(i => new Element(i, i)).ToList();

            // 2. Run Bulk Loader.  Set fill factors to 1.0 (100%). 
            var builder = new TreeBuilder(256, leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Check keys counts.
                int total = tree.CountKeys();
                Assert.AreEqual(data.Count, total, "Missing Keys");

                // 5. Check keys.
                var list = tree.GetKeys();
                Assert.IsTrue(Util.IsSorted(list), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(list), "Duplicate found");

                // 7. Check searches. 
                foreach (var key in data)
                {
                    int i = key.Key;
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(i, out pair), $"Missing Key {i}");
                }
            }

            File.Delete(myPath);
        }

        /// <summary>Bulk Load with  Low Volume Inserts.</summary>
        [TestMethod]
        public void BulkLoadFullKeys20()
        {
            string myPath = "cherry.db";
            File.Delete(myPath);

            // 1. Generate 16 sorted keys.
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 16; i++) data.Add(new Element(i, i));

            // 2. Run Bulk Loader.  Set fill factors to 1.0 (100%). 
            var builder = new TreeBuilder(256, leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 5. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Missing Keys");

                // 5. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
                Assert.AreEqual(0, tree.GetFreeListCount(), "Free Nodes");

                // 6. Insert 4 more keys to force splits.
                for (int i = 17; i <= 20; i++) tree.Insert(i, i);

                // 7. Check counts again.
                count = tree.CountKeys();
                Assert.AreEqual(20, count, "Missing Keys");

                // 8. Check for zombies again.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
                Assert.AreEqual(0, tree.GetFreeListCount(), "Free Nodes");
            }

            File.Delete(myPath);
        }


        /// <summary>Bulk Load with Low Volume Inserts.</summary>
        [TestMethod]
        public void BulkLoadFullKeysThirty()
        {
            string myPath = "oats.db";
            File.Delete(myPath);

            // 1. Generate 24 sorted keys. 
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 24; i++) data.Add(new Element(i, i));

            // 2. Run Bulk Loader.  Set fill factors to 1.0 (100%). 
            var builder = new TreeBuilder(256, leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Missing Keys");

                // 5. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
                Assert.AreEqual(0, tree.GetFreeListCount(), "Free Nodes");

                // 6. Insert 6 more keys to force splits.
                for (int i = 25; i <= 30; i++) tree.Insert(i, i);

                // 7. Check counts again.
                count = tree.CountKeys();
                Assert.AreEqual(30, count, "Missing Keys After Insert.");

                // 8. Check for zombies again.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
                Assert.AreEqual(0, tree.GetFreeListCount(), "Free Nodes");
            }

            File.Delete(myPath);
        }

        /// <summary>
        /// Bulk Load with Moderate Volume.  This test is designed to create a tree with multiple levels and to verify 
        /// that the bulk loader correctly handles the creation of internal nodes and the promotion of keys.  After the bulk load,
        /// insert additional keys to force splits and check that the tree remains consistent and that no zombies are created.
        /// </summary>
        [TestMethod]
        public void BulkLoadFullKeysFifty()
        {
            string myPath = "wheat.db";
            File.Delete(myPath);

            // 1. Generate 50 sorted keys.
            List<Element> data = new List<Element>();
            for (int i = 1; i <= 50; i++) data.Add(new Element(i, i));

            // 2. Run Bulk Loader.  Set fill factors to 1.0 (100%). 
            var builder = new TreeBuilder(256, leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Verify all keys are present.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count, "Missing Keys");

                // 5. Check for zombies.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
                Assert.AreEqual(0, tree.GetFreeListCount(), "Free Nodes");

                // 6. Insert 10 more keys to force splits.
                for (int i = 51; i <= 60; i++) tree.Insert(i, i);

                // 7. Check counts again.
                count = tree.CountKeys();
                Assert.AreEqual(60, count, "Missing Keys After Insert.");

                // 8. Check for zombies again.
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
                Assert.AreEqual(0, tree.GetFreeListCount(), "Free Nodes");
            }

            File.Delete(myPath);
        }


        /// <summary>   
        /// Bulk Load with High Volume and at Full Capacity.
        /// </summary>    
        [TestMethod]
        [DataRow(512, 5)]
        [DataRow(2048, 255)]
        [DataRow(2048, 500)]
        [DataRow(4096, 1000)]
        public void StressTestAlpha(int pageSize, int count)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            Random rnd = new Random();
            string myPath = TestHelper.GetTempDb();    
            File.Delete(myPath);

            // Generate sorted data
            var data = Enumerable.Range(1, count)
                                 .Select(i => new Element(i, i))
                                 .ToList();

            // 2. Run Bulk Loader.
            var builder = new TreeBuilder(pageSize, leafFill: 1.0);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 4. Run the high-speed single-pass audit
                var report = tree.PerformFullAudit();

                // 5. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 6. Get the keys for the sort and count checks.
                var keys = tree.GetKeys();

                // 7. Integrity Checks
                Assert.AreEqual(count, keys.Count, "Missing Keys");
                Assert.IsTrue(Util.IsSorted(keys), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(keys), "Duplicates");
                Assert.AreEqual(0, report.ZombieCount, "Zombie");
                Assert.AreEqual(0, report.GhostCount, "Ghost");
                Assert.IsTrue(report.Height < 10, "Height must be less than 10.");
                Assert.IsTrue(tree.GetFreeListCount() < 8, "Free Nodes");
                if (tree.GetNodeCount() > 10)
                   Assert.IsTrue(report.AverageDensity > 25.0, "Low Density");


                // 8. Check Max.
                Element? lastKey = tree.SelectLast();
                if (lastKey.HasValue)
                {
                    Assert.AreEqual(count, lastKey.Value.Key, "Max Key Search Failed.");
                }
                else
                {
                    Assert.Fail("Max Key Missing");
                }

                // 9. Get random keys.
                int[] targets = new int[10];
                for (int i = 0; i < targets.Length; i++)
                {
                    targets[i] = rnd.Next(1, data.Count);
                }

                // 10. Check searches. 
                foreach (int i in targets)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(i, out pair), $"Missing Key {i}");
                }

            }
            File.Delete(myPath);
        }


        /// <summary>   
        /// Bulk Load with High Volume, and Default Capacity (80% fill).
        /// </summary>   
        [TestMethod]
        [DataRow(1024, 65)]
        [DataRow(1024, 255)]
        [DataRow(2048, 500)]
        [DataRow(4096, 1000)]
        public void StressTestCharlie(int pageSize, int count)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            Random rnd = new Random();
            string myPath = TestHelper.GetTempDb();   // Path.ChangeExtension(Path.GetRandomFileName(), "db");
            File.Delete(myPath);

            // 1. Generate sorted data
            var data = Enumerable.Range(1, count)
                                 .Select(i => new Element(i, i))
                                 .ToList();

            // 2. Run Bulk Loader.  Default Capacity (80%).
            var builder = new TreeBuilder(pageSize);
            builder.CreateFromSorted(myPath, data);


            using (var tree = new BTree(myPath))
            {
                // 3. Check Root.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");


                // 4. Run the high-speed single-pass audit
                var report = tree.PerformFullAudit();


                // 5. Fetch keys for the sort/count check
                var keys = tree.GetKeys();

                // 6. Integrity Checks
                Assert.AreEqual(count, keys.Count, "Missing Keys");
                Assert.IsTrue(Util.IsSorted(keys), "Keys must be sorted.");
                Assert.IsFalse(Util.HasDuplicate(keys), "Duplicates");
                Assert.AreEqual(0, report.ZombieCount, "Zombie");
                Assert.AreEqual(0, report.GhostCount, "Ghost");
                Assert.IsTrue(report.Height < 10, "Height must be 10 or less.");
                Assert.IsTrue(tree.GetFreeListCount() < 8, "Free Nodes");
                if (tree.GetNodeCount() > 10)
                {
                    Assert.IsTrue(report.AverageDensity > 25.0, $"Low Density");
                }

                // 7. Check Max.
                Element? lastKey = tree.SelectLast();
                if (lastKey.HasValue)
                {
                    Assert.AreEqual(count, lastKey.Value.Key, "Max Key Search Failed");
                }
                else
                {
                    Assert.Fail("Max Key Missing");
                }

                // 8. Check Min.
                Element? firstKey = tree.SelectFirst();
                if (firstKey.HasValue)
                {
                    Assert.AreEqual(1, firstKey.Value.Key, "Min Key Search Failed");
                }
                else
                {
                    Assert.Fail("Min Key Missing");
                }


                // 9. Get random keys.
                int[] targets = new int[10];
                for (int i = 0; i < targets.Length; i++)
                {
                    targets[i] = rnd.Next(1, data.Count);
                }

                // 10. Check searches.
                foreach (int i in targets)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(i, out pair), $"Missing Key {i}");
                }
            }
            File.Delete(myPath);
        }

    }
}
