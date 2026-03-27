using HogFishOne;
using UnitTestOne;

namespace UnitTestEight
{
    [TestClass]
    public sealed class UnitEight
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

        /// <summary>
        /// An insertion test for a small number of items.  Ten items or less.
        /// Test seiches. Test one deletion.  Test one min and one max.
        /// Ten items or less.  This is a general purpose sanity check for the code.
        /// </summary>
        [TestMethod]
        public void SimpleInsertEight()
        {
            string outFileName = "rain.db";
            File.Delete(outFileName);

            // 1. Create the B+ Tree 
            // This will create or overwrite the file.
            using (var tree = new BTree(outFileName, 256))
            {

                // 2. Insert elements
                tree.Insert(10, 100);
                tree.Insert(20, 200);
                tree.Insert(30, 300); // Node 0: [10, 20, 30] (Full)

                // Inserting 40 will cause a split: 20 promoted to a new root.
                tree.Insert(40, 400);

                // A B+ Tree of with keys 10, 20, 30, 40 now looks like:
                // Root (Disk 0): [20]
                // Left Child (Disk 1): [10]
                // Right Child (Disk 2): [30, 40]

                tree.Insert(50, 500);
                tree.Insert(60, 600);
                tree.Insert(70, 700);
                tree.Insert(80, 800);

                // 3. Search for an element
                int searchKey = 50;
                Element item;
                Assert.IsTrue(tree.TrySearch(searchKey, out item));

                // 3a. Sanity Checks
                List<int> a = tree.GetKeys();
                Assert.IsTrue(a.Count > 0);
                Assert.IsTrue(Util.IsSorted(a));
                Assert.IsFalse(Util.HasDuplicate(a));

                // 4. Delete an element
                tree.Delete(10, 100);
                searchKey = 10;
                Assert.IsFalse(tree.TrySearch(searchKey, out item));


                // 5. Find Min/Max
                Element? max = tree.SelectLast();
                Assert.IsTrue(max.HasValue);
                if (max.HasValue)
                {
                    Assert.AreEqual(max.Value.Key, 80);
                    Assert.AreEqual(max.Value.Data, 800);
                }

                Element? min = tree.SelectFirst();
                Assert.IsTrue(min.HasValue);
                if (min.HasValue)
                {
                    Assert.AreEqual(min.Value.Key, 20);
                    Assert.AreEqual(min.Value.Data, 200);
                }

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(outFileName);
        }

        /// <summary>
        /// A sequential insertion test for testing split nodes.
        /// </summary>
        [TestMethod]
        public void MediumInsertFifty()
        {
            string outFileName = "bagel.db";
            File.Delete(outFileName);

            // Small order forces lots of splits
            using (var tree = new BTree(outFileName, 256))
            {
                // Sequential Insertion 
                for (int i = 1; i <= 50; i++)
                {
                    // Using i*10 as data just to distinguish Key from Data
                    tree.Insert(i, i * 10);
                }

                Assert.IsTrue(tree.Header.RootId >= 0, "RootId lost");

                // Verification 
                for (int i = 1; i <= 50; i++)
                {
                    Element item;
                    Assert.IsTrue(tree.TrySearch(i, out item), $"Missing Key {i}");
                }

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(outFileName);
        }


        /// <summary>
        /// A sequential insertion test for testing split nodes.
        /// </summary>
        [TestMethod]
        public void MediumInsertHundred()
        {
            string outFileName = "red.db";
            File.Delete(outFileName);

            // Small order forces lots of splits
            using (var tree = new BTree(outFileName, 256))
            {
                // Sequential Insertion 
                for (int i = 1; i <= 100; i++)
                {
                    // Using i*10 as data just to distinguish Key from Data
                    tree.Insert(i, i * 10);
                }

                Assert.IsTrue(tree.Header.RootId >= 0, "RootId lost");

                // Verification 
                for (int i = 1; i <= 100; i++)
                {
                    Element item;
                    Assert.IsTrue(tree.TrySearch(i, out item), $"Missing Key {i}");
                }

                // Zombies
                Assert.AreEqual(0, tree.CountZombies(), "Zombies");
            }

            File.Delete(outFileName);
        }

        [TestMethod]
        public void TestShuffleInsertion()
        {
            // 1. Setup
            string testPath = "cardinal.db";
            File.Delete(testPath);

            // 2. Create test data.
            List<int> data = Enumerable.Range(1, 200).ToList();

            // 3. Shuffle the data.
            Util.Shuffle(data);

            using (var tree = new BTree(testPath, 512))
            {
                // 4. Insert in random order
                foreach (int key in data)
                {
                    tree.Insert(key, key);
                }

                // 5. Commit changes to disk.
                tree.Commit();

                // 6. Check root exists.
                Assert.IsTrue(tree.Header.RootId != -1, "RootId lost");

                // 7. Check root exists.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 8. Verify all keys exist.
                foreach (int i in data)
                {
                    Element item;
                    Assert.IsTrue(tree.TrySearch(i, out item), $"Missing Key {i}");
                }

                // 9. Verify Key Counts.
                int count = tree.CountKeys();
                Assert.AreEqual(data.Count, count);

                // 10. Check for Zombies.
                int zombieCount = tree.CountZombies();
                Assert.AreEqual(0, zombieCount, "Zombies");

                // 11. Check for Ghosts.
                int ghostCount = tree.CountGhost();
                Assert.AreEqual(0, ghostCount, "Ghosts");
            }

            File.Delete(testPath);
        }

        [TestMethod]
        [DataRow(1024, 500)]
        [DataRow(2048, 500)]
        [DataRow(4096, 500)]
        [DataRow(2048, 1024)]
        [DataRow(1024, 500)]

        public void StressTestBravo(int pageSize, int count)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            // 1. Setup
            string testPath = TestHelper.GetTempDb();  
            File.Delete(testPath);
            List<int> data = Enumerable.Range(1, count).ToList();

            // 2. Shuffle the data.
            Util.Shuffle(data);

            File.Delete(testPath);
            using (var tree = new BTree(testPath, pageSize))
            {
                // 3. Insert in random order
                foreach (int key in data)
                {
                    tree.Insert(key, key);
                }

                // 4. Commit changes to disk.
                tree.Commit();

                // 5. Check root exists.
                Assert.IsTrue(tree.Header.RootId != -1, "Root");

                // 6. Verify all keys exist.
                foreach (int i in data)
                {
                    Element item;
                    Assert.IsTrue(tree.TrySearch(i, out item), $"Missing Key {i}");
                }

                // 7. Verify Key Counts.
                int keyCount = tree.CountKeys();
                Assert.AreEqual(data.Count, keyCount);

                // 8. Check for Zombies.
                int zombieCount = tree.CountZombies();
                Assert.AreEqual(0, zombieCount, "Zombies");

                // 9. Check for Ghosts.
                int ghostCount = tree.CountGhost();
                Assert.AreEqual(0, ghostCount, "Ghosts");
            }

            File.Delete(testPath);
        }


    }
}
