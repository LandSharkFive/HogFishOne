using HogFishOne;
using UnitTestOne;

namespace UnitTestTen
{

    [TestClass]
    public sealed class UnitTen
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

        [TestMethod]
        public void GeneratePrimesTest()
        {
            // 1. Does file exist?
            string path = "prime.txt";
            if (File.Exists(path)) return;

            // 2. Setup.
            const int MaxPrime = 104729;
            const int MinFileSize = 127800;

            // 3. Generate primes.
            Util.WritePrimesToFile(path, MaxPrime);

            // 4. Assert,
            Assert.IsTrue(File.Exists(path), "File should exist.");

            // 5. Check the file length.
            long fileLength = new FileInfo(path).Length;
            Assert.IsTrue(fileLength > MinFileSize, $"File Length must be greater than {MinFileSize} bytes.");

            // 6. Check the prime numbers (first 10 primes).
            string[] lines = File.ReadLines(path).Take(10).ToArray();
            string[] expectedPrimes = {
                "1, 2", "2, 3", "3, 5", "4, 7", "5, 11",
                "6, 13", "7, 17", "8, 19", "9, 23", "10, 29"
            };
            CollectionAssert.AreEqual(expectedPrimes, lines, "The primes should match.");
        }


        [TestMethod]
        [DataRow(1024, 1000)]
        [DataRow(2048, 5000)]
        [DataRow(4096, 10000)]
        public void StressTestEcho(int pageSize, int count)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            Random rnd = new Random();
            string myPath = TestHelper.GetTempDb();
            File.Delete(myPath);

            // 1. Generate sorted data
            string primePath = "prime.txt";
            Assert.IsTrue(File.Exists(primePath), "Prime file must exist.");
            var data = TestHelper.ReadElementsFromFile(primePath, count); 

            // 2. Run Bulk Loader.  Default Capacity (80%).
            var builder = new TreeBuilder(pageSize);
            builder.CreateFromSorted(myPath, data);

            using (var tree = new BTree(myPath, pageSize))
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
                    Assert.IsTrue(report.AverageDensity > 25.0, $"Low Density");

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


                // 10. Search every key and check data.
                foreach (var item in data)
                {
                    Element pair;
                    Assert.IsTrue(tree.TrySearch(item.Key, out pair), $"Missing Key {item.Key}");
                    Assert.AreEqual(item.Data, pair.Data, $"Data Mismatch for Key {item.Key}");
                }
            }
            File.Delete(myPath);
        }


    }
}
