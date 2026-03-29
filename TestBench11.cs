using HogFishOne;
using System.Diagnostics;
using UnitTestOne;

namespace UnitTestEleven
{

    [TestClass]
    public sealed class UnitEleven
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;


        /// <summary>
        /// Benchmarks the bulk-loading speed and random lookup performance for a given element count.
        /// </summary>

        [TestMethod]
        [DataRow(1000)]
        [DataRow(10000)]
        [DataRow(100000)]
        public void BenchmarkLoadAndSearch(int elementCount)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            string inputFile = TestHelper.GetTempDb();
            File.Delete(inputFile);

            var data = GenerateSortedData(elementCount);
            var randomKeys = data.Select(x => x.Key).OrderBy(x => Guid.NewGuid()).ToArray();

            GC.Collect();
            var sw = Stopwatch.StartNew();

            // 1. Bulk Load
            var builder = new TreeBuilder(pageSize: 4096);
            builder.CreateFromSorted(inputFile, data);

            sw.Stop();
            Console.WriteLine($"Bulk Load: {sw.ElapsedMilliseconds}ms");

            // 2. Random Lookups
            using (var tree = new BTree(inputFile))
            {
                sw.Restart();
                foreach (var k in randomKeys) tree.TrySearch(k, out Element e);
                sw.Stop();
                Console.WriteLine($"Random Search: {sw.ElapsedMilliseconds}ms");

                // 3. Audit.
                tree.PrintAuditReport();
            }
            File.Delete(inputFile);
        }

        /// <summary>
        /// Benchmarks for random insertion and deletion for a given element count.
        /// </summary>

        [TestMethod]
        [DataRow(1000)]
        [DataRow(10000)]
        [DataRow(100000)]
        public void BenchmarkRandomInsertionAndDeletion(int elementCount)
        {
            if (!IsDebugStress)
            {
                Assert.Inconclusive("Skipped");
            }

            // 1. Prepare data outside the timer
            var data = GenerateRandomData(elementCount);
            // Shuffle specifically for the Insertion phase
            var insertOrder = data.OrderBy(x => Guid.NewGuid()).ToList();
            // Shuffle again for the Deletion phase to ensure a different pattern
            var deleteOrder = data.OrderBy(x => Guid.NewGuid()).ToList();

            string inputFile = TestHelper.GetTempDb();
            File.Delete(inputFile);

            using (var tree = new BTree(inputFile, pageSize: 8192))
            {
                // Prep the environment
                GC.Collect();
                GC.WaitForPendingFinalizers();

                // 2. Random Insertion
                var sw = Stopwatch.StartNew();
                foreach (var item in insertOrder)
                {
                    tree.Insert(item.Key, item.Data);
                }
                sw.Stop();
                Console.WriteLine($"Random Insertion: {sw.ElapsedMilliseconds}ms");
                tree.PrintAuditReport();

                // 3. Random Deletion
                sw.Restart();
                foreach (var item in deleteOrder)
                {
                    tree.Delete(item.Key, item.Data);
                }
                sw.Stop();
                Console.WriteLine($"Random Deletion: {sw.ElapsedMilliseconds}ms");
                tree.PrintAuditReport();
            }
            File.Delete(inputFile);
        }

        /// <summary>
        /// Generates a list of unique random elements.
        /// </summary>
        public static List<Element> GenerateRandomData(int count)
        {
            // We use a HashSet to ensure uniqueness before sorting.
            var uniqueKeys = new HashSet<int>();
            var rng = new Random();

            // If count is large, we use a wider range to avoid collisions 
            // and simulate a sparse index.
            int range = count * 10;

            while (uniqueKeys.Count < count)
            {
                uniqueKeys.Add(rng.Next(0, range));
            }

            // Convert to Elements.
            var randomData = uniqueKeys
                .Select(k => new Element(k, rng.Next(0, range)))
                .ToList();

            return randomData;
        }

        /// <summary>
        /// Generates a list of unique Elements sorted by Key.
        /// </summary>
        public static List<Element> GenerateSortedData(int count)
        {
            // We use a HashSet to ensure uniqueness before sorting.
            var uniqueKeys = new HashSet<int>();
            var rng = new Random(); 

            // If count is large, we use a wider range to avoid collisions 
            // and simulate a sparse index.
            int range = count * 10;

            while (uniqueKeys.Count < count)
            {
                uniqueKeys.Add(rng.Next(0, range));
            }

            // Convert to Elements and sort by Key.
            var sortedData = uniqueKeys
                .Select(k => new Element(k, rng.Next(0, range)))
                .OrderBy(e => e.Key)
                .ToList();

            return sortedData;
        }


    }
}
