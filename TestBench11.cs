using HogFishOne;
using System.Diagnostics;
using UnitTestOne;

namespace UnitTestEleven
{

    [TestClass]
    public sealed class UnitEleven
    {
        private bool IsDebugStress = TestSettings.CanDebugStress;

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
