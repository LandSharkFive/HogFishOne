namespace HogFishOne
{
    internal class Program
    {
        /// <summary>
        /// The application entry point and interactive test harness for the B+ Tree engine.
        /// </summary>
        /// <remarks>
        /// Provides a console-driven interface to execute specific unit tests, 
        /// structural verifications, and high-volume stress tests.
        /// </remarks>
        public static void Main(string[] args)
        {
            Console.WriteLine("Select a test: ");
            int choice = 0;
            Int32.TryParse(Console.ReadLine(), out choice);
            switch (choice)
            {
                case 1:
                    ShowMenu();
                    break;
                case 2:
                    TestBPlusTree();
                    break;
                case 3:
                    TestSequential();
                    break;
                case 4:
                    RunRandomInsertionTest();
                    break;
                case 10:
                    RunSanityCheck();
                    break;
            }
        }

        /// <summary>
        /// Show the menu.
        /// </summary>
        public static void ShowMenu()
        {
            Console.WriteLine("1. Show Menu");
            Console.WriteLine("2. Basic B+ Tree Test");
            Console.WriteLine("3. Sequential Insertion Test");
            Console.WriteLine("4. Random Insertion Test");
            Console.WriteLine("10. Run All Sanity Checks");
        }


        /// <summary>
        /// Provides an end-to-end demonstration of the B+ Tree engine.
        /// This test serves as a "Hello World" for developers, demonstrating 
        /// the lifecycle of a database file: from physical initialization and 
        /// structural splitting during insertions, to high-speed searching and 
        /// horizontal traversal of the leaf-node chain.
        /// </summary>
        public static void TestBPlusTree()
        {
            Console.WriteLine("--- Initializing B+ Tree ---");
            string outFile = "data.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                // Insert keys in an order that forces different types of splits
                int[] keysToInsert = { 10, 20, 30, 40, 50, 25 };

                foreach (int k in keysToInsert)
                {
                    Console.WriteLine($"\nInserting {k}...");
                    tree.Insert(new Element { Key = k, Data = k });
                    tree.PrintTreeSimple(tree.Header.RootId);
                    var rootNode = tree.DiskRead(tree.Header.RootId);
                }

                // 1. Count Keys.
                int keyCount = tree.CountKeys();
                Console.WriteLine($"Key Count: {keyCount}");

                // 2. Print the keys.
                var list = tree.GetKeys();
                Console.Write("Keys: ");
                Util.PrintList(list);
                Console.WriteLine();

                // 3. Test Search.
                Console.WriteLine("\n--- Testing TrySearch ---");
                int searchKey = 30;
                if (tree.TrySearch(searchKey, out Element found))
                    Console.WriteLine($"Success: Found {searchKey} with value: {found.Data}");
                else
                    Console.WriteLine($"Failure: Could not find {searchKey}");

                // 3. Print the Leaf Chain.
                tree.PrintLeafChain();
            }
            File.Delete(outFile);
        }


        /// <summary>
        /// Stress-tests the tree with sequential insertions to verify logarithmic growth
        /// and edge-case balancing. This audit ensures that even with perfectly ordered
        /// input, the engine maintains structural integrity and search efficiency.
        /// </summary>
        public static void TestSequential()
        {
            Console.WriteLine("--- Starting 100 Item Sequential Test ---");
            string outFile = "data.db";
            File.Delete(outFile);

            using (var tree = new BTree(outFile, 256))
            {
                int count = 100;
                for (int i = 1; i <= count; i++)
                {
                    tree.Insert(new Element(i, i));
                }

                Console.WriteLine("Insertion Complete.");

                // 1. Count Keys
                int keyCount = tree.CountKeys();
                Console.WriteLine($"Key Count: {keyCount}");

                var list = tree.GetKeys();
                Console.Write("Keys: ");
                Util.PrintList(list);
                Console.WriteLine();

                // 2. Verify Tree Height (Logarithmic)
                // For Order 4, 100 items should be about 4-5 levels deep.
                tree.PrintTreeSimple(tree.Header.RootId);
                tree.PrintPointers();
                tree.DumpFile();

                // 4. Spot Check Search
                if (tree.TrySearch(50, out Element result) && result.Key == 50)
                    Console.WriteLine("Search Check: OK (Found 50)");
                else
                    Console.WriteLine("Search Check: FAIL");

                // 5. Full Audit.
                tree.PrintAuditReport();

                // 6. Integrity Check.
                tree.ValidateIntegrity();

            }
            File.Delete(outFile);
        }

        /// <summary>
        /// Executes a non-deterministic stress test by inserting unique, random keys. 
        /// This validates the robustness of the tree's internal rebalancing and 
        /// pointer-management logic under unpredictable branching conditions, 
        /// ensuring the engine remains stable regardless of input order.
        /// </summary>
        public static void RunRandomInsertionTest()
        {
            Console.WriteLine($"--- Starting Random Insertion Test ---");
            string outFile = "data.db";
            File.Delete(outFile);

            Random rnd = new Random();
            HashSet<int> insertedKeys = new HashSet<int>();

            using (var tree = new BTree(outFile, 256))
            {
                int count = 100;
                while (insertedKeys.Count < count)
                {
                    int nextKey = rnd.Next(1, 10000);
                    if (insertedKeys.Add(nextKey))
                    {
                        tree.Insert(nextKey, nextKey);
                    }
                }

                Console.WriteLine("Insertion Complete.");

                // 1. Count Keys
                int keyCount = tree.CountKeys();
                Console.WriteLine($"Key Count: {keyCount}");

                var list = tree.GetKeys();
                Console.Write($"Keys: ");
                Util.PrintList(list);
                Console.WriteLine();

                // 2. Display the Tree (to see those Level 0-6 splits)
                tree.PrintTreeSimple(tree.Header.RootId);
                tree.PrintPointers();
                tree.DumpFile();

                // 4. Full Audit.
                tree.PrintAuditReport();

                // 5. Integrity Check.
                tree.ValidateIntegrity();
            }
            File.Delete(outFile); // You've already mastered this!
        }

        /// <summary>
        /// Runs the full gauntlet of structured, sequential, and chaotic tests. 
        /// If the console is still green after this, the physics 
        /// of your tree are officially more stable than the local economy.
        /// </summary>
        public static void RunSanityCheck()
        {
            TestBPlusTree();
            TestSequential();
            RunRandomInsertionTest();
        }


    }
}
