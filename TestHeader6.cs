using HogFishOne;
using UnitTestOne;

namespace UnitTestSix
{

    [TestClass]
    public sealed class UnitSix
    {

        [TestMethod]
        public void TestRoundTripOne()
        {
            string path = "golf.db";
            File.Delete(path);

            int oldKeyCount;

            // Session 1: Create, Insert, Delete and Add to FreeList.
            int oldOrder = 0;
            using (var t1 = new BTree(path, 256))
            {
                for (int i = 1; i <= 10; i++) t1.Insert(i, i * 100);

                // Deleting items to ensure nodes are moved to FreeList
                t1.Delete(1, 100);
                t1.Delete(2, 200);
                oldKeyCount = t1.CountKeys();
                oldOrder = t1.Header.Order;
            }

            // Session 2: Reopen and verify metadata
            using (var t2 = new BTree(path))
            {
                // Verify Header integrity
                Assert.AreEqual(oldOrder, t2.Header.Order);

                // Verify Data integrity
                Element result;
                Assert.IsFalse(t2.TrySearch(1, out result), "Key 1 should not exist.");
                Assert.IsTrue(t2.TrySearch(10, out result), "Key 10 should exist.");
                Assert.AreEqual(1000, result.Data);

                // Verify Key Count.
                t2.Insert(11, 1100);
                int k = t2.CountKeys();
                Assert.IsTrue(k > oldKeyCount, "Key count should increase after insert.");

                // Zombies
                Assert.AreEqual(0, t2.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }

        [TestMethod]
        public void HeaderAndFreeListRoundTrip()
        {
            string path = "raven.db";
            File.Delete(path);

            int oldOrder = 0;
            int nodeCountBefore;
            int keyCountBefore = 0;

            // Create and free some nodes
            using (var t = new BTree(path, 512))
            {
                for (int i = 1; i <= 20; i++) t.Insert(i, i);
                t.Delete(1, 1);
                t.Delete(2, 2);
                nodeCountBefore = t.Header.NodeCount;
                keyCountBefore = t.CountKeys();
                oldOrder = t.Header.Order;
            }

            // Reopen and ensure header/free list was preserved
            using (var t2 = new BTree(path))
            {
                Assert.AreEqual(oldOrder, t2.Header.Order);
                // Reuse a free slot on insert
                t2.Insert(1000, 1000);
                Assert.IsTrue(t2.Header.NodeCount >= nodeCountBefore, "NodeCount should be greater than or equal to old NodeCount");
                int keyCountAfter = t2.CountKeys();
                Assert.AreEqual(keyCountBefore + 1, keyCountAfter, "Missing Keys");
                Assert.AreEqual(0, t2.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }


        [TestMethod]
        public void SmokeTestOne()
        {
            string path = "rose.db";
            File.Delete(path);
            int oldOrder = 0;

            // Session 1: create, insert, validate in-memory
            using (var t1 = new BTree(path, 256))
            {
                t1.Insert(10, 100);
                t1.Insert(20, 200);
                t1.Insert(30, 300);

                Element e;
                Assert.IsTrue(t1.TrySearch(20, out e), "Inserted key 20 must be found");
                Assert.AreEqual(200, e.Data);

                int count = t1.CountKeys();
                Assert.AreEqual(3, count, "Missing Keys");

                // Delete one key to exercise deletion path
                t1.Delete(20, 200);
                Assert.IsFalse(t1.TrySearch(20, out e), "Deleted key 20 must not be found");

                t1.ValidateIntegrity();
                oldOrder = t1.Header.Order;
            }

            // Session 2: reopen and verify persistence
            using (var t2 = new BTree(path))
            {
                Assert.AreEqual(oldOrder, t2.Header.Order, "Order must match.");

                Element e;
                Assert.IsTrue(t2.TrySearch(10, out e), "Key 10 must persist after reopen");
                Assert.AreEqual(100, e.Data);

                Assert.IsTrue(t2.TrySearch(30, out e), "Key 30 must persist after reopen");
                Assert.AreEqual(300, e.Data);

                int count = t2.CountKeys();
                Assert.AreEqual(2, count, "Keys missing.");

                t2.ValidateIntegrity();
                Assert.AreEqual(0, t2.CountZombies(), "Zombies");
            }

            File.Delete(path);
        }

    }
}
