using HogFishOne;

namespace UnitTestOne
{
    public class TestHelper
    {
        /// <summary>
        /// Get a random temporary file name.
        /// </summary>
        /// <returns></returns>
        public static string GetTempDb()
        {
            return Path.ChangeExtension(Path.GetRandomFileName(), "db");
        }

        /// <summary>
        /// Check node constraints.
        /// </summary>
        public void CheckNode(BNode node)
        {
            Assert.IsTrue(node.NumKeys >= 0, "NumKeys must be greater than or equal to zero.");
            Assert.IsTrue(node.NumKeys <= node.Order, "NumKeys must be less than or equal to Order.");

            if (!node.Leaf)
            {
                for (int i = 0; i <= node.NumKeys; i++)
                {
                    Assert.IsTrue(node.GetChildId(i) >= 0, "Child pointer must be greater than or equal to zero.");
                }
            }
        }

        // <summary>
        /// Read elements from the a file.
        /// </summary>
        public static List<Element> ReadElementsFromFile(string path, int count)
        {
            List<Element> list = new List<Element>(count);
            foreach (string line in File.ReadLines(path))
            {
                // 2. Stop as soon as we have what we needed.
                if (list.Count >= count) break;

                string[] part = line.Split(',');
                if (part.Length >= 2)
                {
                    int.TryParse(part[0], out int id);
                    int.TryParse(part[1], out int data);
                    if (id > 0 && data > 0)
                        list.Add(new Element(id, data));
                }
            }
            return list;
        }


    }
}
