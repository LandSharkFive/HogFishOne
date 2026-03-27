namespace HogFishOne
{
    /// <summary>
    /// Manages the low-level disk I/O for a B+ Tree structure, 
    /// handling node serialization, header management, and file stream lifecycle.
    /// </summary>
    public class TreeManager : IDisposable
    {
        public BTreeHeader Header;

        private FileStream MyFileStream; // Keep open during the build process
        private BinaryWriter MyWriter;
        private int Order = 0;
        private int PageSize;
        private const int HeaderSize = 4096;  // Reserved space at top of the file.
        private const int MagicConstant = BTreeHeader.MagicConstant;

        private const int MINPAGESIZE = BTree.MINPAGESIZE;
        private const int MINORDER = BTree.MINORDER;


        /// <summary>
        /// Initializes a new B+ Tree file. Sets up the initial header and validates page sizes.
        /// </summary>
        public TreeManager(string path, int pageSize, int order)
        {
            // 1. Save order.
            PageSize = pageSize;
            Order = order;

            // 2. Validate input parameters.
            if (string.IsNullOrEmpty(path))
                throw new ArgumentException("FileName cannot be empty.");

            if (PageSize < MINPAGESIZE)
               throw new ArgumentException($"PageSize must be at least {MINPAGESIZE} bytes." );

            if (Order < MINORDER)
                throw new ArgumentException($"Order must be at least {MINORDER}.");

            // 3. Open File Stream for persistent storage.
            MyFileStream = new FileStream(path, FileMode.Create, FileAccess.ReadWrite);
            MyWriter = new BinaryWriter(MyFileStream);

            // 4. Initialize the B+ Tree Header with default metadata.
            Header = new BTreeHeader();
            Header.Initialize(PageSize);
            SaveHeader();

            // 5. Structural Validation: Ensure the calculated PageSize can actually hold the node data.
            int metadataSize = 12; // Leaf (4), NumKeys (4), Id (4)
            int keysSize = Order * 8; // Each Element is Key(4) + Data(4)
            int childrenSize = (Order + 1) * 4; // Int pointers
            int required = metadataSize + keysSize + childrenSize;
            if (Header.PageSize < required)
            {
                throw new ArgumentException(nameof(Header.PageSize));
            }
        }

        /// <summary>
        /// Explicitly closes the file stream and releases resources.
        /// </summary>
        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Safely persists data and releases the file stream while preventing redundant cleanup.
        /// </summary>
        public void Dispose()
        {
            // Dispose the writer first, then the stream
            MyWriter?.Dispose();
            MyFileStream?.Dispose();
            GC.SuppressFinalize(this);
        }


        /// <summary>
        /// Persists a single BNode to the physical storage medium.
        /// This method translates the logical Node ID into a physical file offset 
        /// and performs a synchronous write operation. Because the B+ Tree uses 
        /// fixed-width pages, this operation is highly efficient (O(1) seek time).
        /// </summary>
        public void SaveToDisk(BNode node)
        {
            if (node.Id < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(node.Id), "Cannot be negative");
            }

            long offset = CalculateOffset(node.Id);
            MyFileStream.Seek(offset, SeekOrigin.Begin);
            MyFileStream.Write(node.GetBuffer(), 0, node.PageSize);
        }

        /// <summary>
        /// Calculates the exact byte offset in the file for a given Node ID.
        /// </summary>
        private long CalculateOffset(int disk)
        {
            if (disk < 0)
                throw new ArgumentOutOfRangeException(nameof(disk), "Cannot be negative");

            if (Header.PageSize < MINPAGESIZE)
                throw new ArgumentException($"Page Size must be at least {MINPAGESIZE} bytes.");

            return ((long)Header.PageSize * disk) + HeaderSize;
        }

        /// <summary>
        /// Updates the file's header (first 4096 bytes) with current tree metadata.
        /// </summary>
        public void SaveHeader()
        {
            MyFileStream.Seek(0, SeekOrigin.Begin);
            Header.Write(MyWriter);
        }


        /// <summary>
        /// Factory method that instantiates a new BNode and assigns it a unique 
        /// persistent identifier by incrementing the global NodeCount. 
        /// This ensures every node has a designated "home" on the disk 
        /// before any data is even written to it.
        /// </summary>
        public BNode CreateNode(bool isLeaf)
        {
            BNode node = new BNode(Header.Order, Header.PageSize, isLeaf, Header.NodeCount);
            Header.NodeCount++;
            return node;
        }
    }
}