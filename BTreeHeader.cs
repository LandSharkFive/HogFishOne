
using System.Buffers.Binary;

namespace HogFishOne
{
    /// <summary>
    /// Represents the persistent metadata header stored at the beginning of the B+ Tree file (Page 0).
    /// This structure maintains the critical state required to reinitialize the tree 
    /// between sessions, including structural parameters and storage offsets.
    /// </summary>
    public struct BTreeHeader
    {
        public const int MagicConstant = 0x42542145;

        /// <summary>
        /// Represents the persistent metadata header stored at the beginning of the B+ Tree file.
        /// </summary>
        /// 
        public int Magic;           // Unique constant to verify file format integrity
        public int Order;           // Maximum branching factor of the tree
        public int RootId;          // ID of the current root node (-1 if empty)
        public int PageSize;        // Physical size of a single node on disk in bytes
        public int NodeCount;       // Total number of allocated node slots in the file
        public int FreeListCount;   // Number of deleted nodes currently available for reuse
        public long FreeListOffset; // Byte position where the FreeList data begins


        /// <summary>
        /// Orchestrates the initial formatting of the B+ Tree metadata header. 
        /// This method defines the structural invariants for the lifetime of the file, 
        /// including the 'Magic Number' for file-type validation and the branching 
        /// 'Order' which dictates the physical size of every subsequent node.
        /// </summary>
        public void Initialize(int pageSize)
        {
            Magic = MagicConstant;
            PageSize = pageSize;
            Order = BNode.CalculateMaxOrder(pageSize);
            RootId = -1;
            NodeCount = 0;
            FreeListCount = 0;
            FreeListOffset = 0;
        }

        /// <summary> Serializes the header fields to the current file stream. </summary>
        public void Write(BinaryWriter writer)
        {
            Span<byte> buffer = stackalloc byte[32];

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(0, 4), Magic);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(4, 4), Order);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(8, 4), RootId);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(12, 4), PageSize);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(16, 4), NodeCount);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(20, 4), FreeListCount);
            BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(24, 8), (long)FreeListOffset);

            // One single trip to the stream/disk
            writer.Write(buffer);
        }


        /// <summary> Deserializes the header fields from the current file stream. </summary>
        public static BTreeHeader Read(BinaryReader reader)
        {
            Span<byte> buffer = stackalloc byte[32];
            reader.Read(buffer);

            int magic = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, 4));
            if (magic != MagicConstant)
            {
                throw new InvalidDataException("Invalid File Format");
            }

            return new BTreeHeader
            {
                Magic = magic,
                Order = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(4, 4)),
                RootId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(8, 4)),
                PageSize = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(12, 4)),
                NodeCount = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(16, 4)),
                FreeListCount = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(20, 4)),
                FreeListOffset = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(24, 8))
            };
        }

    }
}
