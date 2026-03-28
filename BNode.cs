using System;
using System.Buffers.Binary;
using System.Diagnostics;

namespace HogFishOne
{
    /// <summary>
    /// BNode: A Slotted-Page B+ Tree Node implementation.
    /// 
    /// PHYSICAL LAYOUT:
    /// [Header] [Kid Table (Internal Only)] [Data Heap]
    /// 
    /// The 'Data Heap' grows forward from the top.
    /// </summary>

    public class BNode
    {
        // ##########################################
        // # 1. CONSTANTS & LAYOUT
        // ##########################################

        private readonly byte[] Buffer;
        public byte[] GetBuffer() => Buffer;

        private const int OFF_LEAF = 0;  // 4 byte
        private const int OFF_PAGE_ID = 4;   // 4 bytes. 
        private const int OFF_NUM_KEYS = 8;  // 4 bytes
        private const int OFF_NEXT_LEAF = 12; // 4 bytes
        private const int HEADER_SIZE = 16;

        private const int KID_SIZE = 4;    // int PageId
        private const int RECORD_SIZE = 8; // int Key + int Data.

        public int Order { get; set; }
        public int PageSize { get; set; }

        // ##########################################
        // # 2. CONSTRUCTORS & INITIALIZATION
        // ##########################################

        /// <summary> Creates a fresh, empty node initialized with a header. </summary>
        public BNode(int order, int pageSize, bool isLeaf, int id)
        {
            Order = order;
            PageSize = pageSize;
            Buffer = new byte[pageSize];
            Id = id;
            Leaf = isLeaf;
            NumKeys = 0;
            NextLeafId = -1;
        }

        /// <summary> Wraps an existing byte array (loaded from disk) into a BNode object. </summary>
        public BNode(int order, int pageSize, byte[] existingBuffer)
        {
            Order = order;
            PageSize = pageSize;
            Buffer = existingBuffer;

            // 1. READ THE LEAF.
            this.Leaf = BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_LEAF, 4)) == 1;

            // 1. READ THE ID 
            this.Id = BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_PAGE_ID, 4));

            // 2. READ THE COUNTS 
            this.NumKeys = BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_NUM_KEYS, 4));

            // 3. READ THE NEXT LEAF ID 
            this.NextLeafId = BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_NEXT_LEAF, 4));
        }

        /// <summary> Calculates capacity based on PageSize, accounting for header and kid table overhead. </summary>
        public static int CalculateMaxOrder(int pageSize)
        {
            int fixedOverhead = HEADER_SIZE + KID_SIZE;
            int usableSpace = pageSize - fixedOverhead;
            int perEntryCost = RECORD_SIZE + KID_SIZE;
            return (usableSpace / perEntryCost) - 2;
        }

        // ##########################################
        // # 3. PROPERTIES (Header Access)
        // ##########################################

        public int Id
        {
            get => BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_PAGE_ID, 4));
            set => BinaryPrimitives.WriteInt32LittleEndian(Buffer.AsSpan(OFF_PAGE_ID, 4), value);
        }

        public int NumKeys
        {
            get => BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_NUM_KEYS, 4));
            set => BinaryPrimitives.WriteInt32LittleEndian(Buffer.AsSpan(OFF_NUM_KEYS, 4), value);
        }

        public bool Leaf
        {
            get => BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_LEAF, 4)) == 1;
            set => BinaryPrimitives.WriteInt32LittleEndian(Buffer.AsSpan(OFF_LEAF, 4), value ? 1 : 0 );

        }

        public int NextLeafId
        {
            get => BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(OFF_NEXT_LEAF, 4));
            set => BinaryPrimitives.WriteInt32LittleEndian(Buffer.AsSpan(OFF_NEXT_LEAF, 4), value);
        }


        // ##########################################
        // # 4. ADDRESS ENGINE
        // ##########################################

        /// <summary> Child pointers follow the header in Internal nodes. </summary>
        private int KidTableStart => HEADER_SIZE;

        /// <summary> The physical byte where data records (Keys/Values) are allowed to begin. </summary>
        private int DataAreaStart => KidTableStart + ((Order + 1) * KID_SIZE);


        // ##########################################
        // # 6. DATA & CHILD ACCESS (The Cargo)
        // ##########################################

        /// Calculates the absolute byte offset of a record within the physical page buffer. </summary>
        public int GetDataOffset(int index)
        {
            return DataAreaStart + (index * RECORD_SIZE);
        }

        /// <summary>
        /// Retrieves a 32-bit integer key from the specified logical index.
        /// Uses high-performance BinaryPrimitives to 'peek' into the raw byte buffer.
        /// </summary>
        public int GetKey(int index)
        {
            if (index < 0 || index >= NumKeys) throw new IndexOutOfRangeException();
            int offset = GetDataOffset(index);
            return BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(offset, 4));
        }

        /// <summary> 
        /// Retrieves the 4-byte payload (value) associated with the key at the given index. 
        /// In the unslotted layout, the data immediately follows the 4-byte key.
        /// </summary>
        public int GetData(int index)
        {
            int offset = GetDataOffset(index);
            return BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(offset + 4, 4));
        }


        /// <summary> 
        /// Performs a raw physical write of a Key/Data pair at a fixed index.
        /// Note: This is a low-level buffer operation and does not update NumKeys.
        /// </summary>
        public void SetElementFixed(int index, int key, int data)
        {
            int dataOffset = GetDataOffset(index);
            Span<byte> dataArea = Buffer.AsSpan(dataOffset, RECORD_SIZE);
            BinaryPrimitives.WriteInt32LittleEndian(dataArea.Slice(0, 4), key);
            BinaryPrimitives.WriteInt32LittleEndian(dataArea.Slice(4, 4), data);
        }

        /// <summary> Retrieves the 4-byte PageId from the Kid Table for internal nodes, or returns -1 if called on a leaf. </summary>
        public int GetChildId(int index)
        {
            if (index < 0 || index > NumKeys)
                throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is invalid for Node {Id}.");
            if (Leaf) return -1;
            int address = KidTableStart + (index * KID_SIZE);
            return BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(address, 4));
        }

        /// <summary> Writes a 4-byte PageId into the Kid Table at the specified index, mapping the branch to a child node. </summary>
        public void SetChildId(int index, int childPageId)
        {
            int address = KidTableStart + (index * KID_SIZE);
            BinaryPrimitives.WriteInt32LittleEndian(Buffer.AsSpan(address, 4), childPageId);
        }

        /// <summary> 
        /// Physically shifts the tail of the child pointer table to the right by one slot.
        /// Used during an internal node insertion to open a 'hole' for a new child ID.
        /// </summary>
        public void ShiftChildrenRight(int index)
        {
            int childrenToShift = (NumKeys + 1) - index;
            if (childrenToShift <= 0) return;

            int sourceOffset = KidTableStart + (index * KID_SIZE);
            int destOffset = KidTableStart + ((index + 1) * KID_SIZE);

            Debug.Assert(destOffset >= HEADER_SIZE && destOffset + (childrenToShift * KID_SIZE) <= DataAreaStart);

            System.Buffer.BlockCopy(Buffer, sourceOffset, Buffer, destOffset, childrenToShift * KID_SIZE);
        }

        /// <summary> 
        /// Shifts a range of child pointers one slot to the LEFT to close a gap.
        /// Used during internal node deletions or rebalancing. 
        /// </summary>
        public void ShiftChildrenLeft(int index)
        {
            // Example: index is 1. We want to move children 1, 2, 3 into slots 0, 1, 2.
            int childrenToShift = (NumKeys + 1) - index;
            if (childrenToShift <= 0) return;

            int sourceOffset = KidTableStart + (index * KID_SIZE);
            int destOffset = KidTableStart + ((index - 1) * KID_SIZE);

            System.Buffer.BlockCopy(Buffer, sourceOffset, Buffer, destOffset, childrenToShift * KID_SIZE);

            // Clear the last pointer
            SetChildId(NumKeys, -1);
        }

        /// <summary> 
        /// Performs a high-speed bulk move of Key/Data records within the node buffer.
        /// This is a generic internal shift used for both inserting (Right) and deleting (Left).
        /// </summary>
        public void ShiftRecords(int sourceIndex, int destIndex, int count)
        {
            if (count <= 0 || sourceIndex == destIndex) return;

            // ONLY shift the 8-byte DATA records here. 
            // Do NOT shift children here; handle that in ShiftChildrenLeft/Right.
            int dataSource = DataAreaStart + (sourceIndex * RECORD_SIZE);
            int dataDest = DataAreaStart + (destIndex * RECORD_SIZE);

            Debug.Assert(dataDest >= DataAreaStart && (dataDest + (count * RECORD_SIZE)) <= PageSize);
            System.Buffer.BlockCopy(Buffer, dataSource, Buffer, dataDest, count * RECORD_SIZE);
        }


        // ##########################################
        // # 7. SEARCH & NAVIGATION
        // ##########################################

        /// <summary> 
        /// Performs a binary search through the contiguous key array to locate a specific key 
        /// or determine its potential insertion point.
        /// </summary>
        public int FindIndex(int targetKey)
        {
            int low = 0;
            int high = NumKeys - 1;
            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);
                int midKey = GetKey(mid);
                if (midKey == targetKey) return mid;
                if (midKey < targetKey) low = mid + 1;
                else high = mid - 1;
            }
            return ~low;
        }

        /// <summary> Converts the result of a binary search into a positive index where a new key should be physically placed to maintain sort order. </summary>
        public int FindInsertionPoint(int targetKey)
        {
            int index = FindIndex(targetKey);
            return index >= 0 ? index : ~index;
        }

        // ##########################################
        // # 8. MUTATION (The Heavy Lifters)
        // ##########################################

        /// <summary> 
        /// Performs a physical insertion of a key-data pair and its associated child pointer.
        /// This method handles the low-level memory shifting (BlockCopy) required to 
        /// maintain sorted order in the unslotted, fixed-record layout.
        /// </summary>
        public void InsertAt(int pos, int key, int data, int childPageId = -1)
        {
            // GUARD: With the new CalculateMaxOrder, this is a strict physical limit.
            // If NumKeys == Order, the very last byte of the page is already in use.
            if (NumKeys >= Order)
                throw new InvalidOperationException($"Node {Id} is at maximum capacity ({Order} keys).");

            int countToMove = NumKeys - pos;

            // 1. Physical Memory Shift (Records)
            // We shift the 8-byte Key-Value pairs to the right to make room at 'pos'.
            if (countToMove > 0)
            {
                int srcOffset = GetDataOffset(pos);
                int destOffset = GetDataOffset(pos + 1);
                System.Buffer.BlockCopy(Buffer, srcOffset, Buffer, destOffset, countToMove * RECORD_SIZE);
            }

            // 2. Physical Memory Shift (Children)
            // For internal nodes, we maintain the N+1 relationship.
            // Inserting a key at 'pos' requires inserting its right-hand child at 'pos + 1'.
            if (!this.Leaf)
            {
                this.ShiftChildrenRight(pos + 1);
                this.SetChildId(pos + 1, childPageId);
            }

            // 3. Write the Data Record (Direct Addressing)
            int dataOffset = GetDataOffset(pos);
            Span<byte> dataArea = Buffer.AsSpan(dataOffset, RECORD_SIZE);
            BinaryPrimitives.WriteInt32LittleEndian(dataArea.Slice(0, 4), key);
            BinaryPrimitives.WriteInt32LittleEndian(dataArea.Slice(4, 4), data);

            // 4. Update Header
            this.NumKeys++;
        }

        /// <summary> Deletes a record and its associated child branch by shifting the Kid Table pointers to close the gap. </summary>
        public void RemoveKeyAndChildAt(int pos)
        {
            if (pos < 0 || pos >= NumKeys) throw new IndexOutOfRangeException();

            int moveCount = NumKeys - 1 - pos;

            // 1. Shift Records Left
            if (moveCount > 0)
            {
                int dataSrc = GetDataOffset(pos + 1);
                int dataDest = GetDataOffset(pos);
                // Use Span for a more modern approach than BlockCopy
                Buffer.AsSpan(dataSrc, moveCount * RECORD_SIZE)
                      .CopyTo(Buffer.AsSpan(dataDest));
            }

            // 2. Shift Children Left (Internal Nodes only)
            if (!Leaf)
            {
                // If we remove Key at 'pos', we remove the right-hand Child at 'pos + 1'
                int childrenToShift = NumKeys - (pos + 1);
                if (childrenToShift > 0)
                {
                    int kidSrc = KidTableStart + ((pos + 2) * KID_SIZE);
                    int kidDest = KidTableStart + ((pos + 1) * KID_SIZE);
                    Buffer.AsSpan(kidSrc, childrenToShift * KID_SIZE)
                          .CopyTo(Buffer.AsSpan(kidDest));
                }

                // HYGIENE: Wipe the now-redundant last child pointer
                SetChildId(NumKeys, -1);
            }

            // 3. PHYSICAL CLEANUP: Zero out the trailing record slot
            int lastRecordOffset = GetDataOffset(NumKeys - 1);
            Buffer.AsSpan(lastRecordOffset, RECORD_SIZE).Fill(0);

            // 4. Update Header
            this.NumKeys--;
        }


        /// <summary>
        /// Splits the current node into two, partitioning the keys and pointers.
        /// LEAF SPLIT: The separator key is COPIED to the parent; the original remains in the leaf sibling.
        /// INTERNAL SPLIT: The separator key is PROMOTED to the parent and removed from the children 
        /// to maintain the (N keys, N+1 children) branching balance.
        /// </summary>
        public (BNode Sibling, int SeparatorKey) Split(int newId)
        {
            BNode sibling = new BNode(this.Order, this.PageSize, this.Leaf, newId);
            int midIndex = this.NumKeys / 2;
            int separatorKey = this.GetKey(midIndex);

            if (Leaf)
            {
                // LEAF SPLIT: Copy separator to parent, but keep it in the leaf sibling
                int keysToMove = this.NumKeys - midIndex;
                TransferRangeTo(sibling, midIndex, keysToMove);

                this.NumKeys = midIndex;
                sibling.NumKeys = keysToMove;

                sibling.NextLeafId = this.NextLeafId;
                this.NextLeafId = sibling.Id;
            }
            else
            {
                // INTERNAL SPLIT: Promote separatorKey, it is NOT kept in children
                int moveStartOffset = midIndex + 1;
                int keysToMove = this.NumKeys - moveStartOffset;

                // 1. Transfer records and pointers to the new sibling
                TransferRangeTo(sibling, moveStartOffset, keysToMove);

                // 2. Update the sibling's counts and child pointers
                // The sibling's first child (index 0) is the right child of the promoted key
                this.CopyChildrenTo(sibling, moveStartOffset, 0, keysToMove + 1);

                // 3. HYGIENE: Clear the promoted key and the moved pointers from this node
                int keysToClear = this.NumKeys - midIndex;
                for (int i = midIndex; i < this.NumKeys; i++)
                {
                    int offset = GetDataOffset(i);
                    Buffer.AsSpan(offset, RECORD_SIZE).Fill(0);
                }

                this.ClearChildPointers(midIndex + 1, this.Order - midIndex);
                this.NumKeys = midIndex;
            }

            return (sibling, separatorKey);
        }


        /// <summary> 
        /// Performs a bulk physical transfer of child pointers from this node to a sibling.
        /// Used during an internal node split to move the right-hand pointers to the new node.
        /// </summary>
        public void CopyChildrenTo(BNode destination, int sourceIndex, int destIndex, int count)
        {
            if (count <= 0) return;

            int sourceOffset = KidTableStart + (sourceIndex * KID_SIZE);
            int destOffset = KidTableStart + (destIndex * KID_SIZE);

            // Optimized bulk move between two different node buffers
            System.Buffer.BlockCopy(this.Buffer, sourceOffset, destination.Buffer, destOffset, count * KID_SIZE);
        }

        /// <summary> 
        /// Zeroes out a range of child pointers in the physical buffer.
        /// Essential for 'Hygiene' to ensure stale Page IDs do not persist after a shift or split.
        /// </summary>
        public void ClearChildPointers(int startIndex, int count)
        {
            if (count <= 0) return;

            int offset = KidTableStart + (startIndex * KID_SIZE);

            // Efficiently wipes the specified range of pointers in the buffer
            System.Array.Clear(this.Buffer, offset, count * KID_SIZE);
        }

        /// <summary> Migrates a sequential range of records and their associated child pointers to a destination node.
        /// Theis method is used to populate a new sibling during a split. </summary>
        public void TransferRangeTo(BNode destination, int startIdx, int count)
        {
            if (count <= 0) return;

            // 1. Bulk Copy Records (Key + Data pairs)
            // We calculate the source and destination offsets directly in the physical buffer.
            int srcDataOffset = GetDataOffset(startIdx);
            int destDataOffset = destination.GetDataOffset(0);
            System.Buffer.BlockCopy(this.Buffer, srcDataOffset, destination.Buffer, destDataOffset, count * RECORD_SIZE);

            // 2. Bulk Copy Child Pointers (Internal nodes only)
            if (!Leaf)
            {
                // In the HogFishOne N+1 relationship, 'count' keys are flanked by 'count + 1' pointers.
                // We move the entire block of pointers starting from the left child of the first moved key.
                int srcKidOffset = KidTableStart + (startIdx * KID_SIZE);
                int destKidOffset = KidTableStart + (0 * KID_SIZE);
                Debug.Assert(destKidOffset + ((count + 1) * KID_SIZE) <= destination.DataAreaStart);
                System.Buffer.BlockCopy(this.Buffer, srcKidOffset, destination.Buffer, destKidOffset, (count + 1) * KID_SIZE);
            }

            // Set the sibling's count directly; no need for incremental updates.
            destination.NumKeys = count;
        }

        // ##########################################
        // # 9. DIAGNOSTICS & DISK I/O
        // ##########################################

        /// <summary> Reconstructs a BNode by reading a full page of raw bytes from disk and wrapping them in the Slotted-Page logic. </summary>
        public static BNode LoadFromStream(System.IO.BinaryReader reader, int pageSize, int order)
        {
            byte[] buffer = reader.ReadBytes(pageSize);
            if (buffer.Length < pageSize) throw new EndOfStreamException("Partial page read.");
            return new BNode(order, pageSize, buffer);
        }


        /// <summary> Generates a human-readable summary of the node's identity, type, and current list of logical keys. </summary>
        public override string ToString()
        {
            var keys = string.Join(", ", Enumerable.Range(0, NumKeys).Select(i => GetKey(i)));
            return $"Node {Id} (Leaf: {Leaf}, Keys: {NumKeys})\nKeys: [{keys}]\n";
        }


        // ##########################################
        // # 10. TEST FLUENCY & SHIMS
        // ##########################################

        /// <summary> Wraps the key and data at a specific index into a single Element object for easier manipulation in high-level logic. </summary>
        public Element GetElementFixed(int index) => new Element(GetKey(index), GetData(index));

        /// <summary> Iterates through the data heap to extract all active records into a standard list for bulk processing or validation. </summary>
        public List<Element> GetAllElements()
        {
            var list = new List<Element>(NumKeys); 
            for (int i = 0; i < NumKeys; i++)
            {
                list.Add(GetElementFixed(i));
            }
            return list;
        }

        /// <summary> Updates or appends a record at the specified index using an Element object instead of raw integer parameters. </summary>
        public void SetElementFixed(int index, Element element) => SetElementFixed(index, element.Key, element.Data);

        /// <summary> Fluent test helper that resets the node and populates it with a specific set of keys for rapid unit testing. </summary>
        public BNode WithKeys(params int[] keys)
        {
            this.NumKeys = 0;
            for (int i = 0; i < keys.Length; i++)
            {
                SetElementFixed(i, keys[i], keys[i]);
            }

            // CRITICAL: Update the logical count to match the physical writes.
            this.NumKeys = keys.Length;
            return this;
        }

        /// <summary> Fluent test helper that quickly assigns a series of PageIds to the Kid Table for setting up internal node scenarios. </summary>
        public BNode WithChildren(params int[] childIds)
        {
            for (int i = 0; i < childIds.Length; i++) 
            { 
                SetChildId(i, childIds[i]); 
            }
            return this;
        }

        /// <summary> Fluent test helper that allows for setting the PageId in a single chained method call during node initialization. </summary>
        public BNode WithId(int id) 
        { 
            this.Id = id; 
            return this; 
        }
    }
}