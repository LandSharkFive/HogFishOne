using System.Buffers.Binary;

namespace HogFishOne
{

    /// <summary>
    /// Represents a fundamental key-value pair stored within the B+ Tree.
    /// This structure is designed for fixed-width disk serialization, ensuring 
    /// predictable offsets during binary I/O operations.
    /// </summary>
    public struct Element : IComparable<Element> 
    {
        public int Key;   // The key of the element
        public int Data; // That data that each element contains

        /// <summary> Initializes a new empty element with sentinel values. </summary>
        public Element() { Key = -1; Data = -1; }

        /// <summary> Initializes an element with a specific key and data pair. </summary>
        public Element(int key, int data) { Key = key; Data = data; }

        /// <summary> Returns a sentinel element representing an empty or null state. </summary>
        public static Element GetDefault() => new Element(-1, -1);

        /// <summary> Compares elements based solely on their Key property. </summary>
        public int CompareTo(Element other)
        {
            return this.Key.CompareTo(other.Key);
        }

        /// <summary> Returns a formatted string for debugging: [Key, Data]. </summary>
        public override string ToString()
        {
            return $"[{Key}, {Data}] ";
        }

        public int GetSerializedSize()
        {
            return 8;
        }


        public byte[] ToBytes()
        {
            byte[] bytes = new byte[8];
            Span<byte> span = bytes.AsSpan();
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(0, 4), this.Key);
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4, 4), this.Data);
            return bytes;
        }


    }
}
