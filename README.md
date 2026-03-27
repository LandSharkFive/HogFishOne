# HogFishOne: High-Performance Fixed-Length B+ Tree

A disk-persistent B+ Tree implementation in C# optimized for raw throughput. **HogFishOne** utilizes a **Fixed-Length Page architecture**, using deterministic record positioning and a dedicated **Data Heap** for high-efficiency storage.

## 1. File & Page Architecture
The engine manages data on a `FileStream` using a 4KB padded header for safety and fixed-size pages to ensure predictable disk I/O.

### Physical Layout
| Section | Offset | Size | Purpose |
| :--- | :--- | :--- | :--- |
| **Header** | `0` | 4096 bytes | Padded header: RootId, NodeCount, and FirstLeafId. |
| **Nodes** | `4096` | $N$ * PageSize | Fixed-length pages containing node headers and dense record arrays. |
| **FreeList** | EOF | Variable | A stack of Disk IDs available for structural re-allocation. |

### Fixed-Length Record Management
HogFishOne avoids the overhead of complex page-redirection by using a **Non-Slotted layout**:
* **Deterministic Addressing**: Because every record is a fixed length (4-byte key and 4-byte data), the physical address of any record is calculated directly via its index within the page.
* **Physical Memory Shifts**: Insertion and deletion operations utilize `BlockCopy` to shift records within the page, maintaining sorted order with minimal CPU overhead.
* **Data Heap Leaves**: All actual data is stored exclusively in the leaf-level "Data Heap," ensuring internal nodes remain small and the tree remains shallow.

## 2. Technical Features
* **Maximum Fan-Out**: Optimized for 8-byte record pairs to maximize the number of keys per page and reduce total disk hits.
* **Horizontal Linked Traversal**: Leaf nodes maintain `NextLeafId` pointers, creating a linked chain for $O(N)$ sequential range scans.
* **Proactive Structural Maintenance**: Implements a "single-pass" strategy, splitting or merging nodes during the initial descent to prevent the need for recursive back-tracking.
* **Integrity Validation**: Includes `ValidateIntegrity()` to verify leaf-link continuity, key-range boundaries, and tree balance.

## 3. Usage

```csharp
// Initialize HogFishOne with Fixed-Length page logic.
using (var tree = new BTree("HogFishOne.db")) 
{
    // Records are physically shifted in the page to maintain order
    tree.Insert(42, 100); 
    
    // Search uses deterministic math to locate keys within pages
    if (tree.TrySearch(42, out Element result)) 
    {
        Console.WriteLine($"Found Data: {result.Data}");
    }

    // Range Query follows the physical leaf-link chain across the Data Heap
    var range = tree.GetKeyRange(10, 50); 
}