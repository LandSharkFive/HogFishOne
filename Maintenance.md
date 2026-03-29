# Maintenance: HogFishOne

## 1. Offline Compaction
As deletions occur in the Data Heap, the file may develop "holes" or fragmented space. While the FreeList tracks these for reuse, a heavily modified tree may require compaction to optimize the physical file size and restore perfect record density.

**How it works:**
1. **Temporary Workspace:** Creates a temporary database file.
2. **Live Node Identification:** Performs a Breadth-First Search (BFS) starting from the `RootId` to identify all live internal nodes and active Data Heap leaves.
3. **Sequential Mapping:** Re-maps every live node to a new, contiguous ID. This ensures the linked Data Heap remains physically sequential on disk, which is critical for high-speed range scans.
4. **Atomic Swap:** Swaps the old file with the new, optimized file once the process is verified.

## 2. Bulk Loading
To avoid the overhead of individual `Insert` operations and the associated physical memory shifts (`BlockCopy`), use the `TreeBuilder` mechanism.

* **Pre-Sorted Input:** Ensure your record list is sorted by Key in ascending order.
* **Top-Down Construction:** This builds the tree by recursively partitioning the sorted input, filling leaf pages to maximum capacity in the Data Heap.
* **Horizontal Linking:** As leaf pages are created in the heap, they are immediately linked to their predecessor to establish $O(1)$ transition paths for range queries.

## 3. The FreeList Strategy
The FreeList is a stack of integers stored at the end of the file to manage disk space for both internal nodes and Data Heap leaves.
* **Push (Deallocation):** When a node becomes empty due to a merge or deletion, its ID is pushed onto the FreeList for future allocation.
* **Pop (Allocation):** When a new node ID is required for a split or expansion, the system pops a reused ID from the stack before incrementing the global `NodeCount`.