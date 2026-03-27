# Technical Specification: HogFishOne

## 1. Storage Architecture

### Fixed-Page Paging Logic
To achieve O(1) disk seeking and hardware-aligned I/O, the engine utilizes a fixed-page size strategy. Every node (page) in the file is identical in size, allowing for deterministic physical addressing.

* **Padded File Header**: The first **4096 bytes** are reserved for the File Header to ensure all subsequent data nodes align with standard disk sector boundaries.
* **Disk Offset Calculation**: `Offset = (PageSize * DiskId) + 4096`

### Binary Serialization & Persistence
The engine is built for raw performance using `BinaryReader` and `BinaryWriter` for predictable, low-level I/O.
* **Immediate Persistence**: Every `DiskWrite` operation is followed by a `FileStream.Flush()` to ensure physical synchronization with the storage medium.
* **Data Sanitization**: To prevent "ghost references" or stale data leaks, vacated slots created by deletions or record shifts are explicitly **"Nuclear Wiped"** (overwritten with `-1` or `default`).

## 2. Core Logic & Invariants

### Root Management
The File Header maintains the `RootId`. The system handles two primary structural transformations:
1. **Root Split**: When the root node reaches its fixed capacity, it splits. A new internal node is allocated to become the parent, and the `Header.RootId` is updated.
2. **Root Collapse**: If a deletion results in a root with zero keys but at least one child, the primary child is promoted to the new `RootId` to keep the tree shallow.

### Data Residency & Navigation
* **Internal (Index) Nodes**: These nodes store **Separator Keys** and child pointers. They serve as "signposts" for navigation: keys in the left subtree are strictly less than the separator, while keys in the right subtree are greater than or equal to it.
* **The Data Heap (Leaf Nodes)**: In the HogFishOne architecture, the leaf level is the **exclusive** residence for actual data records. 
* **Horizontal Linking**: Each leaf node in the Data Heap maintains a `NextLeafId` pointer, creating a linked chain that allows for O(N) sequential range scans without returning to the root.

### Single-Pass Maintenance
The engine utilizes a **proactive, top-down** strategy for tree maintenance. As the algorithm descends the tree for an operation, it preemptively rebalances or splits nodes to ensure the operation can be completed in a single pass without the need for expensive recursive backtracking.