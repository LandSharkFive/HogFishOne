# Quick Start: HogFishOne

**HogFishOne** is a high-performance, disk-persistent B+ Tree engine designed for deterministic seek times and a lean physical footprint using a **Fixed-Length** page architecture.

## 1. Setup & Initialization

The engine utilizes a **Fixed-Length** strategy where every record occupies a predictable amount of space. The file is initialized with a **4KB padded safety header** to protect root metadata and ensure proper sector alignment.

```csharp 
using HogFishOne;

// Initialize the tree with fixed-length record logic.
var tree = new BTree("HogFishOne.db");
```

### Part 2: Core Operations
## 2. Core Operations
Operations are immediately persisted to the **Data Heap**. Every Insert or Delete triggers a physical disk flush to ensure your data survives unexpected termination.

```csharp
// 1. Insertion
// Records (4-byte key/data) are physically shifted via BlockCopy 
// to maintain sorted order within the fixed-length page.
tree.Insert(500, 12345); 

// 2. Search
// Traverses the navigation index directly to the Data Heap leaves.
if (tree.TrySearch(500, out Element result))
{
    Console.WriteLine($"Found Data: {result.Data}");
}

// 3. Proactive Deletion
// Implements a "single-pass" strategy that rebalances nodes 
// during the initial descent to prevent recursive backtracking.
tree.Delete(500);
```
### Part 3: Maintenance Tools

## 3. Maintenance Tools
Keep your storage file lean and verified using the built-in utility methods.

```csharp 
// Reclaim space from deleted nodes via the FreeList.
tree.Compact();

// Run a full audit of B+ Tree invariants, including 
// leaf-link continuity and key-range boundaries.
tree.ValidateIntegrity();
```