# HogFishOne Bulk Loader: Top-Down Design

## Overview
The Bulk Loader provides an $O(N)$ mechanism to build a **HogFishOne** tree from a pre-sorted dataset. By utilizing a top-down recursive partitioning strategy, the loader minimizes memory overhead and maximizes write throughput by creating a dense-packed **Data Heap** where all fixed-length records reside.

## The Top-Down Strategy
This loader treats the sorted input as a range to be partitioned into a balanced hierarchy, bypassing the complexity of standard insertion and physical memory shifts:

* **Recursive Partitioning:** The loader calculates optimal pivots from the sorted list to populate internal index nodes, ensuring a clean, balanced structure from the root down to the leaf layer.
* **High Occupancy & Density:** The algorithm is tuned for the **Fixed-Length** architecture, achieving up to 98% node occupancy by eliminating the "dead space" typically found in dynamic-sized pages.
* **Data Heap Linking:** During the build process, the loader automatically links each leaf node in the Data Heap to its neighbor, establishing the horizontal `NextLeafId` pointers required for efficient range scans.
* **Deterministic Memory Management:** The implementation is lightweight and easy on memory, as it calculates the tree structure mathematically and uses `BlockCopy` to write dense record arrays directly to disk.
* **Adaptive Balancing:** While the loader prioritizes high density, it remains flexible to ensure the structural requirements of a B+ Tree are met even in edge cases.

## Usage
The `TreeBuilder` handles the recursive logic internally, ensuring the resulting `HogFishOne.db` is ready for immediate querying.

```csharp
   // 1. Generate sorted data.
   var data = Enumerable.Range(1, 50).Select(i => new Element(i, i)).ToList();

   // 2. Run Top-Down Bulk Loader for HogFishOne.  
   var builder = new TreeBuilder();
   builder.CreateFromSorted("HogFishOne.db", data);
```