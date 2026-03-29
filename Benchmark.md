## Benchmarks

The benchmark results demonstrate a highly performance-oriented B+ Tree implementation, characterized by exceptional Bulk Load efficiency and robust structural integrity. At the 100,000-key threshold, the system maintains a shallow depth (Height=3), ensuring logarithmic search times even as the dataset scales. A notable highlight is the implementation's memory management; despite the heavy churn of 100,000 random insertions and subsequent deletions, the Health Reports consistently show zero zombies and dangling pointers.

---

## 1. Bulk Load Performance
Bulk loading shows exceptional efficiency, likely due to the sequential nature of the data allocation and reduced overhead compared to individual insertions.

| Dataset Size | Duration | Node Density | Tree Height |
| :--- | :--- | :--- | :--- |
| 100,000 Keys | 9ms | 78.94% | 3 |
| 10,000 Keys | 5ms | 76.31% | 2 |
| 1,000 Keys | 3ms | 59.52% | 2 |

> Note: Bulk loading 100k keys is approximately 256x faster than random insertion for the same volume.

---

## 2. Search Performance
Random search benchmarks measure the time taken to locate keys across the tree structure.

| Dataset Size | Duration |  Reachable Nodes |
| :--- | :--- | :--- |
| 100,000 Keys | 1,489ms |  377 |
| 10,000 Keys | 93ms |  39 |
| 1,000 Keys | 5ms |  5 |

---

## 3. Random Insert Performance
This section tracks the cost of dynamic insertions, which include the overhead of finding the leaf, splitting nodes, and updating parent pointers.

| Dataset Size | Duration | Node Density | Peak Height |
| :--- | :--- | :--- | :--- |
| 100,000 Keys | 2,307ms | 59.00% | 3 |
| 10,000 Keys | 209ms | 77.63% | 2 |
| 1,000 Keys | 26ms | 49.16% | 2 |

Integrity Check: Zombies/Ghosts: 0 
* Horizontal Count: Matches total keys (100%).

---

## 4. Random Delete Performance
Deletion benchmarks test the tree's ability to merge nodes and shrink in height while maintaining a valid structure.

| Dataset Size | Duration | 
| :--- | :--- | 
| 100,000 Keys | 2,290ms | 
| 10,000 Keys | 374ms | 
| 1,000 Keys | 33ms | 


Cleanup Efficiency: The tree successfully deallocated all internal nodes in every test case, returning to a root-only state ($H=1$) with no memory leaks.