# Storage Density Analysis: HogFishOne Data Heap

## 1. Theoretical Framework
In the **HogFishOne** architecture, storage density is maximized by utilizing a **Fixed-Length** record strategy. Unlike traditional B-Trees that suffer from pointer overhead and fragmentation, HogFishOne uses a dense-pack approach within the **Data Heap**.

By utilizing `BlockCopy` for physical record shifts, the engine ensures that every page in the leaf-level Data Heap remains packed, minimizing the "dead space" typically found in dynamic-sized slotted pages.

## 2. Density Formula
Efficiency is measured by comparing the active 8-byte records against the total physical capacity of the allocated page (minus the node header):

$$Density = \left( \frac{\text{Active Records}}{\text{Max Records per Page}} \right) \times 100$$

## 3. Example (4KB Page)
| Parameter | Value |
| :--- | :--- |
| **Record Size (Key + Data)** | 8 Bytes |
| **Page Size** | 4096 Bytes |
| **Max Records Per Page** | ~510 (with header) |
| **Actual Records Stored** | 500 |
| **Storage Efficiency** | **~98%** |

## 4. Read and Write Efficiency
* **Deterministic I/O:** Because records are fixed-length, the engine calculates the exact physical offset for any search, eliminating the need to parse a slot table.
* **Cache-Friendly Shifts:** Using `BlockCopy` for insertions keeps data contiguous, which is highly efficient for modern CPU cache lines.
* **Shallow Tree Depth:** By packing 8-byte pairs tightly, the fan-out remains high, ensuring that even massive datasets require minimal disk hops.
* **Linked Leaf Scans:** The `NextLeafId` pointers allow for sequential scans across the Data Heap without ever returning to the root node.

## 5. Implementation Note
The density in HogFishOne is maintained by the proactive rebalancing strategy:
* **Dense Packing:** Best for static or read-heavy datasets where 95%+ density is preferred.
* **Padding Strategy:** If frequent insertions are expected, the engine can leave "breathing room" in pages to reduce the frequency of physical shifts and page splits.