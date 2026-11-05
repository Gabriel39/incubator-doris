# Native MongoDB Catalog design

Design RFC for a native, read-only MongoDB Catalog in Apache Doris.

- [Download the design PDF](native-mongodb-catalog-design.pdf) (Chinese; 47 pages; 16 chapters; four diagrams).
- The PDF includes the architecture, FE/BE contracts, BSON type mappings, safe predicate pushdown, split planning, consistency, resource control, SQL analytics, tests, and a dependency-oriented implementation roadmap.
- Code-reading baseline: `c5bfd6819f393bde37b73147abddc2f9a44641cb`.
- This snapshot describes proposed functionality. Dependency preparation is tracked by [apache/doris#70249](https://github.com/apache/doris/pull/70249); this document does not claim that the native Catalog is implemented or qualified.
- PDF SHA-256: `f6d6f816435dc2ff09a8ed5e9994906f59f481e23913044a36f34a278baa9763`.

This branch publishes design artifacts only.
