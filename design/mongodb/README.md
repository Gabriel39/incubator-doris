# Native MongoDB Catalog design

Design RFC for a native, read-only MongoDB Catalog in Apache Doris.

- [Download the design PDF](native-mongodb-catalog-design.pdf) (Chinese; 49 pages; 16 chapters; four diagrams).
- Initial-release basic reads cover ordinary collections, standard views, and time-series collections. Parallel scans, common snapshots, and pushdown are qualified per object kind.
- The PDF includes architecture, FE/BE contracts, BSON type mappings, Schema management, predicate semantics, splitting, consistency, resource control, SQL analytics, mandatory object-specific tests, and a dependency-oriented roadmap.
- Implementation tracking: [apache/doris#70251](https://github.com/apache/doris/issues/70251).
- Code-reading baseline: `c5bfd6819f393bde37b73147abddc2f9a44641cb`.
- This is proposed functionality. Dependency preparation is tracked by [apache/doris#70249](https://github.com/apache/doris/pull/70249); this document does not claim the native Catalog is implemented or qualified.
- PDF SHA-256: `c8c1b534889580cc6a346406cfbe3f9175b37c2af024552a19506c49ae5e79c5`.

This branch publishes design artifacts only.
