This directory contains temporary measurement infrastructure for the 2026-10-13 optimization cycle.

The immutable source baselines are srg b9933fd84f3e696a15f2a2880bd4093be87b587d and fcupdater 7efaa72ed632cfa4b4eb1aec4c050345786cafa5. Rust is pinned to 1.99.0 on all six native hosts. Product manifests, release profiles, dependency locks, and original workbook bytes are preserved. All training and workbook mutation runs occur in isolated archive copies.

Existing-profile package size and refreshed-profile package size are separate measurements. Fresh training runs the real CLI verification, plus deterministic offline parse, rank, shared-string, and save probes. Probe builds are diagnostic copies and are not distributed as product binaries. New profiles remain measurement artifacts until native behavior, profile compatibility, package size, allocations, and protected runtime bounds have been reviewed.

Protected runtime acceptance: interleaved paired samples, median pair ratio, seeded 10,000-resample 95% bootstrap interval with an upper bound of 1.05; an initial 32 pairs may be followed by one bounded 64-pair repeat. Timing uncertainty is reported as unresolved. Existing cargo tests are also run, but zero built-in tests do not count as behavioral proof. Numerical and workbook checks use independent Python integer/Decimal and ZIP/XML oracles.

The workflows are branch-scoped, have read-only repository permissions, publish only temporary evidence artifacts, and do not release, deploy, or update main.
