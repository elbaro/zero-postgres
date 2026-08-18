# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.10.0](https://github.com/elbaro/zero-postgres/compare/v0.9.0...v0.10.0) - 2026-08-18

### <!-- 1 -->Bug fixes
- new release-plz syntax, breaking digest crate upgrade

### <!-- 9 -->Other
- infra: update Rust crate compio to v0.19.2 ([#62](https://github.com/elbaro/zero-postgres/pull/62))
- [**breaking**] infra!: update breaking dependencies ([#64](https://github.com/elbaro/zero-postgres/pull/64))
- infra: update Rust crate uuid to v1.24.1 ([#65](https://github.com/elbaro/zero-postgres/pull/65))
- tidy: update time crate
- infra: update EmbarkStudios/cargo-deny-action action to v2.1.1 ([#63](https://github.com/elbaro/zero-postgres/pull/63))
- infra: update EmbarkStudios/cargo-deny-action action to v2.0.20 ([#47](https://github.com/elbaro/zero-postgres/pull/47))
- infra: update non-breaking dependencies to v0.3.48 ([#61](https://github.com/elbaro/zero-postgres/pull/61))
- infra: update non-breaking dependencies ([#60](https://github.com/elbaro/zero-postgres/pull/60))
- infra: update non-breaking dependencies to v0.8.52 ([#59](https://github.com/elbaro/zero-postgres/pull/59))
- infra: update non-breaking dependencies to v0.8.51 ([#58](https://github.com/elbaro/zero-postgres/pull/58))
- infra: update non-breaking dependencies to v1.23.3 ([#57](https://github.com/elbaro/zero-postgres/pull/57))
- infra: update non-breaking dependencies to v2.3.10 ([#56](https://github.com/elbaro/zero-postgres/pull/56))
- infra: update non-breaking dependencies to v0.4.45 ([#55](https://github.com/elbaro/zero-postgres/pull/55))
- infra: update non-breaking dependencies to v0.4.32 ([#54](https://github.com/elbaro/zero-postgres/pull/54))
- infra: update non-breaking dependencies to v0.4.31 ([#53](https://github.com/elbaro/zero-postgres/pull/53))
- infra: update Rust crate zerocopy to v0.8.50 ([#52](https://github.com/elbaro/zero-postgres/pull/52))
- infra: update Rust crate uuid to v1.23.2 ([#51](https://github.com/elbaro/zero-postgres/pull/51))
- infra: update Rust crate zerocopy to v0.8.49 ([#50](https://github.com/elbaro/zero-postgres/pull/50))
- infra: update Rust crate memchr to v2.8.1 ([#49](https://github.com/elbaro/zero-postgres/pull/49))
- infra: update Rust crate log to v0.4.30 ([#48](https://github.com/elbaro/zero-postgres/pull/48))
- infra: update EmbarkStudios/cargo-deny-action action to v2.0.18 ([#46](https://github.com/elbaro/zero-postgres/pull/46))
- infra: update Rust crate tokio to v1.52.3 ([#45](https://github.com/elbaro/zero-postgres/pull/45))
- infra: update Rust crate rust_decimal to v1.42.0 ([#44](https://github.com/elbaro/zero-postgres/pull/44))
- infra: update Rust crate tokio to v1.52.2 ([#43](https://github.com/elbaro/zero-postgres/pull/43))
- infra: update Rust crate diesel to v2.3.9 ([#42](https://github.com/elbaro/zero-postgres/pull/42))
- infra: update Rust crate diesel to v2.3.8 ([#41](https://github.com/elbaro/zero-postgres/pull/41))
- infra: fix deny audit failures
- infra: switch CI Rust toolchain from nightly to stable
- infra: use cargo-deny-action v2.0.17
- infra: replace rustsec/audit-check with cargo-deny-action
- infra: remove paths filter so audit required check reports on every PR
- infra: refresh Cargo.lock (includes RUSTSEC-2026-0097 rand fix)
- infra: add deny.toml
- infra: update Rust crate tokio to v1.52.1 ([#39](https://github.com/elbaro/zero-postgres/pull/39))
- infra: update Rust crate uuid to v1.23.1 ([#38](https://github.com/elbaro/zero-postgres/pull/38))
- infra: update Rust crate tokio to v1.52.0 ([#37](https://github.com/elbaro/zero-postgres/pull/37))
- infra: skip release commits in changelog parsers
- infra: disable Renovate dependency dashboard
- infra: update Rust crate rand to v0.10.1 ([#36](https://github.com/elbaro/zero-postgres/pull/36))
- infra: update Rust crate tokio to v1.51.1 ([#35](https://github.com/elbaro/zero-postgres/pull/35))
- infra: update Rust crate tokio to v1.51.0 ([#34](https://github.com/elbaro/zero-postgres/pull/34))
- infra: update non-breaking dependencies ([#33](https://github.com/elbaro/zero-postgres/pull/33))
- infra: update Rust crate zerocopy to v0.8.48 ([#32](https://github.com/elbaro/zero-postgres/pull/32))
- infra: update Rust crate rust_decimal to v1.41.0 ([#31](https://github.com/elbaro/zero-postgres/pull/31))
- infra: update Rust crate uuid to v1.23.0 ([#30](https://github.com/elbaro/zero-postgres/pull/30))
- [**breaking**] infra!: update breaking dependencies ([#25](https://github.com/elbaro/zero-postgres/pull/25))
- infra: update Rust crate zerocopy to v0.8.47 ([#28](https://github.com/elbaro/zero-postgres/pull/28))
- infra: update non-breaking dependencies ([#24](https://github.com/elbaro/zero-postgres/pull/24))
- infra: enable platform automerge and remove schedule restriction
- infra: disable Renovate platformAutomerge
- tidy: move unwrap/expect clippy lints from Cargo.toml to lib.rs

## [v0.9.0](https://github.com/elbaro/zero-postgres/compare/v0.8.0...v0.9.0) - 2026-03-02

### <!-- 0 -->New features
- diesel test
- diesel
- experimental compio
- RefFromRow
- zerocopy exec_foreach_ref

### <!-- 1 -->Bug fixes
- move error draining from drivers into state machines
- drain server errors so the connection can be reused ([#16](https://github.com/elbaro/zero-postgres/pull/16))
- move separateMajorMinor to top-level config

### <!-- 2 -->Performance
- eliminate a temporary allocation from Vec ([#17](https://github.com/elbaro/zero-postgres/pull/17))
- compio streams were not buffered

### <!-- 3 -->Documentation
- document derive feature flag
- add a link to diesel benchmark
- update
- add diesel flag to README

### <!-- 9 -->Other
- tidy: eliminate all clippy warnings in integration tests ([#26](https://github.com/elbaro/zero-postgres/pull/26))
- tidy: relax clippy lints, add clippy.toml, expose PG_EPOCH_JULIAN_DAY
- tidy: remove clippy::integer_division_remainder_used lint
- tidy: suppress redundant clippy restriction lints for --all-features
- tidy: rename remaining Error::Protocol to Error::LibraryBug in compio
- [**breaking**] tidy!: rename Error::Protocol to Error::LibraryBug
- [**breaking**] tidy!: replace IntoStatement Option methods with StatementRef enum
- tidy: eliminate redundant bounds checks and clippy warnings
- tidy: simplify get_conn in error_recovery tests
- [**breaking**] tidy!: rename severity accessors on ServerError for clarity
- tidy: remove redundant test_ prefix from test functions
- tidy: fix trivial clippy warnings and enforce clippy in CI
- infra: grant checks:write permission to security audit
- infra: tune renovate config and normalize dep ranges
- infra: fix renovate hourly limit and branch splitting
- infra: group renovate PRs by breaking changes
- infra: configure renovate
- infra: replace trybuild with compile_fail doc tests
- infra: fix release-plz for multi-packages
- [**breaking**] tidy!: rename feature flags

## [v0.8.0](https://github.com/elbaro/zero-postgres/compare/v0.7.0...v0.8.0) - 2026-01-21

### <!-- 0 -->New features
- add tests for [T], Vec<T> params
- Support [T], Vec<T> as parameters

### <!-- 3 -->Documentation
- revise data type page
- data type conversion

## [v0.7.0](https://github.com/elbaro/zero-postgres/compare/v0.6.0...v0.7.0) - 2026-01-11

### <!-- 1 -->Bug fixes
- [**breaking**] fix async closure API
- [**breaking**] change transaction implicit commit/rollback behavior

### <!-- 3 -->Documentation
- add more explanations

### <!-- 9 -->Other
- [**breaking**] tidy!: rename conn.tx to conn.transaction
- [**breaking**] tidy!: rename TextHandler, BinaryHandler to SimpleHandler, ExtendedHander

## [v0.6.0](https://github.com/elbaro/zero-postgres/compare/v0.5.0...v0.6.0) - 2026-01-10

### <!-- 1 -->Bug fixes
- [**breaking**] exec_foreach closure returns Result

### <!-- 3 -->Documentation
- remove api reference

## [v0.5.0](https://github.com/elbaro/zero-postgres/compare/v0.4.3...v0.5.0) - 2026-01-10

### <!-- 0 -->New features
- add #[derive(FromRawRow)]

### <!-- 1 -->Bug fixes
- wrong release-plz config
- wrong release-plz config
- use Conn::new instead of Conn::connect in docs

### <!-- 3 -->Documentation
- add mdbook and documentation links
- clarify TLS support is experimental

### <!-- 9 -->Other
- tidy: remove unncessary pipeline's own buffer

## [v0.4.3](https://github.com/elbaro/zero-postgres/compare/v0.4.2...v0.4.3) - 2025-12-29

### <!-- 1 -->Bug fixes
- gate unix socket code behind cfg(unix)

## [v0.4.2](https://github.com/elbaro/zero-postgres/compare/v0.4.1...v0.4.2) - 2025-12-29

### <!-- 1 -->Bug fixes
- edge case in pipeline API

## [v0.4.1](https://github.com/elbaro/zero-postgres/compare/v0.4.0...v0.4.1) - 2025-12-29

### <!-- 1 -->Bug fixes
- support complex pipeline cases

## [v0.4.0](https://github.com/elbaro/zero-postgres/compare/v0.3.2...v0.4.0) - 2025-12-28

### <!-- 9 -->Other
- [**breaking**] tidy!: rename prefer_unix_socket to upgrade_to_unix_socket
- infra: simplify pr body
- infra: pr body
- infra: pr sections
- infra: allow clippy failure

## [0.3.2](https://github.com/elbaro/zero-postgres/compare/v0.3.1...v0.3.2) - 2025-12-28

### Other

- fix permission

## [0.3.1](https://github.com/elbaro/zero-postgres/compare/v0.3.0...v0.3.1) - 2025-12-28

### Other

- use postgres 18
- change relase PR title
- trusted publishing
- setup release-plz
- release-plz
