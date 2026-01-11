# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
