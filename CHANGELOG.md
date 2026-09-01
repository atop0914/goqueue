# Changelog

All notable changes to GoQueue are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [v1.0.0] - 2026-09-01

### Added
- Admin operations programmatic API: `Client.Pause/Resume/IsPaused/Purge/RequeueDead/RequeueDeadJob`
- Admin backend capability interfaces: `Pauser`, `Purger`, `DeadRequeuer`, `AdminQueue`
- `ErrAdminUnsupported` sentinel for backends without admin capabilities
- In-memory backend full admin surface (`memory_admin.go`)
- SQLite backend admin surface (`store/sqlite/admin.go`)
- Redis backend admin surface (`store/redis/admin.go`)
- Dashboard admin endpoints: `POST /api/admin/{pause,resume,purge,requeue-dead}`
- Dashboard admin button row in HTML overview
- `examples/admin-ops` demonstrating programmatic admin flows
- `JobIDFromContext` exported helper
- `WithContextDecorator` option
- `CombineHooks` helper
- `obs/metrics` Prometheus extension package
- `obs/tracing` OpenTelemetry extension package

### Changed
- SQLite unique index migration v2: dead rows keep `unique_key` for requeue contention detection
- README expanded with Quick start, per-backend admin notes, production checklist, and observability guides

### Fixed
- SQLite `txlock=IMMEDIATE` to avoid lock-upgrade busy storms under concurrent producers
