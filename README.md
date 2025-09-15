Buffered Log Handler
The handler package provides a buffered slog.Handler implementation for Go's structured logging library.

This handler is designed for situations where you need to collect and batch log records before processing them. Instead of writing logs directly to a destination, the Buffered handler first transforms each slog.Record into a custom type and then sends it to a provided store.
