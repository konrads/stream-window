# tokio-stream windows

[![build](../../workflows/build/badge.svg)](../../actions/workflows/build.yml)

Window implementations:

- [x] tumbling
- [x] sliding
- [x] periodic (requires injection of a clock stream, window is emitted every `tick`)
