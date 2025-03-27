# Farebox

Farebox is a high-performance transit routing engine built using the [RAPTOR algorithm](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/raptor_alenex.pdf), optimized for lightweight, global-scale public transit routing. Designed to serve developers building applications requiring fast and resource-efficient transit planning (e.g., maps apps, trip-planning APIs), it avoids heavy preprocessing steps while supporting planet-scale coverage through memory-mapped timetables.

## Key Features
- **Lightweight & Fast**:
  - Outperforms MOTIS/Transitous by anecdotally observed 5-10x in query speed.
  - Single-core routing across the continental U.S. completes in seconds; local routes usually resolve in <250ms.

- **Planet-Scale Coverage**:
  - Memory-mapped timetable data allows a single instance to handle global networks with minimal RAM usage (via `memmap2`).

- **Multi-Agency Support**:
  - Load multiple GTFS feeds from a directory for seamless cross-agency routing.

- **Timezone Awareness**:
  - Automatically handles timezone conversions based on GTFS feed data. Developers are responsible for converting epoch timestamps to local time in their app layer.

- **HTTP API Endpoint**:
  ```http
  POST /v1/plan
  ```
  Example request:
  ```bash
  curl -d '{"from":{"lat":47.679591,"lon":-122.356388},"to":{"lat":47.616440,"lon":-122.320440},"start_at":1742845000000}' \
       https://transit.maps.earth/v1/plan
  ```

- **GTFS Compatibility**:
  - Supports modern GTFS feeds via the `gtfs-structures` crate.
  - No real-time (GTFS-RT) support yet; prioritized roadmap features include alerts, delays and vehicle position updates.

## Getting Started

### Prerequisites
1. **Rust** (`rustc >= 1.85` tested).
2. **OpenSSL development package**: Install via your OS's package manager (e.g., `libssl-dev` on Ubuntu).

### Quickstart Commands
#### Step 1: Build the Timetable Database
```bash
# For a single GTFS feed:
cargo run --release --bin build_timetable -- \
    --base-path /path/to/timetable \
    --gtfs-path gtfs.zip

# For multi-agency feeds (directory of .zip files):
cargo run --release --bin build_timetable -- \
    --base-path /path/to/timetable \
    --gtfs-path ./gtfs_feeds/
```

#### Step 2: Run the API Server
```bash
cargo run --release --bin serve -- --base-path /path/to/timetable
```

## Architecture
- **RAPTOR Algorithm**: Implements all pruning rules from the original paper for optimal performance.
- **Memory Mapping**: Uses `memmap2` to load timetable data directly from disk, enabling fast access without RAM overhead.
- **Designed for Modularity**: Decouples routing logic from geocoding/external services (e.g., uses Valhalla for transfer routes, but otherwise allows easy integration into an existing stack).

## Performance Benchmarks
| Metric                | Farebox          | OpenTripPlanner/MOTIS       |
|-----------------------|------------------|-----------------|
| Query Latency         | 150ms - 2.5s     | ~150ms-1 minute |
| Memory Usage          | ~1.5GB (global)  | Higher? (OTP: ~4GB+, MOTIS=?) |

*Note: Benchmarks are informal and based on limited testing. Systematic comparisons pending.*

---

## Roadmap
- **GTFS-RT Support** (priority order):
  1. Service alerts and closures
  2. Real-time delays
  3. Vehicle positions
- **Performance Quantification**: Come up with better benchmarks against MOTIS and OpenTripPlanner.
- **rRAPTOR Implementation**: Long-term goal for multi-departure-time routing.
- **Documentation**: Ongoing work to finalize API response formats and provide detailed guides.

## Contributing
- Farebox is in active development; contributions (documentation, testing, or features) are welcome.
- Check the repository's issue tracker for tasks, but note there are no formal contribution guidelines yet.

## Known Limitations
- **No Real-Time Updates**: Only static GTFS feeds supported currently.
- **API Stability**: The `/v1/plan` response format may evolve as documentation finalizes, but no compatibility breaking changes to the v1 endpoint after the initial release.

## When to Use Farebox?
You may want to use this project if you need:
- Fast, lightweight routing for global-scale transit networks on modest hardware.
- A minimal API layer that integrates easily with modern web stacks (geocoding, map rendering handled externally).

Avoid if you require:
- Full-featured trip-planning like OpenTripPlanner's extensive customization or real-time capabilities.

## License
[Apache-2.0](LICENSE)
