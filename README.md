# Farebox

Fast RAPTOR implementation in Rust designed for memory-constrained machines.
Built on top of Valhalla for transfers and first/last mile routing.
This project is very incomplete, but it can generate itineraries based on stops and trips in a single GTFS feed.
Multi-agency routing is not currently supported.
It is not currently aware of the concept of a service day.
It does not currently handle time zones.
Route shapes are not returned with the itineraries.
There's no web service available.

The goal of this project is to supplement OpenTripPlanner in [Headway](https://github.com/headwaymaps/headway) as an infill service for areas not covered by [OpenTripPlanner](https://www.opentripplanner.org/) instances.
Memory mapping is used for the timetables with the aim of enabling planet-scale coverage with a single instance, and affordable hosting costs.
GTFS-RT support is in scope.
Per-request walking/cycling costing model tweaks will likely never be supported because RAPTOR requires pre-computation of transfers, but pre-defined costing models for walking, cycling, and wheelchair usage are in scope.

This project may be obsoleted by Valhalla's built-in multimodal support depending on its performance characteristics.
Based on the information in the RAPTOR paper about its performance versus A* based methods, it seems likely that there will still be a place for `farebox` even once Valhalla can do multimodal trips.
This project may also eventually support a rRAPTOR routing option which would allow simultaneous calculation of itineraries across a wide range of departure times, which isn't something you can do with an A* search through a time-dependant routing graph to my knowledge.
Even without any additional features, the flat, cache-efficient data structures that `farebox` uses allow it to generate optimized itineraries for intra-city trips with multiple transfers in well under 10 milliseconds (often under 5), which may be a tough target for an A* search to match.
