# Farebox

Fast RAPTOR implementation in Rust designed for memory-constrained machines.
* Built on top of Valhalla for transfers and first/last mile routing.
* Multi-agency and timezone-aware routing.
* Route shapes are not returned with the itineraries.

The goal of this project is to supplement OpenTripPlanner in [Headway](https://github.com/headwaymaps/headway) as an infill service for areas not covered by [OpenTripPlanner](https://www.opentripplanner.org/) instances.
Memory mapping is used for the timetables with the aim of enabling planet-scale coverage with a single instance, and affordable hosting costs.
GTFS-RT support is in scope.
Per-request walking/cycling costing model tweaks will likely never be supported because RAPTOR requires pre-computation of transfers, but pre-defined costing models for walking, cycling, and wheelchair usage are in scope.

This project may be obsoleted by Valhalla's built-in multimodal support depending on its performance characteristics.
Based on the information in the RAPTOR paper about its performance versus A* based methods, it seems likely that there will still be a place for `farebox` even once Valhalla can do multimodal trips.
This project may also eventually support a rRAPTOR routing option which would allow simultaneous calculation of itineraries across a wide range of departure times, which isn't something you can do with an A* search through a time-dependant routing graph to my knowledge.
