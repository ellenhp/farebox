pub static EARTH_RADIUS_APPROX: f64 = 6_371_000f64;

pub fn lat_lng_to_cartesian(lat: f64, lng: f64) -> [f64; 3] {
    if !lat.is_finite() || !lng.is_finite() {
        return [0.0; 3];
    }
    let lat = lat.to_radians();
    let lng = lng.to_radians();
    [
        EARTH_RADIUS_APPROX * lat.cos() * lng.sin(),
        EARTH_RADIUS_APPROX * lat.cos() * lng.cos(),
        EARTH_RADIUS_APPROX * lat.sin(),
    ]
}

pub fn cartesian_to_lat_lng(coords: [f64; 3]) -> (f64, f64) {
    let lng = f64::atan2(coords[0], coords[1]);
    let lat = (coords[2] / EARTH_RADIUS_APPROX).asin();
    let lat = lat.to_degrees();
    let lng = lng.to_degrees();
    (lat, lng)
}

#[cfg(test)]
mod test {
    use approx::assert_abs_diff_eq;

    use crate::{cartesian_to_lat_lng, lat_lng_to_cartesian};

    #[test]
    fn test_zeros() {
        let coords = lat_lng_to_cartesian(0f64, 0f64);
        assert_abs_diff_eq!(coords[0], 0f64, epsilon = 0.001);
        assert_abs_diff_eq!(coords[1], super::EARTH_RADIUS_APPROX, epsilon = 0.001);
        assert_abs_diff_eq!(coords[2], 0f64, epsilon = 0.001);
    }

    #[test]
    fn test_poles() {
        let coords = lat_lng_to_cartesian(90f64, 0f64);
        assert_abs_diff_eq!(coords[0], 0f64, epsilon = 0.001);
        assert_abs_diff_eq!(coords[1], 0f64, epsilon = 0.001);
        assert_abs_diff_eq!(coords[2], super::EARTH_RADIUS_APPROX, epsilon = 0.001);

        let coords = lat_lng_to_cartesian(-90f64, 0f64);
        assert_abs_diff_eq!(coords[0], 0f64, epsilon = 0.001);
        assert_abs_diff_eq!(coords[1], 0f64, epsilon = 0.001);
        assert_abs_diff_eq!(coords[2], -super::EARTH_RADIUS_APPROX, epsilon = 0.001);
    }

    #[test]
    fn test_inverse() {
        let coords = lat_lng_to_cartesian(45f64, 60f64);
        let (lat, lng) = cartesian_to_lat_lng(coords);
        assert_abs_diff_eq!(lat, 45f64, epsilon = 0.001);
        assert_abs_diff_eq!(lng, 60f64, epsilon = 0.001);
    }

    #[test]
    fn test_far_west() {
        let coords = lat_lng_to_cartesian(45f64, -150f64);
        let (lat, lng) = cartesian_to_lat_lng(coords);
        assert_abs_diff_eq!(lat, 45f64, epsilon = 0.001);
        assert_abs_diff_eq!(lng, -150f64, epsilon = 0.001);
    }

    #[test]
    fn test_far_east() {
        let coords = lat_lng_to_cartesian(45f64, 150f64);
        let (lat, lng) = cartesian_to_lat_lng(coords);
        assert_abs_diff_eq!(lat, 45f64, epsilon = 0.001);
        assert_abs_diff_eq!(lng, 150f64, epsilon = 0.001);
    }
}
