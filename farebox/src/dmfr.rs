#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]

use serde::{Deserialize, Serialize};

#[doc = r" Error types."]
pub mod error {
    #[doc = r" Error from a TryFrom or FromStr implementation."]
    pub struct ConversionError(std::borrow::Cow<'static, str>);
    impl std::error::Error for ConversionError {}
    impl std::fmt::Display for ConversionError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            std::fmt::Display::fmt(&self.0, f)
        }
    }
    impl std::fmt::Debug for ConversionError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            std::fmt::Debug::fmt(&self.0, f)
        }
    }
    impl From<&'static str> for ConversionError {
        fn from(value: &'static str) -> Self {
            Self(value.into())
        }
    }
    impl From<String> for ConversionError {
        fn from(value: String) -> Self {
            Self(value.into())
        }
    }
}
#[doc = "Authorization"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"type\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"info_url\": {"]
#[doc = "      \"description\": \"Website to visit to sign up for an account.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"param_name\": {"]
#[doc = "      \"description\": \"When type=query_param, this specifies the name of the query parameter.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"type\": {"]
#[doc = "      \"description\": \"Authorization approach: HTTP header, basic authentication, query parameter, or path segment in a Transitland Extended URL.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"header\","]
#[doc = "        \"basic_auth\","]
#[doc = "        \"query_param\","]
#[doc = "        \"path_segment\","]
#[doc = "        \"replace_url\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Authorization {
    #[doc = "Website to visit to sign up for an account."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_url: Option<String>,
    #[doc = "When type=query_param, this specifies the name of the query parameter."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub param_name: Option<String>,
    #[doc = "Authorization approach: HTTP header, basic authentication, query parameter, or path segment in a Transitland Extended URL."]
    #[serde(rename = "type")]
    pub type_: AuthorizationType,
}
impl From<&Authorization> for Authorization {
    fn from(value: &Authorization) -> Self {
        value.clone()
    }
}
impl Authorization {
    pub fn builder() -> builder::Authorization {
        Default::default()
    }
}
#[doc = "Authorization approach: HTTP header, basic authentication, query parameter, or path segment in a Transitland Extended URL."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Authorization approach: HTTP header, basic authentication, query parameter, or path segment in a Transitland Extended URL.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"header\","]
#[doc = "    \"basic_auth\","]
#[doc = "    \"query_param\","]
#[doc = "    \"path_segment\","]
#[doc = "    \"replace_url\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum AuthorizationType {
    #[serde(rename = "header")]
    Header,
    #[serde(rename = "basic_auth")]
    BasicAuth,
    #[serde(rename = "query_param")]
    QueryParam,
    #[serde(rename = "path_segment")]
    PathSegment,
    #[serde(rename = "replace_url")]
    ReplaceUrl,
}
impl From<&AuthorizationType> for AuthorizationType {
    fn from(value: &AuthorizationType) -> Self {
        value.clone()
    }
}
impl ToString for AuthorizationType {
    fn to_string(&self) -> String {
        match *self {
            Self::Header => "header".to_string(),
            Self::BasicAuth => "basic_auth".to_string(),
            Self::QueryParam => "query_param".to_string(),
            Self::PathSegment => "path_segment".to_string(),
            Self::ReplaceUrl => "replace_url".to_string(),
        }
    }
}
impl std::str::FromStr for AuthorizationType {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "header" => Ok(Self::Header),
            "basic_auth" => Ok(Self::BasicAuth),
            "query_param" => Ok(Self::QueryParam),
            "path_segment" => Ok(Self::PathSegment),
            "replace_url" => Ok(Self::ReplaceUrl),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for AuthorizationType {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for AuthorizationType {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for AuthorizationType {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "DistributedMobilityFeedRegistry"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"$id\": \"https://dmfr.transit.land/json-schema/dmfr.schema-v0.5.1.json\","]
#[doc = "  \"title\": \"Distributed Mobility Feed Registry\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"properties\": {"]
#[doc = "    \"feeds\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/feed\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"license_spdx_identifier\": {"]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"anyOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/spdxLicenseIds\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"operators\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/operator\""]
#[doc = "      }"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DistributedMobilityFeedRegistry {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feeds: Vec<Feed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license_spdx_identifier: Option<SpdxLicenseIds>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<Operator>,
}
impl From<&DistributedMobilityFeedRegistry> for DistributedMobilityFeedRegistry {
    fn from(value: &DistributedMobilityFeedRegistry) -> Self {
        value.clone()
    }
}
impl DistributedMobilityFeedRegistry {
    pub fn builder() -> builder::DistributedMobilityFeedRegistry {
        Default::default()
    }
}
#[doc = "Feed"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"id\","]
#[doc = "    \"spec\","]
#[doc = "    \"urls\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"authorization\": {"]
#[doc = "      \"$ref\": \"#/definitions/authorization\""]
#[doc = "    },"]
#[doc = "    \"description\": {"]
#[doc = "      \"description\": \"Optional text providing notes about the feed. May be shown to end-users. May be rendered as Markdown by some consuming applications.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"id\": {"]
#[doc = "      \"description\": \"Identifier for this feed, internal to this DMFR instance. (Optionally can be a Onestop ID.)\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"languages\": {"]
#[doc = "      \"description\": \"Language(s) included in this feed.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/language\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"license\": {"]
#[doc = "      \"$ref\": \"#/definitions/license_description\""]
#[doc = "    },"]
#[doc = "    \"name\": {"]
#[doc = "      \"description\": \"An optional name to describe the feed. May be shown to end-users.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"operators\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/operator\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"spec\": {"]
#[doc = "      \"description\": \"Type of data contained in this feed: GTFS, GTFS-RT, GBFS, or MDS.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"gtfs\","]
#[doc = "        \"gtfs-rt\","]
#[doc = "        \"gbfs\","]
#[doc = "        \"mds\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"supersedes_ids\": {"]
#[doc = "      \"description\": \"One or more Onestop IDs for old feeds records that have since been merged into or taken over by this feed record.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"type\": \"string\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"tags\": {"]
#[doc = "      \"description\": \"Extended information about the feed, as well as fetch and import controls for the Transitland platform.\","]
#[doc = "      \"type\": \"object\""]
#[doc = "    },"]
#[doc = "    \"urls\": {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"properties\": {"]
#[doc = "        \"gbfs_auto_discovery\": {"]
#[doc = "          \"description\": \"Auto-discovery file in JSON format that links to all of the other GBFS files published by the system.\","]
#[doc = "          \"type\": \"string\","]
#[doc = "          \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "        },"]
#[doc = "        \"mds_provider\": {"]
#[doc = "          \"description\": \"MDS provider API endpoints are intended to be implemented by mobility providers and consumed by regulatory agencies.\","]
#[doc = "          \"type\": \"string\","]
#[doc = "          \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "        },"]
#[doc = "        \"realtime_alerts\": {"]
#[doc = "          \"description\": \"URL for GTFS Realtime Alert messages.\","]
#[doc = "          \"type\": \"string\","]
#[doc = "          \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "        },"]
#[doc = "        \"realtime_trip_updates\": {"]
#[doc = "          \"description\": \"URL for GTFS Realtime TripUpdate messages.\","]
#[doc = "          \"type\": \"string\","]
#[doc = "          \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "        },"]
#[doc = "        \"realtime_vehicle_positions\": {"]
#[doc = "          \"description\": \"URL for GTFS Realtime VehiclePosition messages.\","]
#[doc = "          \"type\": \"string\","]
#[doc = "          \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "        },"]
#[doc = "        \"static_current\": {"]
#[doc = "          \"description\": \"URL (in Transitland Extended URL format) for the static feed that represents today's service. (Has the same meaning as url.)\","]
#[doc = "          \"type\": \"string\","]
#[doc = "          \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "        },"]
#[doc = "        \"static_historic\": {"]
#[doc = "          \"type\": \"array\","]
#[doc = "          \"items\": {"]
#[doc = "            \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent past service that is no longer in effect.\","]
#[doc = "            \"type\": \"string\","]
#[doc = "            \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "          }"]
#[doc = "        },"]
#[doc = "        \"static_hypothetical\": {"]
#[doc = "          \"type\": \"array\","]
#[doc = "          \"items\": {"]
#[doc = "            \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent potential service or network changes. Typically used to represent scenarios that may (or may not) take effect months or years in the future.\","]
#[doc = "            \"type\": \"string\","]
#[doc = "            \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "          }"]
#[doc = "        },"]
#[doc = "        \"static_planned\": {"]
#[doc = "          \"type\": \"array\","]
#[doc = "          \"items\": {"]
#[doc = "            \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent service planned for upcoming dates. Typically used to represent calendar/service changes that will take effect few weeks or months in the future.\","]
#[doc = "            \"type\": \"string\","]
#[doc = "            \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      }"]
#[doc = "    }"]
#[doc = "  },"]
#[doc = "  \"additionalProperties\": false"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Feed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<Authorization>,
    #[doc = "Optional text providing notes about the feed. May be shown to end-users. May be rendered as Markdown by some consuming applications."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[doc = "Identifier for this feed, internal to this DMFR instance. (Optionally can be a Onestop ID.)"]
    pub id: String,
    #[doc = "Language(s) included in this feed."]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<Language>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<LicenseDescription>,
    #[doc = "An optional name to describe the feed. May be shown to end-users."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<Operator>,
    #[doc = "Type of data contained in this feed: GTFS, GTFS-RT, GBFS, or MDS."]
    pub spec: FeedSpec,
    #[doc = "One or more Onestop IDs for old feeds records that have since been merged into or taken over by this feed record."]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes_ids: Vec<String>,
    #[doc = "Extended information about the feed, as well as fetch and import controls for the Transitland platform."]
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub tags: serde_json::Map<String, serde_json::Value>,
    pub urls: FeedUrls,
}
impl From<&Feed> for Feed {
    fn from(value: &Feed) -> Self {
        value.clone()
    }
}
impl Feed {
    pub fn builder() -> builder::Feed {
        Default::default()
    }
}
#[doc = "Type of data contained in this feed: GTFS, GTFS-RT, GBFS, or MDS."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Type of data contained in this feed: GTFS, GTFS-RT, GBFS, or MDS.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"gtfs\","]
#[doc = "    \"gtfs-rt\","]
#[doc = "    \"gbfs\","]
#[doc = "    \"mds\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum FeedSpec {
    #[serde(rename = "gtfs")]
    Gtfs,
    #[serde(rename = "gtfs-rt")]
    GtfsRt,
    #[serde(rename = "gbfs")]
    Gbfs,
    #[serde(rename = "mds")]
    Mds,
}
impl From<&FeedSpec> for FeedSpec {
    fn from(value: &FeedSpec) -> Self {
        value.clone()
    }
}
impl ToString for FeedSpec {
    fn to_string(&self) -> String {
        match *self {
            Self::Gtfs => "gtfs".to_string(),
            Self::GtfsRt => "gtfs-rt".to_string(),
            Self::Gbfs => "gbfs".to_string(),
            Self::Mds => "mds".to_string(),
        }
    }
}
impl std::str::FromStr for FeedSpec {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "gtfs" => Ok(Self::Gtfs),
            "gtfs-rt" => Ok(Self::GtfsRt),
            "gbfs" => Ok(Self::Gbfs),
            "mds" => Ok(Self::Mds),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for FeedSpec {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedSpec {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedSpec {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "FeedUrls"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"properties\": {"]
#[doc = "    \"gbfs_auto_discovery\": {"]
#[doc = "      \"description\": \"Auto-discovery file in JSON format that links to all of the other GBFS files published by the system.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "    },"]
#[doc = "    \"mds_provider\": {"]
#[doc = "      \"description\": \"MDS provider API endpoints are intended to be implemented by mobility providers and consumed by regulatory agencies.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "    },"]
#[doc = "    \"realtime_alerts\": {"]
#[doc = "      \"description\": \"URL for GTFS Realtime Alert messages.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "    },"]
#[doc = "    \"realtime_trip_updates\": {"]
#[doc = "      \"description\": \"URL for GTFS Realtime TripUpdate messages.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "    },"]
#[doc = "    \"realtime_vehicle_positions\": {"]
#[doc = "      \"description\": \"URL for GTFS Realtime VehiclePosition messages.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "    },"]
#[doc = "    \"static_current\": {"]
#[doc = "      \"description\": \"URL (in Transitland Extended URL format) for the static feed that represents today's service. (Has the same meaning as url.)\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "    },"]
#[doc = "    \"static_historic\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent past service that is no longer in effect.\","]
#[doc = "        \"type\": \"string\","]
#[doc = "        \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"static_hypothetical\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent potential service or network changes. Typically used to represent scenarios that may (or may not) take effect months or years in the future.\","]
#[doc = "        \"type\": \"string\","]
#[doc = "        \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"static_planned\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent service planned for upcoming dates. Typically used to represent calendar/service changes that will take effect few weeks or months in the future.\","]
#[doc = "        \"type\": \"string\","]
#[doc = "        \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "      }"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FeedUrls {
    #[doc = "Auto-discovery file in JSON format that links to all of the other GBFS files published by the system."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gbfs_auto_discovery: Option<FeedUrlsGbfsAutoDiscovery>,
    #[doc = "MDS provider API endpoints are intended to be implemented by mobility providers and consumed by regulatory agencies."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mds_provider: Option<FeedUrlsMdsProvider>,
    #[doc = "URL for GTFS Realtime Alert messages."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realtime_alerts: Option<FeedUrlsRealtimeAlerts>,
    #[doc = "URL for GTFS Realtime TripUpdate messages."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realtime_trip_updates: Option<FeedUrlsRealtimeTripUpdates>,
    #[doc = "URL for GTFS Realtime VehiclePosition messages."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realtime_vehicle_positions: Option<FeedUrlsRealtimeVehiclePositions>,
    #[doc = "URL (in Transitland Extended URL format) for the static feed that represents today's service. (Has the same meaning as url.)"]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub static_current: Option<FeedUrlsStaticCurrent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub static_historic: Vec<FeedUrlsStaticHistoricItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub static_hypothetical: Vec<FeedUrlsStaticHypotheticalItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub static_planned: Vec<FeedUrlsStaticPlannedItem>,
}
impl From<&FeedUrls> for FeedUrls {
    fn from(value: &FeedUrls) -> Self {
        value.clone()
    }
}
impl FeedUrls {
    pub fn builder() -> builder::FeedUrls {
        Default::default()
    }
}
#[doc = "Auto-discovery file in JSON format that links to all of the other GBFS files published by the system."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Auto-discovery file in JSON format that links to all of the other GBFS files published by the system.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsGbfsAutoDiscovery(String);
impl std::ops::Deref for FeedUrlsGbfsAutoDiscovery {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsGbfsAutoDiscovery> for String {
    fn from(value: FeedUrlsGbfsAutoDiscovery) -> Self {
        value.0
    }
}
impl From<&FeedUrlsGbfsAutoDiscovery> for FeedUrlsGbfsAutoDiscovery {
    fn from(value: &FeedUrlsGbfsAutoDiscovery) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsGbfsAutoDiscovery {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsGbfsAutoDiscovery {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsGbfsAutoDiscovery {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsGbfsAutoDiscovery {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsGbfsAutoDiscovery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "MDS provider API endpoints are intended to be implemented by mobility providers and consumed by regulatory agencies."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"MDS provider API endpoints are intended to be implemented by mobility providers and consumed by regulatory agencies.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsMdsProvider(String);
impl std::ops::Deref for FeedUrlsMdsProvider {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsMdsProvider> for String {
    fn from(value: FeedUrlsMdsProvider) -> Self {
        value.0
    }
}
impl From<&FeedUrlsMdsProvider> for FeedUrlsMdsProvider {
    fn from(value: &FeedUrlsMdsProvider) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsMdsProvider {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsMdsProvider {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsMdsProvider {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsMdsProvider {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsMdsProvider {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "URL for GTFS Realtime Alert messages."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URL for GTFS Realtime Alert messages.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsRealtimeAlerts(String);
impl std::ops::Deref for FeedUrlsRealtimeAlerts {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsRealtimeAlerts> for String {
    fn from(value: FeedUrlsRealtimeAlerts) -> Self {
        value.0
    }
}
impl From<&FeedUrlsRealtimeAlerts> for FeedUrlsRealtimeAlerts {
    fn from(value: &FeedUrlsRealtimeAlerts) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsRealtimeAlerts {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsRealtimeAlerts {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsRealtimeAlerts {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsRealtimeAlerts {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsRealtimeAlerts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "URL for GTFS Realtime TripUpdate messages."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URL for GTFS Realtime TripUpdate messages.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsRealtimeTripUpdates(String);
impl std::ops::Deref for FeedUrlsRealtimeTripUpdates {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsRealtimeTripUpdates> for String {
    fn from(value: FeedUrlsRealtimeTripUpdates) -> Self {
        value.0
    }
}
impl From<&FeedUrlsRealtimeTripUpdates> for FeedUrlsRealtimeTripUpdates {
    fn from(value: &FeedUrlsRealtimeTripUpdates) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsRealtimeTripUpdates {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsRealtimeTripUpdates {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsRealtimeTripUpdates {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsRealtimeTripUpdates {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsRealtimeTripUpdates {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "URL for GTFS Realtime VehiclePosition messages."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URL for GTFS Realtime VehiclePosition messages.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsRealtimeVehiclePositions(String);
impl std::ops::Deref for FeedUrlsRealtimeVehiclePositions {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsRealtimeVehiclePositions> for String {
    fn from(value: FeedUrlsRealtimeVehiclePositions) -> Self {
        value.0
    }
}
impl From<&FeedUrlsRealtimeVehiclePositions> for FeedUrlsRealtimeVehiclePositions {
    fn from(value: &FeedUrlsRealtimeVehiclePositions) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsRealtimeVehiclePositions {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsRealtimeVehiclePositions {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsRealtimeVehiclePositions {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsRealtimeVehiclePositions {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsRealtimeVehiclePositions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "URL (in Transitland Extended URL format) for the static feed that represents today's service. (Has the same meaning as url.)"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URL (in Transitland Extended URL format) for the static feed that represents today's service. (Has the same meaning as url.)\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsStaticCurrent(String);
impl std::ops::Deref for FeedUrlsStaticCurrent {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsStaticCurrent> for String {
    fn from(value: FeedUrlsStaticCurrent) -> Self {
        value.0
    }
}
impl From<&FeedUrlsStaticCurrent> for FeedUrlsStaticCurrent {
    fn from(value: &FeedUrlsStaticCurrent) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsStaticCurrent {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsStaticCurrent {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsStaticCurrent {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsStaticCurrent {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsStaticCurrent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "URLs (in Transitland Extended URL format) for static feeds that represent past service that is no longer in effect."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent past service that is no longer in effect.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsStaticHistoricItem(String);
impl std::ops::Deref for FeedUrlsStaticHistoricItem {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsStaticHistoricItem> for String {
    fn from(value: FeedUrlsStaticHistoricItem) -> Self {
        value.0
    }
}
impl From<&FeedUrlsStaticHistoricItem> for FeedUrlsStaticHistoricItem {
    fn from(value: &FeedUrlsStaticHistoricItem) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsStaticHistoricItem {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsStaticHistoricItem {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsStaticHistoricItem {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsStaticHistoricItem {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsStaticHistoricItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "URLs (in Transitland Extended URL format) for static feeds that represent potential service or network changes. Typically used to represent scenarios that may (or may not) take effect months or years in the future."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent potential service or network changes. Typically used to represent scenarios that may (or may not) take effect months or years in the future.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsStaticHypotheticalItem(String);
impl std::ops::Deref for FeedUrlsStaticHypotheticalItem {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsStaticHypotheticalItem> for String {
    fn from(value: FeedUrlsStaticHypotheticalItem) -> Self {
        value.0
    }
}
impl From<&FeedUrlsStaticHypotheticalItem> for FeedUrlsStaticHypotheticalItem {
    fn from(value: &FeedUrlsStaticHypotheticalItem) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsStaticHypotheticalItem {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsStaticHypotheticalItem {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsStaticHypotheticalItem {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsStaticHypotheticalItem {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsStaticHypotheticalItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "URLs (in Transitland Extended URL format) for static feeds that represent service planned for upcoming dates. Typically used to represent calendar/service changes that will take effect few weeks or months in the future."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URLs (in Transitland Extended URL format) for static feeds that represent service planned for upcoming dates. Typically used to represent calendar/service changes that will take effect few weeks or months in the future.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FeedUrlsStaticPlannedItem(String);
impl std::ops::Deref for FeedUrlsStaticPlannedItem {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<FeedUrlsStaticPlannedItem> for String {
    fn from(value: FeedUrlsStaticPlannedItem) -> Self {
        value.0
    }
}
impl From<&FeedUrlsStaticPlannedItem> for FeedUrlsStaticPlannedItem {
    fn from(value: &FeedUrlsStaticPlannedItem) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for FeedUrlsStaticPlannedItem {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for FeedUrlsStaticPlannedItem {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for FeedUrlsStaticPlannedItem {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for FeedUrlsStaticPlannedItem {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for FeedUrlsStaticPlannedItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "A language specified using an IETF language tag."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"A language specified using an IETF language tag.\","]
#[doc = "  \"type\": \"string\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Language(pub String);
impl std::ops::Deref for Language {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<Language> for String {
    fn from(value: Language) -> Self {
        value.0
    }
}
impl From<&Language> for Language {
    fn from(value: &Language) -> Self {
        value.clone()
    }
}
impl From<String> for Language {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl std::str::FromStr for Language {
    type Err = std::convert::Infallible;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}
impl ToString for Language {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}
#[doc = "LicenseDescription"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"properties\": {"]
#[doc = "    \"attribution_instructions\": {"]
#[doc = "      \"description\": \"Feed consumers must follow these instructions for how to provide attribution.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"attribution_text\": {"]
#[doc = "      \"description\": \"Feed consumers must include this particular text when using this feed.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"commercial_use_allowed\": {"]
#[doc = "      \"description\": \"Are feed consumers allowed to use the feed for commercial purposes?\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"yes\","]
#[doc = "        \"no\","]
#[doc = "        \"unknown\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"create_derived_product\": {"]
#[doc = "      \"description\": \"Are feed consumers allowed to create and share derived products from the feed?\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"yes\","]
#[doc = "        \"no\","]
#[doc = "        \"unknown\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"redistribution_allowed\": {"]
#[doc = "      \"description\": \"Are feed consumers allowed to redistribute the feed in its entirety?\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"yes\","]
#[doc = "        \"no\","]
#[doc = "        \"unknown\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"share_alike_optional\": {"]
#[doc = "      \"description\": \"Are feed consumers allowed to keep their modifications of this feed private?\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"yes\","]
#[doc = "        \"no\","]
#[doc = "        \"unknown\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"spdx_identifier\": {"]
#[doc = "      \"description\": \"SPDX identifier for a common license. See https://spdx.org/licenses/\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"anyOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/spdxLicenseIds\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"url\": {"]
#[doc = "      \"description\": \"URL for a custom license.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"use_without_attribution\": {"]
#[doc = "      \"description\": \"Are feed consumers allowed to use the feed contents without including attribution text in their app or map?\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"yes\","]
#[doc = "        \"no\","]
#[doc = "        \"unknown\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LicenseDescription {
    #[doc = "Feed consumers must follow these instructions for how to provide attribution."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution_instructions: Option<String>,
    #[doc = "Feed consumers must include this particular text when using this feed."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution_text: Option<String>,
    #[doc = "Are feed consumers allowed to use the feed for commercial purposes?"]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commercial_use_allowed: Option<LicenseDescriptionCommercialUseAllowed>,
    #[doc = "Are feed consumers allowed to create and share derived products from the feed?"]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_derived_product: Option<LicenseDescriptionCreateDerivedProduct>,
    #[doc = "Are feed consumers allowed to redistribute the feed in its entirety?"]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redistribution_allowed: Option<LicenseDescriptionRedistributionAllowed>,
    #[doc = "Are feed consumers allowed to keep their modifications of this feed private?"]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share_alike_optional: Option<LicenseDescriptionShareAlikeOptional>,
    #[doc = "SPDX identifier for a common license. See https://spdx.org/licenses/"]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spdx_identifier: Option<SpdxLicenseIds>,
    #[doc = "URL for a custom license."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[doc = "Are feed consumers allowed to use the feed contents without including attribution text in their app or map?"]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_without_attribution: Option<LicenseDescriptionUseWithoutAttribution>,
}
impl From<&LicenseDescription> for LicenseDescription {
    fn from(value: &LicenseDescription) -> Self {
        value.clone()
    }
}
impl LicenseDescription {
    pub fn builder() -> builder::LicenseDescription {
        Default::default()
    }
}
#[doc = "Are feed consumers allowed to use the feed for commercial purposes?"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Are feed consumers allowed to use the feed for commercial purposes?\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"yes\","]
#[doc = "    \"no\","]
#[doc = "    \"unknown\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum LicenseDescriptionCommercialUseAllowed {
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
}
impl From<&LicenseDescriptionCommercialUseAllowed> for LicenseDescriptionCommercialUseAllowed {
    fn from(value: &LicenseDescriptionCommercialUseAllowed) -> Self {
        value.clone()
    }
}
impl ToString for LicenseDescriptionCommercialUseAllowed {
    fn to_string(&self) -> String {
        match *self {
            Self::Yes => "yes".to_string(),
            Self::No => "no".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
impl std::str::FromStr for LicenseDescriptionCommercialUseAllowed {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "yes" => Ok(Self::Yes),
            "no" => Ok(Self::No),
            "unknown" => Ok(Self::Unknown),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for LicenseDescriptionCommercialUseAllowed {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for LicenseDescriptionCommercialUseAllowed {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for LicenseDescriptionCommercialUseAllowed {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "Are feed consumers allowed to create and share derived products from the feed?"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Are feed consumers allowed to create and share derived products from the feed?\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"yes\","]
#[doc = "    \"no\","]
#[doc = "    \"unknown\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum LicenseDescriptionCreateDerivedProduct {
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
}
impl From<&LicenseDescriptionCreateDerivedProduct> for LicenseDescriptionCreateDerivedProduct {
    fn from(value: &LicenseDescriptionCreateDerivedProduct) -> Self {
        value.clone()
    }
}
impl ToString for LicenseDescriptionCreateDerivedProduct {
    fn to_string(&self) -> String {
        match *self {
            Self::Yes => "yes".to_string(),
            Self::No => "no".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
impl std::str::FromStr for LicenseDescriptionCreateDerivedProduct {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "yes" => Ok(Self::Yes),
            "no" => Ok(Self::No),
            "unknown" => Ok(Self::Unknown),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for LicenseDescriptionCreateDerivedProduct {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for LicenseDescriptionCreateDerivedProduct {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for LicenseDescriptionCreateDerivedProduct {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "Are feed consumers allowed to redistribute the feed in its entirety?"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Are feed consumers allowed to redistribute the feed in its entirety?\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"yes\","]
#[doc = "    \"no\","]
#[doc = "    \"unknown\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum LicenseDescriptionRedistributionAllowed {
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
}
impl From<&LicenseDescriptionRedistributionAllowed> for LicenseDescriptionRedistributionAllowed {
    fn from(value: &LicenseDescriptionRedistributionAllowed) -> Self {
        value.clone()
    }
}
impl ToString for LicenseDescriptionRedistributionAllowed {
    fn to_string(&self) -> String {
        match *self {
            Self::Yes => "yes".to_string(),
            Self::No => "no".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
impl std::str::FromStr for LicenseDescriptionRedistributionAllowed {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "yes" => Ok(Self::Yes),
            "no" => Ok(Self::No),
            "unknown" => Ok(Self::Unknown),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for LicenseDescriptionRedistributionAllowed {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for LicenseDescriptionRedistributionAllowed {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for LicenseDescriptionRedistributionAllowed {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "Are feed consumers allowed to keep their modifications of this feed private?"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Are feed consumers allowed to keep their modifications of this feed private?\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"yes\","]
#[doc = "    \"no\","]
#[doc = "    \"unknown\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum LicenseDescriptionShareAlikeOptional {
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
}
impl From<&LicenseDescriptionShareAlikeOptional> for LicenseDescriptionShareAlikeOptional {
    fn from(value: &LicenseDescriptionShareAlikeOptional) -> Self {
        value.clone()
    }
}
impl ToString for LicenseDescriptionShareAlikeOptional {
    fn to_string(&self) -> String {
        match *self {
            Self::Yes => "yes".to_string(),
            Self::No => "no".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
impl std::str::FromStr for LicenseDescriptionShareAlikeOptional {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "yes" => Ok(Self::Yes),
            "no" => Ok(Self::No),
            "unknown" => Ok(Self::Unknown),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for LicenseDescriptionShareAlikeOptional {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for LicenseDescriptionShareAlikeOptional {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for LicenseDescriptionShareAlikeOptional {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "Are feed consumers allowed to use the feed contents without including attribution text in their app or map?"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Are feed consumers allowed to use the feed contents without including attribution text in their app or map?\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"yes\","]
#[doc = "    \"no\","]
#[doc = "    \"unknown\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum LicenseDescriptionUseWithoutAttribution {
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
}
impl From<&LicenseDescriptionUseWithoutAttribution> for LicenseDescriptionUseWithoutAttribution {
    fn from(value: &LicenseDescriptionUseWithoutAttribution) -> Self {
        value.clone()
    }
}
impl ToString for LicenseDescriptionUseWithoutAttribution {
    fn to_string(&self) -> String {
        match *self {
            Self::Yes => "yes".to_string(),
            Self::No => "no".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
impl std::str::FromStr for LicenseDescriptionUseWithoutAttribution {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "yes" => Ok(Self::Yes),
            "no" => Ok(Self::No),
            "unknown" => Ok(Self::Unknown),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for LicenseDescriptionUseWithoutAttribution {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for LicenseDescriptionUseWithoutAttribution {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for LicenseDescriptionUseWithoutAttribution {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "Operator"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"name\","]
#[doc = "    \"onestop_id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"associated_feeds\": {"]
#[doc = "      \"description\": \"Define associations between an operator and one or more feeds. If this operator is defined underneath a feed, it is not necessary to include a feed_onestop_id. In all cases, it is only necessary to specify a gtfs_agency_id when a feed includes more than one agency; Transitland will auto-detect the agency_id if the feed includes only one feed.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"type\": \"object\","]
#[doc = "        \"properties\": {"]
#[doc = "          \"feed_onestop_id\": {},"]
#[doc = "          \"gtfs_agency_id\": {"]
#[doc = "            \"description\": \"ID from the \","]
#[doc = "            \"type\": \"string\""]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"name\": {"]
#[doc = "      \"description\": \"Full name of the operator. If there is an abbreviation or acronym for the operator, also define a short_name.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"onestop_id\": {"]
#[doc = "      \"description\": \"The globally unique Onestop ID for this operator.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"short_name\": {"]
#[doc = "      \"description\": \"Abbreviation, acronym, or secondary name of the operator.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"supersedes_ids\": {"]
#[doc = "      \"description\": \"One or more Onestop IDs for old operator records that have since been merged into or taken over by this operator record.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"type\": \"string\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"tags\": {"]
#[doc = "      \"description\": \"Extended information about the operator, including identifiers for this operator in other datasets.\","]
#[doc = "      \"type\": \"object\""]
#[doc = "    },"]
#[doc = "    \"website\": {"]
#[doc = "      \"description\": \"URL for the operator's public website.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "    }"]
#[doc = "  },"]
#[doc = "  \"additionalProperties\": false"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Operator {
    #[doc = "Define associations between an operator and one or more feeds. If this operator is defined underneath a feed, it is not necessary to include a feed_onestop_id. In all cases, it is only necessary to specify a gtfs_agency_id when a feed includes more than one agency; Transitland will auto-detect the agency_id if the feed includes only one feed."]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub associated_feeds: Vec<OperatorAssociatedFeedsItem>,
    #[doc = "Full name of the operator. If there is an abbreviation or acronym for the operator, also define a short_name."]
    pub name: String,
    #[doc = "The globally unique Onestop ID for this operator."]
    pub onestop_id: String,
    #[doc = "Abbreviation, acronym, or secondary name of the operator."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_name: Option<String>,
    #[doc = "One or more Onestop IDs for old operator records that have since been merged into or taken over by this operator record."]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes_ids: Vec<String>,
    #[doc = "Extended information about the operator, including identifiers for this operator in other datasets."]
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub tags: serde_json::Map<String, serde_json::Value>,
    #[doc = "URL for the operator's public website."]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub website: Option<OperatorWebsite>,
}
impl From<&Operator> for Operator {
    fn from(value: &Operator) -> Self {
        value.clone()
    }
}
impl Operator {
    pub fn builder() -> builder::Operator {
        Default::default()
    }
}
#[doc = "OperatorAssociatedFeedsItem"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"properties\": {"]
#[doc = "    \"feed_onestop_id\": {},"]
#[doc = "    \"gtfs_agency_id\": {"]
#[doc = "      \"description\": \"ID from the \","]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OperatorAssociatedFeedsItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_onestop_id: Option<serde_json::Value>,
    #[doc = "ID from the "]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gtfs_agency_id: Option<String>,
}
impl From<&OperatorAssociatedFeedsItem> for OperatorAssociatedFeedsItem {
    fn from(value: &OperatorAssociatedFeedsItem) -> Self {
        value.clone()
    }
}
impl OperatorAssociatedFeedsItem {
    pub fn builder() -> builder::OperatorAssociatedFeedsItem {
        Default::default()
    }
}
#[doc = "URL for the operator's public website."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"URL for the operator's public website.\","]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"pattern\": \"^(http|https|ftp):\\\\/\\\\/[\\\\p{L}\\\\p{N}.,~#{}():&\\\\/%='?_/-]+$\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OperatorWebsite(String);
impl std::ops::Deref for OperatorWebsite {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<OperatorWebsite> for String {
    fn from(value: OperatorWebsite) -> Self {
        value.0
    }
}
impl From<&OperatorWebsite> for OperatorWebsite {
    fn from(value: &OperatorWebsite) -> Self {
        value.clone()
    }
}
impl std::str::FromStr for OperatorWebsite {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        if regress::Regex::new("^(http|https|ftp):\\/\\/.+$")
            .unwrap()
            .find(value)
            .is_none()
        {
            return Err("doesn't match pattern \"^(http|https|ftp):\\/\\/.+$\"".into());
        }
        Ok(Self(value.to_string()))
    }
}
impl std::convert::TryFrom<&str> for OperatorWebsite {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for OperatorWebsite {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for OperatorWebsite {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl<'de> serde::Deserialize<'de> for OperatorWebsite {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|e: self::error::ConversionError| {
                <D::Error as serde::de::Error>::custom(e.to_string())
            })
    }
}
#[doc = "List of SPDX short-form identifiers. To update: http https://raw.githubusercontent.com/spdx/license-list-data/main/json/licenses.json | jq '.licenses[] .licenseId'"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"List of SPDX short-form identifiers. To update: http https://raw.githubusercontent.com/spdx/license-list-data/main/json/licenses.json | jq '.licenses[] .licenseId'\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"0BSD\","]
#[doc = "    \"AAL\","]
#[doc = "    \"Abstyles\","]
#[doc = "    \"AdaCore-doc\","]
#[doc = "    \"Adobe-2006\","]
#[doc = "    \"Adobe-Glyph\","]
#[doc = "    \"ADSL\","]
#[doc = "    \"AFL-1.1\","]
#[doc = "    \"AFL-1.2\","]
#[doc = "    \"AFL-2.0\","]
#[doc = "    \"AFL-2.1\","]
#[doc = "    \"AFL-3.0\","]
#[doc = "    \"Afmparse\","]
#[doc = "    \"AGPL-1.0\","]
#[doc = "    \"AGPL-1.0-only\","]
#[doc = "    \"AGPL-1.0-or-later\","]
#[doc = "    \"AGPL-3.0\","]
#[doc = "    \"AGPL-3.0-only\","]
#[doc = "    \"AGPL-3.0-or-later\","]
#[doc = "    \"Aladdin\","]
#[doc = "    \"AMDPLPA\","]
#[doc = "    \"AML\","]
#[doc = "    \"AMPAS\","]
#[doc = "    \"ANTLR-PD\","]
#[doc = "    \"ANTLR-PD-fallback\","]
#[doc = "    \"Apache-1.0\","]
#[doc = "    \"Apache-1.1\","]
#[doc = "    \"Apache-2.0\","]
#[doc = "    \"APAFML\","]
#[doc = "    \"APL-1.0\","]
#[doc = "    \"App-s2p\","]
#[doc = "    \"APSL-1.0\","]
#[doc = "    \"APSL-1.1\","]
#[doc = "    \"APSL-1.2\","]
#[doc = "    \"APSL-2.0\","]
#[doc = "    \"Arphic-1999\","]
#[doc = "    \"Artistic-1.0\","]
#[doc = "    \"Artistic-1.0-cl8\","]
#[doc = "    \"Artistic-1.0-Perl\","]
#[doc = "    \"Artistic-2.0\","]
#[doc = "    \"ASWF-Digital-Assets-1.0\","]
#[doc = "    \"ASWF-Digital-Assets-1.1\","]
#[doc = "    \"Baekmuk\","]
#[doc = "    \"Bahyph\","]
#[doc = "    \"Barr\","]
#[doc = "    \"Beerware\","]
#[doc = "    \"Bitstream-Charter\","]
#[doc = "    \"Bitstream-Vera\","]
#[doc = "    \"BitTorrent-1.0\","]
#[doc = "    \"BitTorrent-1.1\","]
#[doc = "    \"blessing\","]
#[doc = "    \"BlueOak-1.0.0\","]
#[doc = "    \"Boehm-GC\","]
#[doc = "    \"Borceux\","]
#[doc = "    \"Brian-Gladman-3-Clause\","]
#[doc = "    \"BSD-1-Clause\","]
#[doc = "    \"BSD-2-Clause\","]
#[doc = "    \"BSD-2-Clause-FreeBSD\","]
#[doc = "    \"BSD-2-Clause-NetBSD\","]
#[doc = "    \"BSD-2-Clause-Patent\","]
#[doc = "    \"BSD-2-Clause-Views\","]
#[doc = "    \"BSD-3-Clause\","]
#[doc = "    \"BSD-3-Clause-Attribution\","]
#[doc = "    \"BSD-3-Clause-Clear\","]
#[doc = "    \"BSD-3-Clause-LBNL\","]
#[doc = "    \"BSD-3-Clause-Modification\","]
#[doc = "    \"BSD-3-Clause-No-Military-License\","]
#[doc = "    \"BSD-3-Clause-No-Nuclear-License\","]
#[doc = "    \"BSD-3-Clause-No-Nuclear-License-2014\","]
#[doc = "    \"BSD-3-Clause-No-Nuclear-Warranty\","]
#[doc = "    \"BSD-3-Clause-Open-MPI\","]
#[doc = "    \"BSD-4-Clause\","]
#[doc = "    \"BSD-4-Clause-Shortened\","]
#[doc = "    \"BSD-4-Clause-UC\","]
#[doc = "    \"BSD-4.3RENO\","]
#[doc = "    \"BSD-4.3TAHOE\","]
#[doc = "    \"BSD-Advertising-Acknowledgement\","]
#[doc = "    \"BSD-Attribution-HPND-disclaimer\","]
#[doc = "    \"BSD-Protection\","]
#[doc = "    \"BSD-Source-Code\","]
#[doc = "    \"BSL-1.0\","]
#[doc = "    \"BUSL-1.1\","]
#[doc = "    \"bzip2-1.0.5\","]
#[doc = "    \"bzip2-1.0.6\","]
#[doc = "    \"C-UDA-1.0\","]
#[doc = "    \"CAL-1.0\","]
#[doc = "    \"CAL-1.0-Combined-Work-Exception\","]
#[doc = "    \"Caldera\","]
#[doc = "    \"CATOSL-1.1\","]
#[doc = "    \"CC-BY-1.0\","]
#[doc = "    \"CC-BY-2.0\","]
#[doc = "    \"CC-BY-2.5\","]
#[doc = "    \"CC-BY-2.5-AU\","]
#[doc = "    \"CC-BY-3.0\","]
#[doc = "    \"CC-BY-3.0-AT\","]
#[doc = "    \"CC-BY-3.0-DE\","]
#[doc = "    \"CC-BY-3.0-IGO\","]
#[doc = "    \"CC-BY-3.0-NL\","]
#[doc = "    \"CC-BY-3.0-US\","]
#[doc = "    \"CC-BY-4.0\","]
#[doc = "    \"CC-BY-NC-1.0\","]
#[doc = "    \"CC-BY-NC-2.0\","]
#[doc = "    \"CC-BY-NC-2.5\","]
#[doc = "    \"CC-BY-NC-3.0\","]
#[doc = "    \"CC-BY-NC-3.0-DE\","]
#[doc = "    \"CC-BY-NC-4.0\","]
#[doc = "    \"CC-BY-NC-ND-1.0\","]
#[doc = "    \"CC-BY-NC-ND-2.0\","]
#[doc = "    \"CC-BY-NC-ND-2.5\","]
#[doc = "    \"CC-BY-NC-ND-3.0\","]
#[doc = "    \"CC-BY-NC-ND-3.0-DE\","]
#[doc = "    \"CC-BY-NC-ND-3.0-IGO\","]
#[doc = "    \"CC-BY-NC-ND-4.0\","]
#[doc = "    \"CC-BY-NC-SA-1.0\","]
#[doc = "    \"CC-BY-NC-SA-2.0\","]
#[doc = "    \"CC-BY-NC-SA-2.0-DE\","]
#[doc = "    \"CC-BY-NC-SA-2.0-FR\","]
#[doc = "    \"CC-BY-NC-SA-2.0-UK\","]
#[doc = "    \"CC-BY-NC-SA-2.5\","]
#[doc = "    \"CC-BY-NC-SA-3.0\","]
#[doc = "    \"CC-BY-NC-SA-3.0-DE\","]
#[doc = "    \"CC-BY-NC-SA-3.0-IGO\","]
#[doc = "    \"CC-BY-NC-SA-4.0\","]
#[doc = "    \"CC-BY-ND-1.0\","]
#[doc = "    \"CC-BY-ND-2.0\","]
#[doc = "    \"CC-BY-ND-2.5\","]
#[doc = "    \"CC-BY-ND-3.0\","]
#[doc = "    \"CC-BY-ND-3.0-DE\","]
#[doc = "    \"CC-BY-ND-4.0\","]
#[doc = "    \"CC-BY-SA-1.0\","]
#[doc = "    \"CC-BY-SA-2.0\","]
#[doc = "    \"CC-BY-SA-2.0-UK\","]
#[doc = "    \"CC-BY-SA-2.1-JP\","]
#[doc = "    \"CC-BY-SA-2.5\","]
#[doc = "    \"CC-BY-SA-3.0\","]
#[doc = "    \"CC-BY-SA-3.0-AT\","]
#[doc = "    \"CC-BY-SA-3.0-DE\","]
#[doc = "    \"CC-BY-SA-3.0-IGO\","]
#[doc = "    \"CC-BY-SA-4.0\","]
#[doc = "    \"CC-PDDC\","]
#[doc = "    \"CC0-1.0\","]
#[doc = "    \"CDDL-1.0\","]
#[doc = "    \"CDDL-1.1\","]
#[doc = "    \"CDL-1.0\","]
#[doc = "    \"CDLA-Permissive-1.0\","]
#[doc = "    \"CDLA-Permissive-2.0\","]
#[doc = "    \"CDLA-Sharing-1.0\","]
#[doc = "    \"CECILL-1.0\","]
#[doc = "    \"CECILL-1.1\","]
#[doc = "    \"CECILL-2.0\","]
#[doc = "    \"CECILL-2.1\","]
#[doc = "    \"CECILL-B\","]
#[doc = "    \"CECILL-C\","]
#[doc = "    \"CERN-OHL-1.1\","]
#[doc = "    \"CERN-OHL-1.2\","]
#[doc = "    \"CERN-OHL-P-2.0\","]
#[doc = "    \"CERN-OHL-S-2.0\","]
#[doc = "    \"CERN-OHL-W-2.0\","]
#[doc = "    \"CFITSIO\","]
#[doc = "    \"checkmk\","]
#[doc = "    \"ClArtistic\","]
#[doc = "    \"Clips\","]
#[doc = "    \"CMU-Mach\","]
#[doc = "    \"CNRI-Jython\","]
#[doc = "    \"CNRI-Python\","]
#[doc = "    \"CNRI-Python-GPL-Compatible\","]
#[doc = "    \"COIL-1.0\","]
#[doc = "    \"Community-Spec-1.0\","]
#[doc = "    \"Condor-1.1\","]
#[doc = "    \"copyleft-next-0.3.0\","]
#[doc = "    \"copyleft-next-0.3.1\","]
#[doc = "    \"Cornell-Lossless-JPEG\","]
#[doc = "    \"CPAL-1.0\","]
#[doc = "    \"CPL-1.0\","]
#[doc = "    \"CPOL-1.02\","]
#[doc = "    \"Crossword\","]
#[doc = "    \"CrystalStacker\","]
#[doc = "    \"CUA-OPL-1.0\","]
#[doc = "    \"Cube\","]
#[doc = "    \"curl\","]
#[doc = "    \"D-FSL-1.0\","]
#[doc = "    \"diffmark\","]
#[doc = "    \"DL-DE-BY-2.0\","]
#[doc = "    \"DOC\","]
#[doc = "    \"Dotseqn\","]
#[doc = "    \"DRL-1.0\","]
#[doc = "    \"DSDP\","]
#[doc = "    \"dtoa\","]
#[doc = "    \"dvipdfm\","]
#[doc = "    \"ECL-1.0\","]
#[doc = "    \"ECL-2.0\","]
#[doc = "    \"eCos-2.0\","]
#[doc = "    \"EFL-1.0\","]
#[doc = "    \"EFL-2.0\","]
#[doc = "    \"eGenix\","]
#[doc = "    \"Elastic-2.0\","]
#[doc = "    \"Entessa\","]
#[doc = "    \"EPICS\","]
#[doc = "    \"EPL-1.0\","]
#[doc = "    \"EPL-2.0\","]
#[doc = "    \"ErlPL-1.1\","]
#[doc = "    \"etalab-2.0\","]
#[doc = "    \"EUDatagrid\","]
#[doc = "    \"EUPL-1.0\","]
#[doc = "    \"EUPL-1.1\","]
#[doc = "    \"EUPL-1.2\","]
#[doc = "    \"Eurosym\","]
#[doc = "    \"Fair\","]
#[doc = "    \"FDK-AAC\","]
#[doc = "    \"Frameworx-1.0\","]
#[doc = "    \"FreeBSD-DOC\","]
#[doc = "    \"FreeImage\","]
#[doc = "    \"FSFAP\","]
#[doc = "    \"FSFUL\","]
#[doc = "    \"FSFULLR\","]
#[doc = "    \"FSFULLRWD\","]
#[doc = "    \"FTL\","]
#[doc = "    \"GD\","]
#[doc = "    \"GFDL-1.1\","]
#[doc = "    \"GFDL-1.1-invariants-only\","]
#[doc = "    \"GFDL-1.1-invariants-or-later\","]
#[doc = "    \"GFDL-1.1-no-invariants-only\","]
#[doc = "    \"GFDL-1.1-no-invariants-or-later\","]
#[doc = "    \"GFDL-1.1-only\","]
#[doc = "    \"GFDL-1.1-or-later\","]
#[doc = "    \"GFDL-1.2\","]
#[doc = "    \"GFDL-1.2-invariants-only\","]
#[doc = "    \"GFDL-1.2-invariants-or-later\","]
#[doc = "    \"GFDL-1.2-no-invariants-only\","]
#[doc = "    \"GFDL-1.2-no-invariants-or-later\","]
#[doc = "    \"GFDL-1.2-only\","]
#[doc = "    \"GFDL-1.2-or-later\","]
#[doc = "    \"GFDL-1.3\","]
#[doc = "    \"GFDL-1.3-invariants-only\","]
#[doc = "    \"GFDL-1.3-invariants-or-later\","]
#[doc = "    \"GFDL-1.3-no-invariants-only\","]
#[doc = "    \"GFDL-1.3-no-invariants-or-later\","]
#[doc = "    \"GFDL-1.3-only\","]
#[doc = "    \"GFDL-1.3-or-later\","]
#[doc = "    \"Giftware\","]
#[doc = "    \"GL2PS\","]
#[doc = "    \"Glide\","]
#[doc = "    \"Glulxe\","]
#[doc = "    \"GLWTPL\","]
#[doc = "    \"gnuplot\","]
#[doc = "    \"GPL-1.0\","]
#[doc = "    \"GPL-1.0+\","]
#[doc = "    \"GPL-1.0-only\","]
#[doc = "    \"GPL-1.0-or-later\","]
#[doc = "    \"GPL-2.0\","]
#[doc = "    \"GPL-2.0+\","]
#[doc = "    \"GPL-2.0-only\","]
#[doc = "    \"GPL-2.0-or-later\","]
#[doc = "    \"GPL-2.0-with-autoconf-exception\","]
#[doc = "    \"GPL-2.0-with-bison-exception\","]
#[doc = "    \"GPL-2.0-with-classpath-exception\","]
#[doc = "    \"GPL-2.0-with-font-exception\","]
#[doc = "    \"GPL-2.0-with-GCC-exception\","]
#[doc = "    \"GPL-3.0\","]
#[doc = "    \"GPL-3.0+\","]
#[doc = "    \"GPL-3.0-only\","]
#[doc = "    \"GPL-3.0-or-later\","]
#[doc = "    \"GPL-3.0-with-autoconf-exception\","]
#[doc = "    \"GPL-3.0-with-GCC-exception\","]
#[doc = "    \"Graphics-Gems\","]
#[doc = "    \"gSOAP-1.3b\","]
#[doc = "    \"HaskellReport\","]
#[doc = "    \"Hippocratic-2.1\","]
#[doc = "    \"HP-1986\","]
#[doc = "    \"HPND\","]
#[doc = "    \"HPND-export-US\","]
#[doc = "    \"HPND-Markus-Kuhn\","]
#[doc = "    \"HPND-sell-variant\","]
#[doc = "    \"HPND-sell-variant-MIT-disclaimer\","]
#[doc = "    \"HTMLTIDY\","]
#[doc = "    \"IBM-pibs\","]
#[doc = "    \"ICU\","]
#[doc = "    \"IEC-Code-Components-EULA\","]
#[doc = "    \"IJG\","]
#[doc = "    \"IJG-short\","]
#[doc = "    \"ImageMagick\","]
#[doc = "    \"iMatix\","]
#[doc = "    \"Imlib2\","]
#[doc = "    \"Info-ZIP\","]
#[doc = "    \"Inner-Net-2.0\","]
#[doc = "    \"Intel\","]
#[doc = "    \"Intel-ACPI\","]
#[doc = "    \"Interbase-1.0\","]
#[doc = "    \"IPA\","]
#[doc = "    \"IPL-1.0\","]
#[doc = "    \"ISC\","]
#[doc = "    \"Jam\","]
#[doc = "    \"JasPer-2.0\","]
#[doc = "    \"JPL-image\","]
#[doc = "    \"JPNIC\","]
#[doc = "    \"JSON\","]
#[doc = "    \"Kazlib\","]
#[doc = "    \"Knuth-CTAN\","]
#[doc = "    \"LAL-1.2\","]
#[doc = "    \"LAL-1.3\","]
#[doc = "    \"Latex2e\","]
#[doc = "    \"Latex2e-translated-notice\","]
#[doc = "    \"Leptonica\","]
#[doc = "    \"LGPL-2.0\","]
#[doc = "    \"LGPL-2.0+\","]
#[doc = "    \"LGPL-2.0-only\","]
#[doc = "    \"LGPL-2.0-or-later\","]
#[doc = "    \"LGPL-2.1\","]
#[doc = "    \"LGPL-2.1+\","]
#[doc = "    \"LGPL-2.1-only\","]
#[doc = "    \"LGPL-2.1-or-later\","]
#[doc = "    \"LGPL-3.0\","]
#[doc = "    \"LGPL-3.0+\","]
#[doc = "    \"LGPL-3.0-only\","]
#[doc = "    \"LGPL-3.0-or-later\","]
#[doc = "    \"LGPLLR\","]
#[doc = "    \"Libpng\","]
#[doc = "    \"libpng-2.0\","]
#[doc = "    \"libselinux-1.0\","]
#[doc = "    \"libtiff\","]
#[doc = "    \"libutil-David-Nugent\","]
#[doc = "    \"LiLiQ-P-1.1\","]
#[doc = "    \"LiLiQ-R-1.1\","]
#[doc = "    \"LiLiQ-Rplus-1.1\","]
#[doc = "    \"Linux-man-pages-1-para\","]
#[doc = "    \"Linux-man-pages-copyleft\","]
#[doc = "    \"Linux-man-pages-copyleft-2-para\","]
#[doc = "    \"Linux-man-pages-copyleft-var\","]
#[doc = "    \"Linux-OpenIB\","]
#[doc = "    \"LOOP\","]
#[doc = "    \"LPL-1.0\","]
#[doc = "    \"LPL-1.02\","]
#[doc = "    \"LPPL-1.0\","]
#[doc = "    \"LPPL-1.1\","]
#[doc = "    \"LPPL-1.2\","]
#[doc = "    \"LPPL-1.3a\","]
#[doc = "    \"LPPL-1.3c\","]
#[doc = "    \"LZMA-SDK-9.11-to-9.20\","]
#[doc = "    \"LZMA-SDK-9.22\","]
#[doc = "    \"MakeIndex\","]
#[doc = "    \"Martin-Birgmeier\","]
#[doc = "    \"metamail\","]
#[doc = "    \"Minpack\","]
#[doc = "    \"MirOS\","]
#[doc = "    \"MIT\","]
#[doc = "    \"MIT-0\","]
#[doc = "    \"MIT-advertising\","]
#[doc = "    \"MIT-CMU\","]
#[doc = "    \"MIT-enna\","]
#[doc = "    \"MIT-feh\","]
#[doc = "    \"MIT-Festival\","]
#[doc = "    \"MIT-Modern-Variant\","]
#[doc = "    \"MIT-open-group\","]
#[doc = "    \"MIT-Wu\","]
#[doc = "    \"MITNFA\","]
#[doc = "    \"Motosoto\","]
#[doc = "    \"mpi-permissive\","]
#[doc = "    \"mpich2\","]
#[doc = "    \"MPL-1.0\","]
#[doc = "    \"MPL-1.1\","]
#[doc = "    \"MPL-2.0\","]
#[doc = "    \"MPL-2.0-no-copyleft-exception\","]
#[doc = "    \"mplus\","]
#[doc = "    \"MS-LPL\","]
#[doc = "    \"MS-PL\","]
#[doc = "    \"MS-RL\","]
#[doc = "    \"MTLL\","]
#[doc = "    \"MulanPSL-1.0\","]
#[doc = "    \"MulanPSL-2.0\","]
#[doc = "    \"Multics\","]
#[doc = "    \"Mup\","]
#[doc = "    \"NAIST-2003\","]
#[doc = "    \"NASA-1.3\","]
#[doc = "    \"Naumen\","]
#[doc = "    \"NBPL-1.0\","]
#[doc = "    \"NCGL-UK-2.0\","]
#[doc = "    \"NCSA\","]
#[doc = "    \"Net-SNMP\","]
#[doc = "    \"NetCDF\","]
#[doc = "    \"Newsletr\","]
#[doc = "    \"NGPL\","]
#[doc = "    \"NICTA-1.0\","]
#[doc = "    \"NIST-PD\","]
#[doc = "    \"NIST-PD-fallback\","]
#[doc = "    \"NIST-Software\","]
#[doc = "    \"NLOD-1.0\","]
#[doc = "    \"NLOD-2.0\","]
#[doc = "    \"NLPL\","]
#[doc = "    \"Nokia\","]
#[doc = "    \"NOSL\","]
#[doc = "    \"Noweb\","]
#[doc = "    \"NPL-1.0\","]
#[doc = "    \"NPL-1.1\","]
#[doc = "    \"NPOSL-3.0\","]
#[doc = "    \"NRL\","]
#[doc = "    \"NTP\","]
#[doc = "    \"NTP-0\","]
#[doc = "    \"Nunit\","]
#[doc = "    \"O-UDA-1.0\","]
#[doc = "    \"OCCT-PL\","]
#[doc = "    \"OCLC-2.0\","]
#[doc = "    \"ODbL-1.0\","]
#[doc = "    \"ODC-By-1.0\","]
#[doc = "    \"OFFIS\","]
#[doc = "    \"OFL-1.0\","]
#[doc = "    \"OFL-1.0-no-RFN\","]
#[doc = "    \"OFL-1.0-RFN\","]
#[doc = "    \"OFL-1.1\","]
#[doc = "    \"OFL-1.1-no-RFN\","]
#[doc = "    \"OFL-1.1-RFN\","]
#[doc = "    \"OGC-1.0\","]
#[doc = "    \"OGDL-Taiwan-1.0\","]
#[doc = "    \"OGL-Canada-2.0\","]
#[doc = "    \"OGL-UK-1.0\","]
#[doc = "    \"OGL-UK-2.0\","]
#[doc = "    \"OGL-UK-3.0\","]
#[doc = "    \"OGTSL\","]
#[doc = "    \"OLDAP-1.1\","]
#[doc = "    \"OLDAP-1.2\","]
#[doc = "    \"OLDAP-1.3\","]
#[doc = "    \"OLDAP-1.4\","]
#[doc = "    \"OLDAP-2.0\","]
#[doc = "    \"OLDAP-2.0.1\","]
#[doc = "    \"OLDAP-2.1\","]
#[doc = "    \"OLDAP-2.2\","]
#[doc = "    \"OLDAP-2.2.1\","]
#[doc = "    \"OLDAP-2.2.2\","]
#[doc = "    \"OLDAP-2.3\","]
#[doc = "    \"OLDAP-2.4\","]
#[doc = "    \"OLDAP-2.5\","]
#[doc = "    \"OLDAP-2.6\","]
#[doc = "    \"OLDAP-2.7\","]
#[doc = "    \"OLDAP-2.8\","]
#[doc = "    \"OLFL-1.3\","]
#[doc = "    \"OML\","]
#[doc = "    \"OpenPBS-2.3\","]
#[doc = "    \"OpenSSL\","]
#[doc = "    \"OPL-1.0\","]
#[doc = "    \"OPL-UK-3.0\","]
#[doc = "    \"OPUBL-1.0\","]
#[doc = "    \"OSET-PL-2.1\","]
#[doc = "    \"OSL-1.0\","]
#[doc = "    \"OSL-1.1\","]
#[doc = "    \"OSL-2.0\","]
#[doc = "    \"OSL-2.1\","]
#[doc = "    \"OSL-3.0\","]
#[doc = "    \"Parity-6.0.0\","]
#[doc = "    \"Parity-7.0.0\","]
#[doc = "    \"PDDL-1.0\","]
#[doc = "    \"PHP-3.0\","]
#[doc = "    \"PHP-3.01\","]
#[doc = "    \"Plexus\","]
#[doc = "    \"PolyForm-Noncommercial-1.0.0\","]
#[doc = "    \"PolyForm-Small-Business-1.0.0\","]
#[doc = "    \"PostgreSQL\","]
#[doc = "    \"PSF-2.0\","]
#[doc = "    \"psfrag\","]
#[doc = "    \"psutils\","]
#[doc = "    \"Python-2.0\","]
#[doc = "    \"Python-2.0.1\","]
#[doc = "    \"Qhull\","]
#[doc = "    \"QPL-1.0\","]
#[doc = "    \"QPL-1.0-INRIA-2004\","]
#[doc = "    \"Rdisc\","]
#[doc = "    \"RHeCos-1.1\","]
#[doc = "    \"RPL-1.1\","]
#[doc = "    \"RPL-1.5\","]
#[doc = "    \"RPSL-1.0\","]
#[doc = "    \"RSA-MD\","]
#[doc = "    \"RSCPL\","]
#[doc = "    \"Ruby\","]
#[doc = "    \"SAX-PD\","]
#[doc = "    \"Saxpath\","]
#[doc = "    \"SCEA\","]
#[doc = "    \"SchemeReport\","]
#[doc = "    \"Sendmail\","]
#[doc = "    \"Sendmail-8.23\","]
#[doc = "    \"SGI-B-1.0\","]
#[doc = "    \"SGI-B-1.1\","]
#[doc = "    \"SGI-B-2.0\","]
#[doc = "    \"SGP4\","]
#[doc = "    \"SHL-0.5\","]
#[doc = "    \"SHL-0.51\","]
#[doc = "    \"SimPL-2.0\","]
#[doc = "    \"SISSL\","]
#[doc = "    \"SISSL-1.2\","]
#[doc = "    \"Sleepycat\","]
#[doc = "    \"SMLNJ\","]
#[doc = "    \"SMPPL\","]
#[doc = "    \"SNIA\","]
#[doc = "    \"snprintf\","]
#[doc = "    \"Spencer-86\","]
#[doc = "    \"Spencer-94\","]
#[doc = "    \"Spencer-99\","]
#[doc = "    \"SPL-1.0\","]
#[doc = "    \"SSH-OpenSSH\","]
#[doc = "    \"SSH-short\","]
#[doc = "    \"SSPL-1.0\","]
#[doc = "    \"StandardML-NJ\","]
#[doc = "    \"SugarCRM-1.1.3\","]
#[doc = "    \"SunPro\","]
#[doc = "    \"SWL\","]
#[doc = "    \"Symlinks\","]
#[doc = "    \"TAPR-OHL-1.0\","]
#[doc = "    \"TCL\","]
#[doc = "    \"TCP-wrappers\","]
#[doc = "    \"TermReadKey\","]
#[doc = "    \"TMate\","]
#[doc = "    \"TORQUE-1.1\","]
#[doc = "    \"TOSL\","]
#[doc = "    \"TPDL\","]
#[doc = "    \"TPL-1.0\","]
#[doc = "    \"TTWL\","]
#[doc = "    \"TU-Berlin-1.0\","]
#[doc = "    \"TU-Berlin-2.0\","]
#[doc = "    \"UCAR\","]
#[doc = "    \"UCL-1.0\","]
#[doc = "    \"Unicode-DFS-2015\","]
#[doc = "    \"Unicode-DFS-2016\","]
#[doc = "    \"Unicode-TOU\","]
#[doc = "    \"UnixCrypt\","]
#[doc = "    \"Unlicense\","]
#[doc = "    \"UPL-1.0\","]
#[doc = "    \"Vim\","]
#[doc = "    \"VOSTROM\","]
#[doc = "    \"VSL-1.0\","]
#[doc = "    \"W3C\","]
#[doc = "    \"W3C-19980720\","]
#[doc = "    \"W3C-20150513\","]
#[doc = "    \"w3m\","]
#[doc = "    \"Watcom-1.0\","]
#[doc = "    \"Widget-Workshop\","]
#[doc = "    \"Wsuipa\","]
#[doc = "    \"WTFPL\","]
#[doc = "    \"wxWindows\","]
#[doc = "    \"X11\","]
#[doc = "    \"X11-distribute-modifications-variant\","]
#[doc = "    \"Xdebug-1.03\","]
#[doc = "    \"Xerox\","]
#[doc = "    \"Xfig\","]
#[doc = "    \"XFree86-1.1\","]
#[doc = "    \"xinetd\","]
#[doc = "    \"xlock\","]
#[doc = "    \"Xnet\","]
#[doc = "    \"xpp\","]
#[doc = "    \"XSkat\","]
#[doc = "    \"YPL-1.0\","]
#[doc = "    \"YPL-1.1\","]
#[doc = "    \"Zed\","]
#[doc = "    \"Zend-2.0\","]
#[doc = "    \"Zimbra-1.3\","]
#[doc = "    \"Zimbra-1.4\","]
#[doc = "    \"Zlib\","]
#[doc = "    \"zlib-acknowledgement\","]
#[doc = "    \"ZPL-1.1\","]
#[doc = "    \"ZPL-2.0\","]
#[doc = "    \"ZPL-2.1\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum SpdxLicenseIds {
    #[serde(rename = "0BSD")]
    _0bsd,
    #[serde(rename = "AAL")]
    Aal,
    Abstyles,
    #[serde(rename = "AdaCore-doc")]
    AdaCoreDoc,
    #[serde(rename = "Adobe-2006")]
    Adobe2006,
    #[serde(rename = "Adobe-Glyph")]
    AdobeGlyph,
    #[serde(rename = "ADSL")]
    Adsl,
    #[serde(rename = "AFL-1.1")]
    Afl11,
    #[serde(rename = "AFL-1.2")]
    Afl12,
    #[serde(rename = "AFL-2.0")]
    Afl20,
    #[serde(rename = "AFL-2.1")]
    Afl21,
    #[serde(rename = "AFL-3.0")]
    Afl30,
    Afmparse,
    #[serde(rename = "AGPL-1.0")]
    Agpl10,
    #[serde(rename = "AGPL-1.0-only")]
    Agpl10Only,
    #[serde(rename = "AGPL-1.0-or-later")]
    Agpl10OrLater,
    #[serde(rename = "AGPL-3.0")]
    Agpl30,
    #[serde(rename = "AGPL-3.0-only")]
    Agpl30Only,
    #[serde(rename = "AGPL-3.0-or-later")]
    Agpl30OrLater,
    Aladdin,
    #[serde(rename = "AMDPLPA")]
    Amdplpa,
    #[serde(rename = "AML")]
    Aml,
    #[serde(rename = "AMPAS")]
    Ampas,
    #[serde(rename = "ANTLR-PD")]
    AntlrPd,
    #[serde(rename = "ANTLR-PD-fallback")]
    AntlrPdFallback,
    #[serde(rename = "Apache-1.0")]
    Apache10,
    #[serde(rename = "Apache-1.1")]
    Apache11,
    #[serde(rename = "Apache-2.0")]
    Apache20,
    #[serde(rename = "APAFML")]
    Apafml,
    #[serde(rename = "APL-1.0")]
    Apl10,
    #[serde(rename = "App-s2p")]
    AppS2p,
    #[serde(rename = "APSL-1.0")]
    Apsl10,
    #[serde(rename = "APSL-1.1")]
    Apsl11,
    #[serde(rename = "APSL-1.2")]
    Apsl12,
    #[serde(rename = "APSL-2.0")]
    Apsl20,
    #[serde(rename = "Arphic-1999")]
    Arphic1999,
    #[serde(rename = "Artistic-1.0")]
    Artistic10,
    #[serde(rename = "Artistic-1.0-cl8")]
    Artistic10Cl8,
    #[serde(rename = "Artistic-1.0-Perl")]
    Artistic10Perl,
    #[serde(rename = "Artistic-2.0")]
    Artistic20,
    #[serde(rename = "ASWF-Digital-Assets-1.0")]
    AswfDigitalAssets10,
    #[serde(rename = "ASWF-Digital-Assets-1.1")]
    AswfDigitalAssets11,
    Baekmuk,
    Bahyph,
    Barr,
    Beerware,
    #[serde(rename = "Bitstream-Charter")]
    BitstreamCharter,
    #[serde(rename = "Bitstream-Vera")]
    BitstreamVera,
    #[serde(rename = "BitTorrent-1.0")]
    BitTorrent10,
    #[serde(rename = "BitTorrent-1.1")]
    BitTorrent11,
    #[serde(rename = "blessing")]
    Blessing,
    #[serde(rename = "BlueOak-1.0.0")]
    BlueOak100,
    #[serde(rename = "Boehm-GC")]
    BoehmGc,
    Borceux,
    #[serde(rename = "Brian-Gladman-3-Clause")]
    BrianGladman3Clause,
    #[serde(rename = "BSD-1-Clause")]
    Bsd1Clause,
    #[serde(rename = "BSD-2-Clause")]
    Bsd2Clause,
    #[serde(rename = "BSD-2-Clause-FreeBSD")]
    Bsd2ClauseFreeBsd,
    #[serde(rename = "BSD-2-Clause-NetBSD")]
    Bsd2ClauseNetBsd,
    #[serde(rename = "BSD-2-Clause-Patent")]
    Bsd2ClausePatent,
    #[serde(rename = "BSD-2-Clause-Views")]
    Bsd2ClauseViews,
    #[serde(rename = "BSD-3-Clause")]
    Bsd3Clause,
    #[serde(rename = "BSD-3-Clause-Attribution")]
    Bsd3ClauseAttribution,
    #[serde(rename = "BSD-3-Clause-Clear")]
    Bsd3ClauseClear,
    #[serde(rename = "BSD-3-Clause-LBNL")]
    Bsd3ClauseLbnl,
    #[serde(rename = "BSD-3-Clause-Modification")]
    Bsd3ClauseModification,
    #[serde(rename = "BSD-3-Clause-No-Military-License")]
    Bsd3ClauseNoMilitaryLicense,
    #[serde(rename = "BSD-3-Clause-No-Nuclear-License")]
    Bsd3ClauseNoNuclearLicense,
    #[serde(rename = "BSD-3-Clause-No-Nuclear-License-2014")]
    Bsd3ClauseNoNuclearLicense2014,
    #[serde(rename = "BSD-3-Clause-No-Nuclear-Warranty")]
    Bsd3ClauseNoNuclearWarranty,
    #[serde(rename = "BSD-3-Clause-Open-MPI")]
    Bsd3ClauseOpenMpi,
    #[serde(rename = "BSD-4-Clause")]
    Bsd4Clause,
    #[serde(rename = "BSD-4-Clause-Shortened")]
    Bsd4ClauseShortened,
    #[serde(rename = "BSD-4-Clause-UC")]
    Bsd4ClauseUc,
    #[serde(rename = "BSD-4.3RENO")]
    Bsd43reno,
    #[serde(rename = "BSD-4.3TAHOE")]
    Bsd43tahoe,
    #[serde(rename = "BSD-Advertising-Acknowledgement")]
    BsdAdvertisingAcknowledgement,
    #[serde(rename = "BSD-Attribution-HPND-disclaimer")]
    BsdAttributionHpndDisclaimer,
    #[serde(rename = "BSD-Protection")]
    BsdProtection,
    #[serde(rename = "BSD-Source-Code")]
    BsdSourceCode,
    #[serde(rename = "BSL-1.0")]
    Bsl10,
    #[serde(rename = "BUSL-1.1")]
    Busl11,
    #[serde(rename = "bzip2-1.0.5")]
    Bzip2105,
    #[serde(rename = "bzip2-1.0.6")]
    Bzip2106,
    #[serde(rename = "C-UDA-1.0")]
    CUda10,
    #[serde(rename = "CAL-1.0")]
    Cal10,
    #[serde(rename = "CAL-1.0-Combined-Work-Exception")]
    Cal10CombinedWorkException,
    Caldera,
    #[serde(rename = "CATOSL-1.1")]
    Catosl11,
    #[serde(rename = "CC-BY-1.0")]
    CcBy10,
    #[serde(rename = "CC-BY-2.0")]
    CcBy20,
    #[serde(rename = "CC-BY-2.5")]
    CcBy25,
    #[serde(rename = "CC-BY-2.5-AU")]
    CcBy25Au,
    #[serde(rename = "CC-BY-3.0")]
    CcBy30,
    #[serde(rename = "CC-BY-3.0-AT")]
    CcBy30At,
    #[serde(rename = "CC-BY-3.0-DE")]
    CcBy30De,
    #[serde(rename = "CC-BY-3.0-IGO")]
    CcBy30Igo,
    #[serde(rename = "CC-BY-3.0-NL")]
    CcBy30Nl,
    #[serde(rename = "CC-BY-3.0-US")]
    CcBy30Us,
    #[serde(rename = "CC-BY-4.0")]
    CcBy40,
    #[serde(rename = "CC-BY-NC-1.0")]
    CcByNc10,
    #[serde(rename = "CC-BY-NC-2.0")]
    CcByNc20,
    #[serde(rename = "CC-BY-NC-2.5")]
    CcByNc25,
    #[serde(rename = "CC-BY-NC-3.0")]
    CcByNc30,
    #[serde(rename = "CC-BY-NC-3.0-DE")]
    CcByNc30De,
    #[serde(rename = "CC-BY-NC-4.0")]
    CcByNc40,
    #[serde(rename = "CC-BY-NC-ND-1.0")]
    CcByNcNd10,
    #[serde(rename = "CC-BY-NC-ND-2.0")]
    CcByNcNd20,
    #[serde(rename = "CC-BY-NC-ND-2.5")]
    CcByNcNd25,
    #[serde(rename = "CC-BY-NC-ND-3.0")]
    CcByNcNd30,
    #[serde(rename = "CC-BY-NC-ND-3.0-DE")]
    CcByNcNd30De,
    #[serde(rename = "CC-BY-NC-ND-3.0-IGO")]
    CcByNcNd30Igo,
    #[serde(rename = "CC-BY-NC-ND-4.0")]
    CcByNcNd40,
    #[serde(rename = "CC-BY-NC-SA-1.0")]
    CcByNcSa10,
    #[serde(rename = "CC-BY-NC-SA-2.0")]
    CcByNcSa20,
    #[serde(rename = "CC-BY-NC-SA-2.0-DE")]
    CcByNcSa20De,
    #[serde(rename = "CC-BY-NC-SA-2.0-FR")]
    CcByNcSa20Fr,
    #[serde(rename = "CC-BY-NC-SA-2.0-UK")]
    CcByNcSa20Uk,
    #[serde(rename = "CC-BY-NC-SA-2.5")]
    CcByNcSa25,
    #[serde(rename = "CC-BY-NC-SA-3.0")]
    CcByNcSa30,
    #[serde(rename = "CC-BY-NC-SA-3.0-DE")]
    CcByNcSa30De,
    #[serde(rename = "CC-BY-NC-SA-3.0-IGO")]
    CcByNcSa30Igo,
    #[serde(rename = "CC-BY-NC-SA-4.0")]
    CcByNcSa40,
    #[serde(rename = "CC-BY-ND-1.0")]
    CcByNd10,
    #[serde(rename = "CC-BY-ND-2.0")]
    CcByNd20,
    #[serde(rename = "CC-BY-ND-2.5")]
    CcByNd25,
    #[serde(rename = "CC-BY-ND-3.0")]
    CcByNd30,
    #[serde(rename = "CC-BY-ND-3.0-DE")]
    CcByNd30De,
    #[serde(rename = "CC-BY-ND-4.0")]
    CcByNd40,
    #[serde(rename = "CC-BY-SA-1.0")]
    CcBySa10,
    #[serde(rename = "CC-BY-SA-2.0")]
    CcBySa20,
    #[serde(rename = "CC-BY-SA-2.0-UK")]
    CcBySa20Uk,
    #[serde(rename = "CC-BY-SA-2.1-JP")]
    CcBySa21Jp,
    #[serde(rename = "CC-BY-SA-2.5")]
    CcBySa25,
    #[serde(rename = "CC-BY-SA-3.0")]
    CcBySa30,
    #[serde(rename = "CC-BY-SA-3.0-AT")]
    CcBySa30At,
    #[serde(rename = "CC-BY-SA-3.0-DE")]
    CcBySa30De,
    #[serde(rename = "CC-BY-SA-3.0-IGO")]
    CcBySa30Igo,
    #[serde(rename = "CC-BY-SA-4.0")]
    CcBySa40,
    #[serde(rename = "CC-PDDC")]
    CcPddc,
    #[serde(rename = "CC0-1.0")]
    Cc010,
    #[serde(rename = "CDDL-1.0")]
    Cddl10,
    #[serde(rename = "CDDL-1.1")]
    Cddl11,
    #[serde(rename = "CDL-1.0")]
    Cdl10,
    #[serde(rename = "CDLA-Permissive-1.0")]
    CdlaPermissive10,
    #[serde(rename = "CDLA-Permissive-2.0")]
    CdlaPermissive20,
    #[serde(rename = "CDLA-Sharing-1.0")]
    CdlaSharing10,
    #[serde(rename = "CECILL-1.0")]
    Cecill10,
    #[serde(rename = "CECILL-1.1")]
    Cecill11,
    #[serde(rename = "CECILL-2.0")]
    Cecill20,
    #[serde(rename = "CECILL-2.1")]
    Cecill21,
    #[serde(rename = "CECILL-B")]
    CecillB,
    #[serde(rename = "CECILL-C")]
    CecillC,
    #[serde(rename = "CERN-OHL-1.1")]
    CernOhl11,
    #[serde(rename = "CERN-OHL-1.2")]
    CernOhl12,
    #[serde(rename = "CERN-OHL-P-2.0")]
    CernOhlP20,
    #[serde(rename = "CERN-OHL-S-2.0")]
    CernOhlS20,
    #[serde(rename = "CERN-OHL-W-2.0")]
    CernOhlW20,
    #[serde(rename = "CFITSIO")]
    Cfitsio,
    #[serde(rename = "checkmk")]
    Checkmk,
    ClArtistic,
    Clips,
    #[serde(rename = "CMU-Mach")]
    CmuMach,
    #[serde(rename = "CNRI-Jython")]
    CnriJython,
    #[serde(rename = "CNRI-Python")]
    CnriPython,
    #[serde(rename = "CNRI-Python-GPL-Compatible")]
    CnriPythonGplCompatible,
    #[serde(rename = "COIL-1.0")]
    Coil10,
    #[serde(rename = "Community-Spec-1.0")]
    CommunitySpec10,
    #[serde(rename = "Condor-1.1")]
    Condor11,
    #[serde(rename = "copyleft-next-0.3.0")]
    CopyleftNext030,
    #[serde(rename = "copyleft-next-0.3.1")]
    CopyleftNext031,
    #[serde(rename = "Cornell-Lossless-JPEG")]
    CornellLosslessJpeg,
    #[serde(rename = "CPAL-1.0")]
    Cpal10,
    #[serde(rename = "CPL-1.0")]
    Cpl10,
    #[serde(rename = "CPOL-1.02")]
    Cpol102,
    Crossword,
    CrystalStacker,
    #[serde(rename = "CUA-OPL-1.0")]
    CuaOpl10,
    Cube,
    #[serde(rename = "curl")]
    Curl,
    #[serde(rename = "D-FSL-1.0")]
    DFsl10,
    #[serde(rename = "diffmark")]
    Diffmark,
    #[serde(rename = "DL-DE-BY-2.0")]
    DlDeBy20,
    #[serde(rename = "DOC")]
    Doc,
    Dotseqn,
    #[serde(rename = "DRL-1.0")]
    Drl10,
    #[serde(rename = "DSDP")]
    Dsdp,
    #[serde(rename = "dtoa")]
    Dtoa,
    #[serde(rename = "dvipdfm")]
    Dvipdfm,
    #[serde(rename = "ECL-1.0")]
    Ecl10,
    #[serde(rename = "ECL-2.0")]
    Ecl20,
    #[serde(rename = "eCos-2.0")]
    ECos20,
    #[serde(rename = "EFL-1.0")]
    Efl10,
    #[serde(rename = "EFL-2.0")]
    Efl20,
    #[serde(rename = "eGenix")]
    EGenix,
    #[serde(rename = "Elastic-2.0")]
    Elastic20,
    Entessa,
    #[serde(rename = "EPICS")]
    Epics,
    #[serde(rename = "EPL-1.0")]
    Epl10,
    #[serde(rename = "EPL-2.0")]
    Epl20,
    #[serde(rename = "ErlPL-1.1")]
    ErlPl11,
    #[serde(rename = "etalab-2.0")]
    Etalab20,
    #[serde(rename = "EUDatagrid")]
    EuDatagrid,
    #[serde(rename = "EUPL-1.0")]
    Eupl10,
    #[serde(rename = "EUPL-1.1")]
    Eupl11,
    #[serde(rename = "EUPL-1.2")]
    Eupl12,
    Eurosym,
    Fair,
    #[serde(rename = "FDK-AAC")]
    FdkAac,
    #[serde(rename = "Frameworx-1.0")]
    Frameworx10,
    #[serde(rename = "FreeBSD-DOC")]
    FreeBsdDoc,
    FreeImage,
    #[serde(rename = "FSFAP")]
    Fsfap,
    #[serde(rename = "FSFUL")]
    Fsful,
    #[serde(rename = "FSFULLR")]
    Fsfullr,
    #[serde(rename = "FSFULLRWD")]
    Fsfullrwd,
    #[serde(rename = "FTL")]
    Ftl,
    #[serde(rename = "GD")]
    Gd,
    #[serde(rename = "GFDL-1.1")]
    Gfdl11,
    #[serde(rename = "GFDL-1.1-invariants-only")]
    Gfdl11InvariantsOnly,
    #[serde(rename = "GFDL-1.1-invariants-or-later")]
    Gfdl11InvariantsOrLater,
    #[serde(rename = "GFDL-1.1-no-invariants-only")]
    Gfdl11NoInvariantsOnly,
    #[serde(rename = "GFDL-1.1-no-invariants-or-later")]
    Gfdl11NoInvariantsOrLater,
    #[serde(rename = "GFDL-1.1-only")]
    Gfdl11Only,
    #[serde(rename = "GFDL-1.1-or-later")]
    Gfdl11OrLater,
    #[serde(rename = "GFDL-1.2")]
    Gfdl12,
    #[serde(rename = "GFDL-1.2-invariants-only")]
    Gfdl12InvariantsOnly,
    #[serde(rename = "GFDL-1.2-invariants-or-later")]
    Gfdl12InvariantsOrLater,
    #[serde(rename = "GFDL-1.2-no-invariants-only")]
    Gfdl12NoInvariantsOnly,
    #[serde(rename = "GFDL-1.2-no-invariants-or-later")]
    Gfdl12NoInvariantsOrLater,
    #[serde(rename = "GFDL-1.2-only")]
    Gfdl12Only,
    #[serde(rename = "GFDL-1.2-or-later")]
    Gfdl12OrLater,
    #[serde(rename = "GFDL-1.3")]
    Gfdl13,
    #[serde(rename = "GFDL-1.3-invariants-only")]
    Gfdl13InvariantsOnly,
    #[serde(rename = "GFDL-1.3-invariants-or-later")]
    Gfdl13InvariantsOrLater,
    #[serde(rename = "GFDL-1.3-no-invariants-only")]
    Gfdl13NoInvariantsOnly,
    #[serde(rename = "GFDL-1.3-no-invariants-or-later")]
    Gfdl13NoInvariantsOrLater,
    #[serde(rename = "GFDL-1.3-only")]
    Gfdl13Only,
    #[serde(rename = "GFDL-1.3-or-later")]
    Gfdl13OrLater,
    Giftware,
    #[serde(rename = "GL2PS")]
    Gl2ps,
    Glide,
    Glulxe,
    #[serde(rename = "GLWTPL")]
    Glwtpl,
    #[serde(rename = "gnuplot")]
    Gnuplot,
    #[serde(rename = "GPL-1.0")]
    Gpl10,
    #[serde(rename = "GPL-1.0+")]
    Gpl10p,
    #[serde(rename = "GPL-1.0-only")]
    Gpl10Only,
    #[serde(rename = "GPL-1.0-or-later")]
    Gpl10OrLater,
    #[serde(rename = "GPL-2.0")]
    Gpl20,
    #[serde(rename = "GPL-2.0+")]
    Gpl20p,
    #[serde(rename = "GPL-2.0-only")]
    Gpl20Only,
    #[serde(rename = "GPL-2.0-or-later")]
    Gpl20OrLater,
    #[serde(rename = "GPL-2.0-with-autoconf-exception")]
    Gpl20WithAutoconfException,
    #[serde(rename = "GPL-2.0-with-bison-exception")]
    Gpl20WithBisonException,
    #[serde(rename = "GPL-2.0-with-classpath-exception")]
    Gpl20WithClasspathException,
    #[serde(rename = "GPL-2.0-with-font-exception")]
    Gpl20WithFontException,
    #[serde(rename = "GPL-2.0-with-GCC-exception")]
    Gpl20WithGccException,
    #[serde(rename = "GPL-3.0")]
    Gpl30,
    #[serde(rename = "GPL-3.0+")]
    Gpl30p,
    #[serde(rename = "GPL-3.0-only")]
    Gpl30Only,
    #[serde(rename = "GPL-3.0-or-later")]
    Gpl30OrLater,
    #[serde(rename = "GPL-3.0-with-autoconf-exception")]
    Gpl30WithAutoconfException,
    #[serde(rename = "GPL-3.0-with-GCC-exception")]
    Gpl30WithGccException,
    #[serde(rename = "Graphics-Gems")]
    GraphicsGems,
    #[serde(rename = "gSOAP-1.3b")]
    GSoap13b,
    HaskellReport,
    #[serde(rename = "Hippocratic-2.1")]
    Hippocratic21,
    #[serde(rename = "HP-1986")]
    Hp1986,
    #[serde(rename = "HPND")]
    Hpnd,
    #[serde(rename = "HPND-export-US")]
    HpndExportUs,
    #[serde(rename = "HPND-Markus-Kuhn")]
    HpndMarkusKuhn,
    #[serde(rename = "HPND-sell-variant")]
    HpndSellVariant,
    #[serde(rename = "HPND-sell-variant-MIT-disclaimer")]
    HpndSellVariantMitDisclaimer,
    #[serde(rename = "HTMLTIDY")]
    Htmltidy,
    #[serde(rename = "IBM-pibs")]
    IbmPibs,
    #[serde(rename = "ICU")]
    Icu,
    #[serde(rename = "IEC-Code-Components-EULA")]
    IecCodeComponentsEula,
    #[serde(rename = "IJG")]
    Ijg,
    #[serde(rename = "IJG-short")]
    IjgShort,
    ImageMagick,
    #[serde(rename = "iMatix")]
    IMatix,
    Imlib2,
    #[serde(rename = "Info-ZIP")]
    InfoZip,
    #[serde(rename = "Inner-Net-2.0")]
    InnerNet20,
    Intel,
    #[serde(rename = "Intel-ACPI")]
    IntelAcpi,
    #[serde(rename = "Interbase-1.0")]
    Interbase10,
    #[serde(rename = "IPA")]
    Ipa,
    #[serde(rename = "IPL-1.0")]
    Ipl10,
    #[serde(rename = "ISC")]
    Isc,
    Jam,
    #[serde(rename = "JasPer-2.0")]
    JasPer20,
    #[serde(rename = "JPL-image")]
    JplImage,
    #[serde(rename = "JPNIC")]
    Jpnic,
    #[serde(rename = "JSON")]
    Json,
    Kazlib,
    #[serde(rename = "Knuth-CTAN")]
    KnuthCtan,
    #[serde(rename = "LAL-1.2")]
    Lal12,
    #[serde(rename = "LAL-1.3")]
    Lal13,
    Latex2e,
    #[serde(rename = "Latex2e-translated-notice")]
    Latex2eTranslatedNotice,
    Leptonica,
    #[serde(rename = "LGPL-2.0")]
    Lgpl20,
    #[serde(rename = "LGPL-2.0+")]
    Lgpl20p,
    #[serde(rename = "LGPL-2.0-only")]
    Lgpl20Only,
    #[serde(rename = "LGPL-2.0-or-later")]
    Lgpl20OrLater,
    #[serde(rename = "LGPL-2.1")]
    Lgpl21,
    #[serde(rename = "LGPL-2.1+")]
    Lgpl21p,
    #[serde(rename = "LGPL-2.1-only")]
    Lgpl21Only,
    #[serde(rename = "LGPL-2.1-or-later")]
    Lgpl21OrLater,
    #[serde(rename = "LGPL-3.0")]
    Lgpl30,
    #[serde(rename = "LGPL-3.0+")]
    Lgpl30p,
    #[serde(rename = "LGPL-3.0-only")]
    Lgpl30Only,
    #[serde(rename = "LGPL-3.0-or-later")]
    Lgpl30OrLater,
    #[serde(rename = "LGPLLR")]
    Lgpllr,
    Libpng,
    #[serde(rename = "libpng-2.0")]
    Libpng20,
    #[serde(rename = "libselinux-1.0")]
    Libselinux10,
    #[serde(rename = "libtiff")]
    Libtiff,
    #[serde(rename = "libutil-David-Nugent")]
    LibutilDavidNugent,
    #[serde(rename = "LiLiQ-P-1.1")]
    LiLiQP11,
    #[serde(rename = "LiLiQ-R-1.1")]
    LiLiQR11,
    #[serde(rename = "LiLiQ-Rplus-1.1")]
    LiLiQRplus11,
    #[serde(rename = "Linux-man-pages-1-para")]
    LinuxManPages1Para,
    #[serde(rename = "Linux-man-pages-copyleft")]
    LinuxManPagesCopyleft,
    #[serde(rename = "Linux-man-pages-copyleft-2-para")]
    LinuxManPagesCopyleft2Para,
    #[serde(rename = "Linux-man-pages-copyleft-var")]
    LinuxManPagesCopyleftVar,
    #[serde(rename = "Linux-OpenIB")]
    LinuxOpenIb,
    #[serde(rename = "LOOP")]
    Loop,
    #[serde(rename = "LPL-1.0")]
    Lpl10,
    #[serde(rename = "LPL-1.02")]
    Lpl102,
    #[serde(rename = "LPPL-1.0")]
    Lppl10,
    #[serde(rename = "LPPL-1.1")]
    Lppl11,
    #[serde(rename = "LPPL-1.2")]
    Lppl12,
    #[serde(rename = "LPPL-1.3a")]
    Lppl13a,
    #[serde(rename = "LPPL-1.3c")]
    Lppl13c,
    #[serde(rename = "LZMA-SDK-9.11-to-9.20")]
    LzmaSdk911To920,
    #[serde(rename = "LZMA-SDK-9.22")]
    LzmaSdk922,
    MakeIndex,
    #[serde(rename = "Martin-Birgmeier")]
    MartinBirgmeier,
    #[serde(rename = "metamail")]
    Metamail,
    Minpack,
    #[serde(rename = "MirOS")]
    MirOs,
    #[serde(rename = "MIT")]
    Mit,
    #[serde(rename = "MIT-0")]
    Mit0,
    #[serde(rename = "MIT-advertising")]
    MitAdvertising,
    #[serde(rename = "MIT-CMU")]
    MitCmu,
    #[serde(rename = "MIT-enna")]
    MitEnna,
    #[serde(rename = "MIT-feh")]
    MitFeh,
    #[serde(rename = "MIT-Festival")]
    MitFestival,
    #[serde(rename = "MIT-Modern-Variant")]
    MitModernVariant,
    #[serde(rename = "MIT-open-group")]
    MitOpenGroup,
    #[serde(rename = "MIT-Wu")]
    MitWu,
    #[serde(rename = "MITNFA")]
    Mitnfa,
    Motosoto,
    #[serde(rename = "mpi-permissive")]
    MpiPermissive,
    #[serde(rename = "mpich2")]
    Mpich2,
    #[serde(rename = "MPL-1.0")]
    Mpl10,
    #[serde(rename = "MPL-1.1")]
    Mpl11,
    #[serde(rename = "MPL-2.0")]
    Mpl20,
    #[serde(rename = "MPL-2.0-no-copyleft-exception")]
    Mpl20NoCopyleftException,
    #[serde(rename = "mplus")]
    Mplus,
    #[serde(rename = "MS-LPL")]
    MsLpl,
    #[serde(rename = "MS-PL")]
    MsPl,
    #[serde(rename = "MS-RL")]
    MsRl,
    #[serde(rename = "MTLL")]
    Mtll,
    #[serde(rename = "MulanPSL-1.0")]
    MulanPsl10,
    #[serde(rename = "MulanPSL-2.0")]
    MulanPsl20,
    Multics,
    Mup,
    #[serde(rename = "NAIST-2003")]
    Naist2003,
    #[serde(rename = "NASA-1.3")]
    Nasa13,
    Naumen,
    #[serde(rename = "NBPL-1.0")]
    Nbpl10,
    #[serde(rename = "NCGL-UK-2.0")]
    NcglUk20,
    #[serde(rename = "NCSA")]
    Ncsa,
    #[serde(rename = "Net-SNMP")]
    NetSnmp,
    #[serde(rename = "NetCDF")]
    NetCdf,
    Newsletr,
    #[serde(rename = "NGPL")]
    Ngpl,
    #[serde(rename = "NICTA-1.0")]
    Nicta10,
    #[serde(rename = "NIST-PD")]
    NistPd,
    #[serde(rename = "NIST-PD-fallback")]
    NistPdFallback,
    #[serde(rename = "NIST-Software")]
    NistSoftware,
    #[serde(rename = "NLOD-1.0")]
    Nlod10,
    #[serde(rename = "NLOD-2.0")]
    Nlod20,
    #[serde(rename = "NLPL")]
    Nlpl,
    Nokia,
    #[serde(rename = "NOSL")]
    Nosl,
    Noweb,
    #[serde(rename = "NPL-1.0")]
    Npl10,
    #[serde(rename = "NPL-1.1")]
    Npl11,
    #[serde(rename = "NPOSL-3.0")]
    Nposl30,
    #[serde(rename = "NRL")]
    Nrl,
    #[serde(rename = "NTP")]
    Ntp,
    #[serde(rename = "NTP-0")]
    Ntp0,
    Nunit,
    #[serde(rename = "O-UDA-1.0")]
    OUda10,
    #[serde(rename = "OCCT-PL")]
    OcctPl,
    #[serde(rename = "OCLC-2.0")]
    Oclc20,
    #[serde(rename = "ODbL-1.0")]
    ODbL10,
    #[serde(rename = "ODC-By-1.0")]
    OdcBy10,
    #[serde(rename = "OFFIS")]
    Offis,
    #[serde(rename = "OFL-1.0")]
    Ofl10,
    #[serde(rename = "OFL-1.0-no-RFN")]
    Ofl10NoRfn,
    #[serde(rename = "OFL-1.0-RFN")]
    Ofl10Rfn,
    #[serde(rename = "OFL-1.1")]
    Ofl11,
    #[serde(rename = "OFL-1.1-no-RFN")]
    Ofl11NoRfn,
    #[serde(rename = "OFL-1.1-RFN")]
    Ofl11Rfn,
    #[serde(rename = "OGC-1.0")]
    Ogc10,
    #[serde(rename = "OGDL-Taiwan-1.0")]
    OgdlTaiwan10,
    #[serde(rename = "OGL-Canada-2.0")]
    OglCanada20,
    #[serde(rename = "OGL-UK-1.0")]
    OglUk10,
    #[serde(rename = "OGL-UK-2.0")]
    OglUk20,
    #[serde(rename = "OGL-UK-3.0")]
    OglUk30,
    #[serde(rename = "OGTSL")]
    Ogtsl,
    #[serde(rename = "OLDAP-1.1")]
    Oldap11,
    #[serde(rename = "OLDAP-1.2")]
    Oldap12,
    #[serde(rename = "OLDAP-1.3")]
    Oldap13,
    #[serde(rename = "OLDAP-1.4")]
    Oldap14,
    #[serde(rename = "OLDAP-2.0")]
    Oldap20,
    #[serde(rename = "OLDAP-2.0.1")]
    Oldap201,
    #[serde(rename = "OLDAP-2.1")]
    Oldap21,
    #[serde(rename = "OLDAP-2.2")]
    Oldap22,
    #[serde(rename = "OLDAP-2.2.1")]
    Oldap221,
    #[serde(rename = "OLDAP-2.2.2")]
    Oldap222,
    #[serde(rename = "OLDAP-2.3")]
    Oldap23,
    #[serde(rename = "OLDAP-2.4")]
    Oldap24,
    #[serde(rename = "OLDAP-2.5")]
    Oldap25,
    #[serde(rename = "OLDAP-2.6")]
    Oldap26,
    #[serde(rename = "OLDAP-2.7")]
    Oldap27,
    #[serde(rename = "OLDAP-2.8")]
    Oldap28,
    #[serde(rename = "OLFL-1.3")]
    Olfl13,
    #[serde(rename = "OML")]
    Oml,
    #[serde(rename = "OpenPBS-2.3")]
    OpenPbs23,
    #[serde(rename = "OpenSSL")]
    OpenSsl,
    #[serde(rename = "OPL-1.0")]
    Opl10,
    #[serde(rename = "OPL-UK-3.0")]
    OplUk30,
    #[serde(rename = "OPUBL-1.0")]
    Opubl10,
    #[serde(rename = "OSET-PL-2.1")]
    OsetPl21,
    #[serde(rename = "OSL-1.0")]
    Osl10,
    #[serde(rename = "OSL-1.1")]
    Osl11,
    #[serde(rename = "OSL-2.0")]
    Osl20,
    #[serde(rename = "OSL-2.1")]
    Osl21,
    #[serde(rename = "OSL-3.0")]
    Osl30,
    #[serde(rename = "Parity-6.0.0")]
    Parity600,
    #[serde(rename = "Parity-7.0.0")]
    Parity700,
    #[serde(rename = "PDDL-1.0")]
    Pddl10,
    #[serde(rename = "PHP-3.0")]
    Php30,
    #[serde(rename = "PHP-3.01")]
    Php301,
    Plexus,
    #[serde(rename = "PolyForm-Noncommercial-1.0.0")]
    PolyFormNoncommercial100,
    #[serde(rename = "PolyForm-Small-Business-1.0.0")]
    PolyFormSmallBusiness100,
    #[serde(rename = "PostgreSQL")]
    PostgreSql,
    #[serde(rename = "PSF-2.0")]
    Psf20,
    #[serde(rename = "psfrag")]
    Psfrag,
    #[serde(rename = "psutils")]
    Psutils,
    #[serde(rename = "Python-2.0")]
    Python20,
    #[serde(rename = "Python-2.0.1")]
    Python201,
    Qhull,
    #[serde(rename = "QPL-1.0")]
    Qpl10,
    #[serde(rename = "QPL-1.0-INRIA-2004")]
    Qpl10Inria2004,
    Rdisc,
    #[serde(rename = "RHeCos-1.1")]
    RHeCos11,
    #[serde(rename = "RPL-1.1")]
    Rpl11,
    #[serde(rename = "RPL-1.5")]
    Rpl15,
    #[serde(rename = "RPSL-1.0")]
    Rpsl10,
    #[serde(rename = "RSA-MD")]
    RsaMd,
    #[serde(rename = "RSCPL")]
    Rscpl,
    Ruby,
    #[serde(rename = "SAX-PD")]
    SaxPd,
    Saxpath,
    #[serde(rename = "SCEA")]
    Scea,
    SchemeReport,
    Sendmail,
    #[serde(rename = "Sendmail-8.23")]
    Sendmail823,
    #[serde(rename = "SGI-B-1.0")]
    SgiB10,
    #[serde(rename = "SGI-B-1.1")]
    SgiB11,
    #[serde(rename = "SGI-B-2.0")]
    SgiB20,
    #[serde(rename = "SGP4")]
    Sgp4,
    #[serde(rename = "SHL-0.5")]
    Shl05,
    #[serde(rename = "SHL-0.51")]
    Shl051,
    #[serde(rename = "SimPL-2.0")]
    SimPl20,
    #[serde(rename = "SISSL")]
    Sissl,
    #[serde(rename = "SISSL-1.2")]
    Sissl12,
    Sleepycat,
    #[serde(rename = "SMLNJ")]
    Smlnj,
    #[serde(rename = "SMPPL")]
    Smppl,
    #[serde(rename = "SNIA")]
    Snia,
    #[serde(rename = "snprintf")]
    Snprintf,
    #[serde(rename = "Spencer-86")]
    Spencer86,
    #[serde(rename = "Spencer-94")]
    Spencer94,
    #[serde(rename = "Spencer-99")]
    Spencer99,
    #[serde(rename = "SPL-1.0")]
    Spl10,
    #[serde(rename = "SSH-OpenSSH")]
    SshOpenSsh,
    #[serde(rename = "SSH-short")]
    SshShort,
    #[serde(rename = "SSPL-1.0")]
    Sspl10,
    #[serde(rename = "StandardML-NJ")]
    StandardMlNj,
    #[serde(rename = "SugarCRM-1.1.3")]
    SugarCrm113,
    SunPro,
    #[serde(rename = "SWL")]
    Swl,
    Symlinks,
    #[serde(rename = "TAPR-OHL-1.0")]
    TaprOhl10,
    #[serde(rename = "TCL")]
    Tcl,
    #[serde(rename = "TCP-wrappers")]
    TcpWrappers,
    TermReadKey,
    TMate,
    #[serde(rename = "TORQUE-1.1")]
    Torque11,
    #[serde(rename = "TOSL")]
    Tosl,
    #[serde(rename = "TPDL")]
    Tpdl,
    #[serde(rename = "TPL-1.0")]
    Tpl10,
    #[serde(rename = "TTWL")]
    Ttwl,
    #[serde(rename = "TU-Berlin-1.0")]
    TuBerlin10,
    #[serde(rename = "TU-Berlin-2.0")]
    TuBerlin20,
    #[serde(rename = "UCAR")]
    Ucar,
    #[serde(rename = "UCL-1.0")]
    Ucl10,
    #[serde(rename = "Unicode-DFS-2015")]
    UnicodeDfs2015,
    #[serde(rename = "Unicode-DFS-2016")]
    UnicodeDfs2016,
    #[serde(rename = "Unicode-TOU")]
    UnicodeTou,
    UnixCrypt,
    Unlicense,
    #[serde(rename = "UPL-1.0")]
    Upl10,
    Vim,
    #[serde(rename = "VOSTROM")]
    Vostrom,
    #[serde(rename = "VSL-1.0")]
    Vsl10,
    #[serde(rename = "W3C")]
    W3c,
    #[serde(rename = "W3C-19980720")]
    W3c19980720,
    #[serde(rename = "W3C-20150513")]
    W3c20150513,
    #[serde(rename = "w3m")]
    W3m,
    #[serde(rename = "Watcom-1.0")]
    Watcom10,
    #[serde(rename = "Widget-Workshop")]
    WidgetWorkshop,
    Wsuipa,
    #[serde(rename = "WTFPL")]
    Wtfpl,
    #[serde(rename = "wxWindows")]
    WxWindows,
    X11,
    #[serde(rename = "X11-distribute-modifications-variant")]
    X11DistributeModificationsVariant,
    #[serde(rename = "Xdebug-1.03")]
    Xdebug103,
    Xerox,
    Xfig,
    #[serde(rename = "XFree86-1.1")]
    XFree8611,
    #[serde(rename = "xinetd")]
    Xinetd,
    #[serde(rename = "xlock")]
    Xlock,
    Xnet,
    #[serde(rename = "xpp")]
    Xpp,
    XSkat,
    #[serde(rename = "YPL-1.0")]
    Ypl10,
    #[serde(rename = "YPL-1.1")]
    Ypl11,
    Zed,
    #[serde(rename = "Zend-2.0")]
    Zend20,
    #[serde(rename = "Zimbra-1.3")]
    Zimbra13,
    #[serde(rename = "Zimbra-1.4")]
    Zimbra14,
    Zlib,
    #[serde(rename = "zlib-acknowledgement")]
    ZlibAcknowledgement,
    #[serde(rename = "ZPL-1.1")]
    Zpl11,
    #[serde(rename = "ZPL-2.0")]
    Zpl20,
    #[serde(rename = "ZPL-2.1")]
    Zpl21,
}
impl From<&SpdxLicenseIds> for SpdxLicenseIds {
    fn from(value: &SpdxLicenseIds) -> Self {
        value.clone()
    }
}
impl ToString for SpdxLicenseIds {
    fn to_string(&self) -> String {
        match *self {
            Self::_0bsd => "0BSD".to_string(),
            Self::Aal => "AAL".to_string(),
            Self::Abstyles => "Abstyles".to_string(),
            Self::AdaCoreDoc => "AdaCore-doc".to_string(),
            Self::Adobe2006 => "Adobe-2006".to_string(),
            Self::AdobeGlyph => "Adobe-Glyph".to_string(),
            Self::Adsl => "ADSL".to_string(),
            Self::Afl11 => "AFL-1.1".to_string(),
            Self::Afl12 => "AFL-1.2".to_string(),
            Self::Afl20 => "AFL-2.0".to_string(),
            Self::Afl21 => "AFL-2.1".to_string(),
            Self::Afl30 => "AFL-3.0".to_string(),
            Self::Afmparse => "Afmparse".to_string(),
            Self::Agpl10 => "AGPL-1.0".to_string(),
            Self::Agpl10Only => "AGPL-1.0-only".to_string(),
            Self::Agpl10OrLater => "AGPL-1.0-or-later".to_string(),
            Self::Agpl30 => "AGPL-3.0".to_string(),
            Self::Agpl30Only => "AGPL-3.0-only".to_string(),
            Self::Agpl30OrLater => "AGPL-3.0-or-later".to_string(),
            Self::Aladdin => "Aladdin".to_string(),
            Self::Amdplpa => "AMDPLPA".to_string(),
            Self::Aml => "AML".to_string(),
            Self::Ampas => "AMPAS".to_string(),
            Self::AntlrPd => "ANTLR-PD".to_string(),
            Self::AntlrPdFallback => "ANTLR-PD-fallback".to_string(),
            Self::Apache10 => "Apache-1.0".to_string(),
            Self::Apache11 => "Apache-1.1".to_string(),
            Self::Apache20 => "Apache-2.0".to_string(),
            Self::Apafml => "APAFML".to_string(),
            Self::Apl10 => "APL-1.0".to_string(),
            Self::AppS2p => "App-s2p".to_string(),
            Self::Apsl10 => "APSL-1.0".to_string(),
            Self::Apsl11 => "APSL-1.1".to_string(),
            Self::Apsl12 => "APSL-1.2".to_string(),
            Self::Apsl20 => "APSL-2.0".to_string(),
            Self::Arphic1999 => "Arphic-1999".to_string(),
            Self::Artistic10 => "Artistic-1.0".to_string(),
            Self::Artistic10Cl8 => "Artistic-1.0-cl8".to_string(),
            Self::Artistic10Perl => "Artistic-1.0-Perl".to_string(),
            Self::Artistic20 => "Artistic-2.0".to_string(),
            Self::AswfDigitalAssets10 => "ASWF-Digital-Assets-1.0".to_string(),
            Self::AswfDigitalAssets11 => "ASWF-Digital-Assets-1.1".to_string(),
            Self::Baekmuk => "Baekmuk".to_string(),
            Self::Bahyph => "Bahyph".to_string(),
            Self::Barr => "Barr".to_string(),
            Self::Beerware => "Beerware".to_string(),
            Self::BitstreamCharter => "Bitstream-Charter".to_string(),
            Self::BitstreamVera => "Bitstream-Vera".to_string(),
            Self::BitTorrent10 => "BitTorrent-1.0".to_string(),
            Self::BitTorrent11 => "BitTorrent-1.1".to_string(),
            Self::Blessing => "blessing".to_string(),
            Self::BlueOak100 => "BlueOak-1.0.0".to_string(),
            Self::BoehmGc => "Boehm-GC".to_string(),
            Self::Borceux => "Borceux".to_string(),
            Self::BrianGladman3Clause => "Brian-Gladman-3-Clause".to_string(),
            Self::Bsd1Clause => "BSD-1-Clause".to_string(),
            Self::Bsd2Clause => "BSD-2-Clause".to_string(),
            Self::Bsd2ClauseFreeBsd => "BSD-2-Clause-FreeBSD".to_string(),
            Self::Bsd2ClauseNetBsd => "BSD-2-Clause-NetBSD".to_string(),
            Self::Bsd2ClausePatent => "BSD-2-Clause-Patent".to_string(),
            Self::Bsd2ClauseViews => "BSD-2-Clause-Views".to_string(),
            Self::Bsd3Clause => "BSD-3-Clause".to_string(),
            Self::Bsd3ClauseAttribution => "BSD-3-Clause-Attribution".to_string(),
            Self::Bsd3ClauseClear => "BSD-3-Clause-Clear".to_string(),
            Self::Bsd3ClauseLbnl => "BSD-3-Clause-LBNL".to_string(),
            Self::Bsd3ClauseModification => "BSD-3-Clause-Modification".to_string(),
            Self::Bsd3ClauseNoMilitaryLicense => "BSD-3-Clause-No-Military-License".to_string(),
            Self::Bsd3ClauseNoNuclearLicense => "BSD-3-Clause-No-Nuclear-License".to_string(),
            Self::Bsd3ClauseNoNuclearLicense2014 => {
                "BSD-3-Clause-No-Nuclear-License-2014".to_string()
            }
            Self::Bsd3ClauseNoNuclearWarranty => "BSD-3-Clause-No-Nuclear-Warranty".to_string(),
            Self::Bsd3ClauseOpenMpi => "BSD-3-Clause-Open-MPI".to_string(),
            Self::Bsd4Clause => "BSD-4-Clause".to_string(),
            Self::Bsd4ClauseShortened => "BSD-4-Clause-Shortened".to_string(),
            Self::Bsd4ClauseUc => "BSD-4-Clause-UC".to_string(),
            Self::Bsd43reno => "BSD-4.3RENO".to_string(),
            Self::Bsd43tahoe => "BSD-4.3TAHOE".to_string(),
            Self::BsdAdvertisingAcknowledgement => "BSD-Advertising-Acknowledgement".to_string(),
            Self::BsdAttributionHpndDisclaimer => "BSD-Attribution-HPND-disclaimer".to_string(),
            Self::BsdProtection => "BSD-Protection".to_string(),
            Self::BsdSourceCode => "BSD-Source-Code".to_string(),
            Self::Bsl10 => "BSL-1.0".to_string(),
            Self::Busl11 => "BUSL-1.1".to_string(),
            Self::Bzip2105 => "bzip2-1.0.5".to_string(),
            Self::Bzip2106 => "bzip2-1.0.6".to_string(),
            Self::CUda10 => "C-UDA-1.0".to_string(),
            Self::Cal10 => "CAL-1.0".to_string(),
            Self::Cal10CombinedWorkException => "CAL-1.0-Combined-Work-Exception".to_string(),
            Self::Caldera => "Caldera".to_string(),
            Self::Catosl11 => "CATOSL-1.1".to_string(),
            Self::CcBy10 => "CC-BY-1.0".to_string(),
            Self::CcBy20 => "CC-BY-2.0".to_string(),
            Self::CcBy25 => "CC-BY-2.5".to_string(),
            Self::CcBy25Au => "CC-BY-2.5-AU".to_string(),
            Self::CcBy30 => "CC-BY-3.0".to_string(),
            Self::CcBy30At => "CC-BY-3.0-AT".to_string(),
            Self::CcBy30De => "CC-BY-3.0-DE".to_string(),
            Self::CcBy30Igo => "CC-BY-3.0-IGO".to_string(),
            Self::CcBy30Nl => "CC-BY-3.0-NL".to_string(),
            Self::CcBy30Us => "CC-BY-3.0-US".to_string(),
            Self::CcBy40 => "CC-BY-4.0".to_string(),
            Self::CcByNc10 => "CC-BY-NC-1.0".to_string(),
            Self::CcByNc20 => "CC-BY-NC-2.0".to_string(),
            Self::CcByNc25 => "CC-BY-NC-2.5".to_string(),
            Self::CcByNc30 => "CC-BY-NC-3.0".to_string(),
            Self::CcByNc30De => "CC-BY-NC-3.0-DE".to_string(),
            Self::CcByNc40 => "CC-BY-NC-4.0".to_string(),
            Self::CcByNcNd10 => "CC-BY-NC-ND-1.0".to_string(),
            Self::CcByNcNd20 => "CC-BY-NC-ND-2.0".to_string(),
            Self::CcByNcNd25 => "CC-BY-NC-ND-2.5".to_string(),
            Self::CcByNcNd30 => "CC-BY-NC-ND-3.0".to_string(),
            Self::CcByNcNd30De => "CC-BY-NC-ND-3.0-DE".to_string(),
            Self::CcByNcNd30Igo => "CC-BY-NC-ND-3.0-IGO".to_string(),
            Self::CcByNcNd40 => "CC-BY-NC-ND-4.0".to_string(),
            Self::CcByNcSa10 => "CC-BY-NC-SA-1.0".to_string(),
            Self::CcByNcSa20 => "CC-BY-NC-SA-2.0".to_string(),
            Self::CcByNcSa20De => "CC-BY-NC-SA-2.0-DE".to_string(),
            Self::CcByNcSa20Fr => "CC-BY-NC-SA-2.0-FR".to_string(),
            Self::CcByNcSa20Uk => "CC-BY-NC-SA-2.0-UK".to_string(),
            Self::CcByNcSa25 => "CC-BY-NC-SA-2.5".to_string(),
            Self::CcByNcSa30 => "CC-BY-NC-SA-3.0".to_string(),
            Self::CcByNcSa30De => "CC-BY-NC-SA-3.0-DE".to_string(),
            Self::CcByNcSa30Igo => "CC-BY-NC-SA-3.0-IGO".to_string(),
            Self::CcByNcSa40 => "CC-BY-NC-SA-4.0".to_string(),
            Self::CcByNd10 => "CC-BY-ND-1.0".to_string(),
            Self::CcByNd20 => "CC-BY-ND-2.0".to_string(),
            Self::CcByNd25 => "CC-BY-ND-2.5".to_string(),
            Self::CcByNd30 => "CC-BY-ND-3.0".to_string(),
            Self::CcByNd30De => "CC-BY-ND-3.0-DE".to_string(),
            Self::CcByNd40 => "CC-BY-ND-4.0".to_string(),
            Self::CcBySa10 => "CC-BY-SA-1.0".to_string(),
            Self::CcBySa20 => "CC-BY-SA-2.0".to_string(),
            Self::CcBySa20Uk => "CC-BY-SA-2.0-UK".to_string(),
            Self::CcBySa21Jp => "CC-BY-SA-2.1-JP".to_string(),
            Self::CcBySa25 => "CC-BY-SA-2.5".to_string(),
            Self::CcBySa30 => "CC-BY-SA-3.0".to_string(),
            Self::CcBySa30At => "CC-BY-SA-3.0-AT".to_string(),
            Self::CcBySa30De => "CC-BY-SA-3.0-DE".to_string(),
            Self::CcBySa30Igo => "CC-BY-SA-3.0-IGO".to_string(),
            Self::CcBySa40 => "CC-BY-SA-4.0".to_string(),
            Self::CcPddc => "CC-PDDC".to_string(),
            Self::Cc010 => "CC0-1.0".to_string(),
            Self::Cddl10 => "CDDL-1.0".to_string(),
            Self::Cddl11 => "CDDL-1.1".to_string(),
            Self::Cdl10 => "CDL-1.0".to_string(),
            Self::CdlaPermissive10 => "CDLA-Permissive-1.0".to_string(),
            Self::CdlaPermissive20 => "CDLA-Permissive-2.0".to_string(),
            Self::CdlaSharing10 => "CDLA-Sharing-1.0".to_string(),
            Self::Cecill10 => "CECILL-1.0".to_string(),
            Self::Cecill11 => "CECILL-1.1".to_string(),
            Self::Cecill20 => "CECILL-2.0".to_string(),
            Self::Cecill21 => "CECILL-2.1".to_string(),
            Self::CecillB => "CECILL-B".to_string(),
            Self::CecillC => "CECILL-C".to_string(),
            Self::CernOhl11 => "CERN-OHL-1.1".to_string(),
            Self::CernOhl12 => "CERN-OHL-1.2".to_string(),
            Self::CernOhlP20 => "CERN-OHL-P-2.0".to_string(),
            Self::CernOhlS20 => "CERN-OHL-S-2.0".to_string(),
            Self::CernOhlW20 => "CERN-OHL-W-2.0".to_string(),
            Self::Cfitsio => "CFITSIO".to_string(),
            Self::Checkmk => "checkmk".to_string(),
            Self::ClArtistic => "ClArtistic".to_string(),
            Self::Clips => "Clips".to_string(),
            Self::CmuMach => "CMU-Mach".to_string(),
            Self::CnriJython => "CNRI-Jython".to_string(),
            Self::CnriPython => "CNRI-Python".to_string(),
            Self::CnriPythonGplCompatible => "CNRI-Python-GPL-Compatible".to_string(),
            Self::Coil10 => "COIL-1.0".to_string(),
            Self::CommunitySpec10 => "Community-Spec-1.0".to_string(),
            Self::Condor11 => "Condor-1.1".to_string(),
            Self::CopyleftNext030 => "copyleft-next-0.3.0".to_string(),
            Self::CopyleftNext031 => "copyleft-next-0.3.1".to_string(),
            Self::CornellLosslessJpeg => "Cornell-Lossless-JPEG".to_string(),
            Self::Cpal10 => "CPAL-1.0".to_string(),
            Self::Cpl10 => "CPL-1.0".to_string(),
            Self::Cpol102 => "CPOL-1.02".to_string(),
            Self::Crossword => "Crossword".to_string(),
            Self::CrystalStacker => "CrystalStacker".to_string(),
            Self::CuaOpl10 => "CUA-OPL-1.0".to_string(),
            Self::Cube => "Cube".to_string(),
            Self::Curl => "curl".to_string(),
            Self::DFsl10 => "D-FSL-1.0".to_string(),
            Self::Diffmark => "diffmark".to_string(),
            Self::DlDeBy20 => "DL-DE-BY-2.0".to_string(),
            Self::Doc => "DOC".to_string(),
            Self::Dotseqn => "Dotseqn".to_string(),
            Self::Drl10 => "DRL-1.0".to_string(),
            Self::Dsdp => "DSDP".to_string(),
            Self::Dtoa => "dtoa".to_string(),
            Self::Dvipdfm => "dvipdfm".to_string(),
            Self::Ecl10 => "ECL-1.0".to_string(),
            Self::Ecl20 => "ECL-2.0".to_string(),
            Self::ECos20 => "eCos-2.0".to_string(),
            Self::Efl10 => "EFL-1.0".to_string(),
            Self::Efl20 => "EFL-2.0".to_string(),
            Self::EGenix => "eGenix".to_string(),
            Self::Elastic20 => "Elastic-2.0".to_string(),
            Self::Entessa => "Entessa".to_string(),
            Self::Epics => "EPICS".to_string(),
            Self::Epl10 => "EPL-1.0".to_string(),
            Self::Epl20 => "EPL-2.0".to_string(),
            Self::ErlPl11 => "ErlPL-1.1".to_string(),
            Self::Etalab20 => "etalab-2.0".to_string(),
            Self::EuDatagrid => "EUDatagrid".to_string(),
            Self::Eupl10 => "EUPL-1.0".to_string(),
            Self::Eupl11 => "EUPL-1.1".to_string(),
            Self::Eupl12 => "EUPL-1.2".to_string(),
            Self::Eurosym => "Eurosym".to_string(),
            Self::Fair => "Fair".to_string(),
            Self::FdkAac => "FDK-AAC".to_string(),
            Self::Frameworx10 => "Frameworx-1.0".to_string(),
            Self::FreeBsdDoc => "FreeBSD-DOC".to_string(),
            Self::FreeImage => "FreeImage".to_string(),
            Self::Fsfap => "FSFAP".to_string(),
            Self::Fsful => "FSFUL".to_string(),
            Self::Fsfullr => "FSFULLR".to_string(),
            Self::Fsfullrwd => "FSFULLRWD".to_string(),
            Self::Ftl => "FTL".to_string(),
            Self::Gd => "GD".to_string(),
            Self::Gfdl11 => "GFDL-1.1".to_string(),
            Self::Gfdl11InvariantsOnly => "GFDL-1.1-invariants-only".to_string(),
            Self::Gfdl11InvariantsOrLater => "GFDL-1.1-invariants-or-later".to_string(),
            Self::Gfdl11NoInvariantsOnly => "GFDL-1.1-no-invariants-only".to_string(),
            Self::Gfdl11NoInvariantsOrLater => "GFDL-1.1-no-invariants-or-later".to_string(),
            Self::Gfdl11Only => "GFDL-1.1-only".to_string(),
            Self::Gfdl11OrLater => "GFDL-1.1-or-later".to_string(),
            Self::Gfdl12 => "GFDL-1.2".to_string(),
            Self::Gfdl12InvariantsOnly => "GFDL-1.2-invariants-only".to_string(),
            Self::Gfdl12InvariantsOrLater => "GFDL-1.2-invariants-or-later".to_string(),
            Self::Gfdl12NoInvariantsOnly => "GFDL-1.2-no-invariants-only".to_string(),
            Self::Gfdl12NoInvariantsOrLater => "GFDL-1.2-no-invariants-or-later".to_string(),
            Self::Gfdl12Only => "GFDL-1.2-only".to_string(),
            Self::Gfdl12OrLater => "GFDL-1.2-or-later".to_string(),
            Self::Gfdl13 => "GFDL-1.3".to_string(),
            Self::Gfdl13InvariantsOnly => "GFDL-1.3-invariants-only".to_string(),
            Self::Gfdl13InvariantsOrLater => "GFDL-1.3-invariants-or-later".to_string(),
            Self::Gfdl13NoInvariantsOnly => "GFDL-1.3-no-invariants-only".to_string(),
            Self::Gfdl13NoInvariantsOrLater => "GFDL-1.3-no-invariants-or-later".to_string(),
            Self::Gfdl13Only => "GFDL-1.3-only".to_string(),
            Self::Gfdl13OrLater => "GFDL-1.3-or-later".to_string(),
            Self::Giftware => "Giftware".to_string(),
            Self::Gl2ps => "GL2PS".to_string(),
            Self::Glide => "Glide".to_string(),
            Self::Glulxe => "Glulxe".to_string(),
            Self::Glwtpl => "GLWTPL".to_string(),
            Self::Gnuplot => "gnuplot".to_string(),
            Self::Gpl10 => "GPL-1.0".to_string(),
            Self::Gpl10p => "GPL-1.0+".to_string(),
            Self::Gpl10Only => "GPL-1.0-only".to_string(),
            Self::Gpl10OrLater => "GPL-1.0-or-later".to_string(),
            Self::Gpl20 => "GPL-2.0".to_string(),
            Self::Gpl20p => "GPL-2.0+".to_string(),
            Self::Gpl20Only => "GPL-2.0-only".to_string(),
            Self::Gpl20OrLater => "GPL-2.0-or-later".to_string(),
            Self::Gpl20WithAutoconfException => "GPL-2.0-with-autoconf-exception".to_string(),
            Self::Gpl20WithBisonException => "GPL-2.0-with-bison-exception".to_string(),
            Self::Gpl20WithClasspathException => "GPL-2.0-with-classpath-exception".to_string(),
            Self::Gpl20WithFontException => "GPL-2.0-with-font-exception".to_string(),
            Self::Gpl20WithGccException => "GPL-2.0-with-GCC-exception".to_string(),
            Self::Gpl30 => "GPL-3.0".to_string(),
            Self::Gpl30p => "GPL-3.0+".to_string(),
            Self::Gpl30Only => "GPL-3.0-only".to_string(),
            Self::Gpl30OrLater => "GPL-3.0-or-later".to_string(),
            Self::Gpl30WithAutoconfException => "GPL-3.0-with-autoconf-exception".to_string(),
            Self::Gpl30WithGccException => "GPL-3.0-with-GCC-exception".to_string(),
            Self::GraphicsGems => "Graphics-Gems".to_string(),
            Self::GSoap13b => "gSOAP-1.3b".to_string(),
            Self::HaskellReport => "HaskellReport".to_string(),
            Self::Hippocratic21 => "Hippocratic-2.1".to_string(),
            Self::Hp1986 => "HP-1986".to_string(),
            Self::Hpnd => "HPND".to_string(),
            Self::HpndExportUs => "HPND-export-US".to_string(),
            Self::HpndMarkusKuhn => "HPND-Markus-Kuhn".to_string(),
            Self::HpndSellVariant => "HPND-sell-variant".to_string(),
            Self::HpndSellVariantMitDisclaimer => "HPND-sell-variant-MIT-disclaimer".to_string(),
            Self::Htmltidy => "HTMLTIDY".to_string(),
            Self::IbmPibs => "IBM-pibs".to_string(),
            Self::Icu => "ICU".to_string(),
            Self::IecCodeComponentsEula => "IEC-Code-Components-EULA".to_string(),
            Self::Ijg => "IJG".to_string(),
            Self::IjgShort => "IJG-short".to_string(),
            Self::ImageMagick => "ImageMagick".to_string(),
            Self::IMatix => "iMatix".to_string(),
            Self::Imlib2 => "Imlib2".to_string(),
            Self::InfoZip => "Info-ZIP".to_string(),
            Self::InnerNet20 => "Inner-Net-2.0".to_string(),
            Self::Intel => "Intel".to_string(),
            Self::IntelAcpi => "Intel-ACPI".to_string(),
            Self::Interbase10 => "Interbase-1.0".to_string(),
            Self::Ipa => "IPA".to_string(),
            Self::Ipl10 => "IPL-1.0".to_string(),
            Self::Isc => "ISC".to_string(),
            Self::Jam => "Jam".to_string(),
            Self::JasPer20 => "JasPer-2.0".to_string(),
            Self::JplImage => "JPL-image".to_string(),
            Self::Jpnic => "JPNIC".to_string(),
            Self::Json => "JSON".to_string(),
            Self::Kazlib => "Kazlib".to_string(),
            Self::KnuthCtan => "Knuth-CTAN".to_string(),
            Self::Lal12 => "LAL-1.2".to_string(),
            Self::Lal13 => "LAL-1.3".to_string(),
            Self::Latex2e => "Latex2e".to_string(),
            Self::Latex2eTranslatedNotice => "Latex2e-translated-notice".to_string(),
            Self::Leptonica => "Leptonica".to_string(),
            Self::Lgpl20 => "LGPL-2.0".to_string(),
            Self::Lgpl20p => "LGPL-2.0+".to_string(),
            Self::Lgpl20Only => "LGPL-2.0-only".to_string(),
            Self::Lgpl20OrLater => "LGPL-2.0-or-later".to_string(),
            Self::Lgpl21 => "LGPL-2.1".to_string(),
            Self::Lgpl21p => "LGPL-2.1+".to_string(),
            Self::Lgpl21Only => "LGPL-2.1-only".to_string(),
            Self::Lgpl21OrLater => "LGPL-2.1-or-later".to_string(),
            Self::Lgpl30 => "LGPL-3.0".to_string(),
            Self::Lgpl30p => "LGPL-3.0+".to_string(),
            Self::Lgpl30Only => "LGPL-3.0-only".to_string(),
            Self::Lgpl30OrLater => "LGPL-3.0-or-later".to_string(),
            Self::Lgpllr => "LGPLLR".to_string(),
            Self::Libpng => "Libpng".to_string(),
            Self::Libpng20 => "libpng-2.0".to_string(),
            Self::Libselinux10 => "libselinux-1.0".to_string(),
            Self::Libtiff => "libtiff".to_string(),
            Self::LibutilDavidNugent => "libutil-David-Nugent".to_string(),
            Self::LiLiQP11 => "LiLiQ-P-1.1".to_string(),
            Self::LiLiQR11 => "LiLiQ-R-1.1".to_string(),
            Self::LiLiQRplus11 => "LiLiQ-Rplus-1.1".to_string(),
            Self::LinuxManPages1Para => "Linux-man-pages-1-para".to_string(),
            Self::LinuxManPagesCopyleft => "Linux-man-pages-copyleft".to_string(),
            Self::LinuxManPagesCopyleft2Para => "Linux-man-pages-copyleft-2-para".to_string(),
            Self::LinuxManPagesCopyleftVar => "Linux-man-pages-copyleft-var".to_string(),
            Self::LinuxOpenIb => "Linux-OpenIB".to_string(),
            Self::Loop => "LOOP".to_string(),
            Self::Lpl10 => "LPL-1.0".to_string(),
            Self::Lpl102 => "LPL-1.02".to_string(),
            Self::Lppl10 => "LPPL-1.0".to_string(),
            Self::Lppl11 => "LPPL-1.1".to_string(),
            Self::Lppl12 => "LPPL-1.2".to_string(),
            Self::Lppl13a => "LPPL-1.3a".to_string(),
            Self::Lppl13c => "LPPL-1.3c".to_string(),
            Self::LzmaSdk911To920 => "LZMA-SDK-9.11-to-9.20".to_string(),
            Self::LzmaSdk922 => "LZMA-SDK-9.22".to_string(),
            Self::MakeIndex => "MakeIndex".to_string(),
            Self::MartinBirgmeier => "Martin-Birgmeier".to_string(),
            Self::Metamail => "metamail".to_string(),
            Self::Minpack => "Minpack".to_string(),
            Self::MirOs => "MirOS".to_string(),
            Self::Mit => "MIT".to_string(),
            Self::Mit0 => "MIT-0".to_string(),
            Self::MitAdvertising => "MIT-advertising".to_string(),
            Self::MitCmu => "MIT-CMU".to_string(),
            Self::MitEnna => "MIT-enna".to_string(),
            Self::MitFeh => "MIT-feh".to_string(),
            Self::MitFestival => "MIT-Festival".to_string(),
            Self::MitModernVariant => "MIT-Modern-Variant".to_string(),
            Self::MitOpenGroup => "MIT-open-group".to_string(),
            Self::MitWu => "MIT-Wu".to_string(),
            Self::Mitnfa => "MITNFA".to_string(),
            Self::Motosoto => "Motosoto".to_string(),
            Self::MpiPermissive => "mpi-permissive".to_string(),
            Self::Mpich2 => "mpich2".to_string(),
            Self::Mpl10 => "MPL-1.0".to_string(),
            Self::Mpl11 => "MPL-1.1".to_string(),
            Self::Mpl20 => "MPL-2.0".to_string(),
            Self::Mpl20NoCopyleftException => "MPL-2.0-no-copyleft-exception".to_string(),
            Self::Mplus => "mplus".to_string(),
            Self::MsLpl => "MS-LPL".to_string(),
            Self::MsPl => "MS-PL".to_string(),
            Self::MsRl => "MS-RL".to_string(),
            Self::Mtll => "MTLL".to_string(),
            Self::MulanPsl10 => "MulanPSL-1.0".to_string(),
            Self::MulanPsl20 => "MulanPSL-2.0".to_string(),
            Self::Multics => "Multics".to_string(),
            Self::Mup => "Mup".to_string(),
            Self::Naist2003 => "NAIST-2003".to_string(),
            Self::Nasa13 => "NASA-1.3".to_string(),
            Self::Naumen => "Naumen".to_string(),
            Self::Nbpl10 => "NBPL-1.0".to_string(),
            Self::NcglUk20 => "NCGL-UK-2.0".to_string(),
            Self::Ncsa => "NCSA".to_string(),
            Self::NetSnmp => "Net-SNMP".to_string(),
            Self::NetCdf => "NetCDF".to_string(),
            Self::Newsletr => "Newsletr".to_string(),
            Self::Ngpl => "NGPL".to_string(),
            Self::Nicta10 => "NICTA-1.0".to_string(),
            Self::NistPd => "NIST-PD".to_string(),
            Self::NistPdFallback => "NIST-PD-fallback".to_string(),
            Self::NistSoftware => "NIST-Software".to_string(),
            Self::Nlod10 => "NLOD-1.0".to_string(),
            Self::Nlod20 => "NLOD-2.0".to_string(),
            Self::Nlpl => "NLPL".to_string(),
            Self::Nokia => "Nokia".to_string(),
            Self::Nosl => "NOSL".to_string(),
            Self::Noweb => "Noweb".to_string(),
            Self::Npl10 => "NPL-1.0".to_string(),
            Self::Npl11 => "NPL-1.1".to_string(),
            Self::Nposl30 => "NPOSL-3.0".to_string(),
            Self::Nrl => "NRL".to_string(),
            Self::Ntp => "NTP".to_string(),
            Self::Ntp0 => "NTP-0".to_string(),
            Self::Nunit => "Nunit".to_string(),
            Self::OUda10 => "O-UDA-1.0".to_string(),
            Self::OcctPl => "OCCT-PL".to_string(),
            Self::Oclc20 => "OCLC-2.0".to_string(),
            Self::ODbL10 => "ODbL-1.0".to_string(),
            Self::OdcBy10 => "ODC-By-1.0".to_string(),
            Self::Offis => "OFFIS".to_string(),
            Self::Ofl10 => "OFL-1.0".to_string(),
            Self::Ofl10NoRfn => "OFL-1.0-no-RFN".to_string(),
            Self::Ofl10Rfn => "OFL-1.0-RFN".to_string(),
            Self::Ofl11 => "OFL-1.1".to_string(),
            Self::Ofl11NoRfn => "OFL-1.1-no-RFN".to_string(),
            Self::Ofl11Rfn => "OFL-1.1-RFN".to_string(),
            Self::Ogc10 => "OGC-1.0".to_string(),
            Self::OgdlTaiwan10 => "OGDL-Taiwan-1.0".to_string(),
            Self::OglCanada20 => "OGL-Canada-2.0".to_string(),
            Self::OglUk10 => "OGL-UK-1.0".to_string(),
            Self::OglUk20 => "OGL-UK-2.0".to_string(),
            Self::OglUk30 => "OGL-UK-3.0".to_string(),
            Self::Ogtsl => "OGTSL".to_string(),
            Self::Oldap11 => "OLDAP-1.1".to_string(),
            Self::Oldap12 => "OLDAP-1.2".to_string(),
            Self::Oldap13 => "OLDAP-1.3".to_string(),
            Self::Oldap14 => "OLDAP-1.4".to_string(),
            Self::Oldap20 => "OLDAP-2.0".to_string(),
            Self::Oldap201 => "OLDAP-2.0.1".to_string(),
            Self::Oldap21 => "OLDAP-2.1".to_string(),
            Self::Oldap22 => "OLDAP-2.2".to_string(),
            Self::Oldap221 => "OLDAP-2.2.1".to_string(),
            Self::Oldap222 => "OLDAP-2.2.2".to_string(),
            Self::Oldap23 => "OLDAP-2.3".to_string(),
            Self::Oldap24 => "OLDAP-2.4".to_string(),
            Self::Oldap25 => "OLDAP-2.5".to_string(),
            Self::Oldap26 => "OLDAP-2.6".to_string(),
            Self::Oldap27 => "OLDAP-2.7".to_string(),
            Self::Oldap28 => "OLDAP-2.8".to_string(),
            Self::Olfl13 => "OLFL-1.3".to_string(),
            Self::Oml => "OML".to_string(),
            Self::OpenPbs23 => "OpenPBS-2.3".to_string(),
            Self::OpenSsl => "OpenSSL".to_string(),
            Self::Opl10 => "OPL-1.0".to_string(),
            Self::OplUk30 => "OPL-UK-3.0".to_string(),
            Self::Opubl10 => "OPUBL-1.0".to_string(),
            Self::OsetPl21 => "OSET-PL-2.1".to_string(),
            Self::Osl10 => "OSL-1.0".to_string(),
            Self::Osl11 => "OSL-1.1".to_string(),
            Self::Osl20 => "OSL-2.0".to_string(),
            Self::Osl21 => "OSL-2.1".to_string(),
            Self::Osl30 => "OSL-3.0".to_string(),
            Self::Parity600 => "Parity-6.0.0".to_string(),
            Self::Parity700 => "Parity-7.0.0".to_string(),
            Self::Pddl10 => "PDDL-1.0".to_string(),
            Self::Php30 => "PHP-3.0".to_string(),
            Self::Php301 => "PHP-3.01".to_string(),
            Self::Plexus => "Plexus".to_string(),
            Self::PolyFormNoncommercial100 => "PolyForm-Noncommercial-1.0.0".to_string(),
            Self::PolyFormSmallBusiness100 => "PolyForm-Small-Business-1.0.0".to_string(),
            Self::PostgreSql => "PostgreSQL".to_string(),
            Self::Psf20 => "PSF-2.0".to_string(),
            Self::Psfrag => "psfrag".to_string(),
            Self::Psutils => "psutils".to_string(),
            Self::Python20 => "Python-2.0".to_string(),
            Self::Python201 => "Python-2.0.1".to_string(),
            Self::Qhull => "Qhull".to_string(),
            Self::Qpl10 => "QPL-1.0".to_string(),
            Self::Qpl10Inria2004 => "QPL-1.0-INRIA-2004".to_string(),
            Self::Rdisc => "Rdisc".to_string(),
            Self::RHeCos11 => "RHeCos-1.1".to_string(),
            Self::Rpl11 => "RPL-1.1".to_string(),
            Self::Rpl15 => "RPL-1.5".to_string(),
            Self::Rpsl10 => "RPSL-1.0".to_string(),
            Self::RsaMd => "RSA-MD".to_string(),
            Self::Rscpl => "RSCPL".to_string(),
            Self::Ruby => "Ruby".to_string(),
            Self::SaxPd => "SAX-PD".to_string(),
            Self::Saxpath => "Saxpath".to_string(),
            Self::Scea => "SCEA".to_string(),
            Self::SchemeReport => "SchemeReport".to_string(),
            Self::Sendmail => "Sendmail".to_string(),
            Self::Sendmail823 => "Sendmail-8.23".to_string(),
            Self::SgiB10 => "SGI-B-1.0".to_string(),
            Self::SgiB11 => "SGI-B-1.1".to_string(),
            Self::SgiB20 => "SGI-B-2.0".to_string(),
            Self::Sgp4 => "SGP4".to_string(),
            Self::Shl05 => "SHL-0.5".to_string(),
            Self::Shl051 => "SHL-0.51".to_string(),
            Self::SimPl20 => "SimPL-2.0".to_string(),
            Self::Sissl => "SISSL".to_string(),
            Self::Sissl12 => "SISSL-1.2".to_string(),
            Self::Sleepycat => "Sleepycat".to_string(),
            Self::Smlnj => "SMLNJ".to_string(),
            Self::Smppl => "SMPPL".to_string(),
            Self::Snia => "SNIA".to_string(),
            Self::Snprintf => "snprintf".to_string(),
            Self::Spencer86 => "Spencer-86".to_string(),
            Self::Spencer94 => "Spencer-94".to_string(),
            Self::Spencer99 => "Spencer-99".to_string(),
            Self::Spl10 => "SPL-1.0".to_string(),
            Self::SshOpenSsh => "SSH-OpenSSH".to_string(),
            Self::SshShort => "SSH-short".to_string(),
            Self::Sspl10 => "SSPL-1.0".to_string(),
            Self::StandardMlNj => "StandardML-NJ".to_string(),
            Self::SugarCrm113 => "SugarCRM-1.1.3".to_string(),
            Self::SunPro => "SunPro".to_string(),
            Self::Swl => "SWL".to_string(),
            Self::Symlinks => "Symlinks".to_string(),
            Self::TaprOhl10 => "TAPR-OHL-1.0".to_string(),
            Self::Tcl => "TCL".to_string(),
            Self::TcpWrappers => "TCP-wrappers".to_string(),
            Self::TermReadKey => "TermReadKey".to_string(),
            Self::TMate => "TMate".to_string(),
            Self::Torque11 => "TORQUE-1.1".to_string(),
            Self::Tosl => "TOSL".to_string(),
            Self::Tpdl => "TPDL".to_string(),
            Self::Tpl10 => "TPL-1.0".to_string(),
            Self::Ttwl => "TTWL".to_string(),
            Self::TuBerlin10 => "TU-Berlin-1.0".to_string(),
            Self::TuBerlin20 => "TU-Berlin-2.0".to_string(),
            Self::Ucar => "UCAR".to_string(),
            Self::Ucl10 => "UCL-1.0".to_string(),
            Self::UnicodeDfs2015 => "Unicode-DFS-2015".to_string(),
            Self::UnicodeDfs2016 => "Unicode-DFS-2016".to_string(),
            Self::UnicodeTou => "Unicode-TOU".to_string(),
            Self::UnixCrypt => "UnixCrypt".to_string(),
            Self::Unlicense => "Unlicense".to_string(),
            Self::Upl10 => "UPL-1.0".to_string(),
            Self::Vim => "Vim".to_string(),
            Self::Vostrom => "VOSTROM".to_string(),
            Self::Vsl10 => "VSL-1.0".to_string(),
            Self::W3c => "W3C".to_string(),
            Self::W3c19980720 => "W3C-19980720".to_string(),
            Self::W3c20150513 => "W3C-20150513".to_string(),
            Self::W3m => "w3m".to_string(),
            Self::Watcom10 => "Watcom-1.0".to_string(),
            Self::WidgetWorkshop => "Widget-Workshop".to_string(),
            Self::Wsuipa => "Wsuipa".to_string(),
            Self::Wtfpl => "WTFPL".to_string(),
            Self::WxWindows => "wxWindows".to_string(),
            Self::X11 => "X11".to_string(),
            Self::X11DistributeModificationsVariant => {
                "X11-distribute-modifications-variant".to_string()
            }
            Self::Xdebug103 => "Xdebug-1.03".to_string(),
            Self::Xerox => "Xerox".to_string(),
            Self::Xfig => "Xfig".to_string(),
            Self::XFree8611 => "XFree86-1.1".to_string(),
            Self::Xinetd => "xinetd".to_string(),
            Self::Xlock => "xlock".to_string(),
            Self::Xnet => "Xnet".to_string(),
            Self::Xpp => "xpp".to_string(),
            Self::XSkat => "XSkat".to_string(),
            Self::Ypl10 => "YPL-1.0".to_string(),
            Self::Ypl11 => "YPL-1.1".to_string(),
            Self::Zed => "Zed".to_string(),
            Self::Zend20 => "Zend-2.0".to_string(),
            Self::Zimbra13 => "Zimbra-1.3".to_string(),
            Self::Zimbra14 => "Zimbra-1.4".to_string(),
            Self::Zlib => "Zlib".to_string(),
            Self::ZlibAcknowledgement => "zlib-acknowledgement".to_string(),
            Self::Zpl11 => "ZPL-1.1".to_string(),
            Self::Zpl20 => "ZPL-2.0".to_string(),
            Self::Zpl21 => "ZPL-2.1".to_string(),
        }
    }
}
impl std::str::FromStr for SpdxLicenseIds {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> Result<Self, self::error::ConversionError> {
        match value {
            "0BSD" => Ok(Self::_0bsd),
            "AAL" => Ok(Self::Aal),
            "Abstyles" => Ok(Self::Abstyles),
            "AdaCore-doc" => Ok(Self::AdaCoreDoc),
            "Adobe-2006" => Ok(Self::Adobe2006),
            "Adobe-Glyph" => Ok(Self::AdobeGlyph),
            "ADSL" => Ok(Self::Adsl),
            "AFL-1.1" => Ok(Self::Afl11),
            "AFL-1.2" => Ok(Self::Afl12),
            "AFL-2.0" => Ok(Self::Afl20),
            "AFL-2.1" => Ok(Self::Afl21),
            "AFL-3.0" => Ok(Self::Afl30),
            "Afmparse" => Ok(Self::Afmparse),
            "AGPL-1.0" => Ok(Self::Agpl10),
            "AGPL-1.0-only" => Ok(Self::Agpl10Only),
            "AGPL-1.0-or-later" => Ok(Self::Agpl10OrLater),
            "AGPL-3.0" => Ok(Self::Agpl30),
            "AGPL-3.0-only" => Ok(Self::Agpl30Only),
            "AGPL-3.0-or-later" => Ok(Self::Agpl30OrLater),
            "Aladdin" => Ok(Self::Aladdin),
            "AMDPLPA" => Ok(Self::Amdplpa),
            "AML" => Ok(Self::Aml),
            "AMPAS" => Ok(Self::Ampas),
            "ANTLR-PD" => Ok(Self::AntlrPd),
            "ANTLR-PD-fallback" => Ok(Self::AntlrPdFallback),
            "Apache-1.0" => Ok(Self::Apache10),
            "Apache-1.1" => Ok(Self::Apache11),
            "Apache-2.0" => Ok(Self::Apache20),
            "APAFML" => Ok(Self::Apafml),
            "APL-1.0" => Ok(Self::Apl10),
            "App-s2p" => Ok(Self::AppS2p),
            "APSL-1.0" => Ok(Self::Apsl10),
            "APSL-1.1" => Ok(Self::Apsl11),
            "APSL-1.2" => Ok(Self::Apsl12),
            "APSL-2.0" => Ok(Self::Apsl20),
            "Arphic-1999" => Ok(Self::Arphic1999),
            "Artistic-1.0" => Ok(Self::Artistic10),
            "Artistic-1.0-cl8" => Ok(Self::Artistic10Cl8),
            "Artistic-1.0-Perl" => Ok(Self::Artistic10Perl),
            "Artistic-2.0" => Ok(Self::Artistic20),
            "ASWF-Digital-Assets-1.0" => Ok(Self::AswfDigitalAssets10),
            "ASWF-Digital-Assets-1.1" => Ok(Self::AswfDigitalAssets11),
            "Baekmuk" => Ok(Self::Baekmuk),
            "Bahyph" => Ok(Self::Bahyph),
            "Barr" => Ok(Self::Barr),
            "Beerware" => Ok(Self::Beerware),
            "Bitstream-Charter" => Ok(Self::BitstreamCharter),
            "Bitstream-Vera" => Ok(Self::BitstreamVera),
            "BitTorrent-1.0" => Ok(Self::BitTorrent10),
            "BitTorrent-1.1" => Ok(Self::BitTorrent11),
            "blessing" => Ok(Self::Blessing),
            "BlueOak-1.0.0" => Ok(Self::BlueOak100),
            "Boehm-GC" => Ok(Self::BoehmGc),
            "Borceux" => Ok(Self::Borceux),
            "Brian-Gladman-3-Clause" => Ok(Self::BrianGladman3Clause),
            "BSD-1-Clause" => Ok(Self::Bsd1Clause),
            "BSD-2-Clause" => Ok(Self::Bsd2Clause),
            "BSD-2-Clause-FreeBSD" => Ok(Self::Bsd2ClauseFreeBsd),
            "BSD-2-Clause-NetBSD" => Ok(Self::Bsd2ClauseNetBsd),
            "BSD-2-Clause-Patent" => Ok(Self::Bsd2ClausePatent),
            "BSD-2-Clause-Views" => Ok(Self::Bsd2ClauseViews),
            "BSD-3-Clause" => Ok(Self::Bsd3Clause),
            "BSD-3-Clause-Attribution" => Ok(Self::Bsd3ClauseAttribution),
            "BSD-3-Clause-Clear" => Ok(Self::Bsd3ClauseClear),
            "BSD-3-Clause-LBNL" => Ok(Self::Bsd3ClauseLbnl),
            "BSD-3-Clause-Modification" => Ok(Self::Bsd3ClauseModification),
            "BSD-3-Clause-No-Military-License" => Ok(Self::Bsd3ClauseNoMilitaryLicense),
            "BSD-3-Clause-No-Nuclear-License" => Ok(Self::Bsd3ClauseNoNuclearLicense),
            "BSD-3-Clause-No-Nuclear-License-2014" => Ok(Self::Bsd3ClauseNoNuclearLicense2014),
            "BSD-3-Clause-No-Nuclear-Warranty" => Ok(Self::Bsd3ClauseNoNuclearWarranty),
            "BSD-3-Clause-Open-MPI" => Ok(Self::Bsd3ClauseOpenMpi),
            "BSD-4-Clause" => Ok(Self::Bsd4Clause),
            "BSD-4-Clause-Shortened" => Ok(Self::Bsd4ClauseShortened),
            "BSD-4-Clause-UC" => Ok(Self::Bsd4ClauseUc),
            "BSD-4.3RENO" => Ok(Self::Bsd43reno),
            "BSD-4.3TAHOE" => Ok(Self::Bsd43tahoe),
            "BSD-Advertising-Acknowledgement" => Ok(Self::BsdAdvertisingAcknowledgement),
            "BSD-Attribution-HPND-disclaimer" => Ok(Self::BsdAttributionHpndDisclaimer),
            "BSD-Protection" => Ok(Self::BsdProtection),
            "BSD-Source-Code" => Ok(Self::BsdSourceCode),
            "BSL-1.0" => Ok(Self::Bsl10),
            "BUSL-1.1" => Ok(Self::Busl11),
            "bzip2-1.0.5" => Ok(Self::Bzip2105),
            "bzip2-1.0.6" => Ok(Self::Bzip2106),
            "C-UDA-1.0" => Ok(Self::CUda10),
            "CAL-1.0" => Ok(Self::Cal10),
            "CAL-1.0-Combined-Work-Exception" => Ok(Self::Cal10CombinedWorkException),
            "Caldera" => Ok(Self::Caldera),
            "CATOSL-1.1" => Ok(Self::Catosl11),
            "CC-BY-1.0" => Ok(Self::CcBy10),
            "CC-BY-2.0" => Ok(Self::CcBy20),
            "CC-BY-2.5" => Ok(Self::CcBy25),
            "CC-BY-2.5-AU" => Ok(Self::CcBy25Au),
            "CC-BY-3.0" => Ok(Self::CcBy30),
            "CC-BY-3.0-AT" => Ok(Self::CcBy30At),
            "CC-BY-3.0-DE" => Ok(Self::CcBy30De),
            "CC-BY-3.0-IGO" => Ok(Self::CcBy30Igo),
            "CC-BY-3.0-NL" => Ok(Self::CcBy30Nl),
            "CC-BY-3.0-US" => Ok(Self::CcBy30Us),
            "CC-BY-4.0" => Ok(Self::CcBy40),
            "CC-BY-NC-1.0" => Ok(Self::CcByNc10),
            "CC-BY-NC-2.0" => Ok(Self::CcByNc20),
            "CC-BY-NC-2.5" => Ok(Self::CcByNc25),
            "CC-BY-NC-3.0" => Ok(Self::CcByNc30),
            "CC-BY-NC-3.0-DE" => Ok(Self::CcByNc30De),
            "CC-BY-NC-4.0" => Ok(Self::CcByNc40),
            "CC-BY-NC-ND-1.0" => Ok(Self::CcByNcNd10),
            "CC-BY-NC-ND-2.0" => Ok(Self::CcByNcNd20),
            "CC-BY-NC-ND-2.5" => Ok(Self::CcByNcNd25),
            "CC-BY-NC-ND-3.0" => Ok(Self::CcByNcNd30),
            "CC-BY-NC-ND-3.0-DE" => Ok(Self::CcByNcNd30De),
            "CC-BY-NC-ND-3.0-IGO" => Ok(Self::CcByNcNd30Igo),
            "CC-BY-NC-ND-4.0" => Ok(Self::CcByNcNd40),
            "CC-BY-NC-SA-1.0" => Ok(Self::CcByNcSa10),
            "CC-BY-NC-SA-2.0" => Ok(Self::CcByNcSa20),
            "CC-BY-NC-SA-2.0-DE" => Ok(Self::CcByNcSa20De),
            "CC-BY-NC-SA-2.0-FR" => Ok(Self::CcByNcSa20Fr),
            "CC-BY-NC-SA-2.0-UK" => Ok(Self::CcByNcSa20Uk),
            "CC-BY-NC-SA-2.5" => Ok(Self::CcByNcSa25),
            "CC-BY-NC-SA-3.0" => Ok(Self::CcByNcSa30),
            "CC-BY-NC-SA-3.0-DE" => Ok(Self::CcByNcSa30De),
            "CC-BY-NC-SA-3.0-IGO" => Ok(Self::CcByNcSa30Igo),
            "CC-BY-NC-SA-4.0" => Ok(Self::CcByNcSa40),
            "CC-BY-ND-1.0" => Ok(Self::CcByNd10),
            "CC-BY-ND-2.0" => Ok(Self::CcByNd20),
            "CC-BY-ND-2.5" => Ok(Self::CcByNd25),
            "CC-BY-ND-3.0" => Ok(Self::CcByNd30),
            "CC-BY-ND-3.0-DE" => Ok(Self::CcByNd30De),
            "CC-BY-ND-4.0" => Ok(Self::CcByNd40),
            "CC-BY-SA-1.0" => Ok(Self::CcBySa10),
            "CC-BY-SA-2.0" => Ok(Self::CcBySa20),
            "CC-BY-SA-2.0-UK" => Ok(Self::CcBySa20Uk),
            "CC-BY-SA-2.1-JP" => Ok(Self::CcBySa21Jp),
            "CC-BY-SA-2.5" => Ok(Self::CcBySa25),
            "CC-BY-SA-3.0" => Ok(Self::CcBySa30),
            "CC-BY-SA-3.0-AT" => Ok(Self::CcBySa30At),
            "CC-BY-SA-3.0-DE" => Ok(Self::CcBySa30De),
            "CC-BY-SA-3.0-IGO" => Ok(Self::CcBySa30Igo),
            "CC-BY-SA-4.0" => Ok(Self::CcBySa40),
            "CC-PDDC" => Ok(Self::CcPddc),
            "CC0-1.0" => Ok(Self::Cc010),
            "CDDL-1.0" => Ok(Self::Cddl10),
            "CDDL-1.1" => Ok(Self::Cddl11),
            "CDL-1.0" => Ok(Self::Cdl10),
            "CDLA-Permissive-1.0" => Ok(Self::CdlaPermissive10),
            "CDLA-Permissive-2.0" => Ok(Self::CdlaPermissive20),
            "CDLA-Sharing-1.0" => Ok(Self::CdlaSharing10),
            "CECILL-1.0" => Ok(Self::Cecill10),
            "CECILL-1.1" => Ok(Self::Cecill11),
            "CECILL-2.0" => Ok(Self::Cecill20),
            "CECILL-2.1" => Ok(Self::Cecill21),
            "CECILL-B" => Ok(Self::CecillB),
            "CECILL-C" => Ok(Self::CecillC),
            "CERN-OHL-1.1" => Ok(Self::CernOhl11),
            "CERN-OHL-1.2" => Ok(Self::CernOhl12),
            "CERN-OHL-P-2.0" => Ok(Self::CernOhlP20),
            "CERN-OHL-S-2.0" => Ok(Self::CernOhlS20),
            "CERN-OHL-W-2.0" => Ok(Self::CernOhlW20),
            "CFITSIO" => Ok(Self::Cfitsio),
            "checkmk" => Ok(Self::Checkmk),
            "ClArtistic" => Ok(Self::ClArtistic),
            "Clips" => Ok(Self::Clips),
            "CMU-Mach" => Ok(Self::CmuMach),
            "CNRI-Jython" => Ok(Self::CnriJython),
            "CNRI-Python" => Ok(Self::CnriPython),
            "CNRI-Python-GPL-Compatible" => Ok(Self::CnriPythonGplCompatible),
            "COIL-1.0" => Ok(Self::Coil10),
            "Community-Spec-1.0" => Ok(Self::CommunitySpec10),
            "Condor-1.1" => Ok(Self::Condor11),
            "copyleft-next-0.3.0" => Ok(Self::CopyleftNext030),
            "copyleft-next-0.3.1" => Ok(Self::CopyleftNext031),
            "Cornell-Lossless-JPEG" => Ok(Self::CornellLosslessJpeg),
            "CPAL-1.0" => Ok(Self::Cpal10),
            "CPL-1.0" => Ok(Self::Cpl10),
            "CPOL-1.02" => Ok(Self::Cpol102),
            "Crossword" => Ok(Self::Crossword),
            "CrystalStacker" => Ok(Self::CrystalStacker),
            "CUA-OPL-1.0" => Ok(Self::CuaOpl10),
            "Cube" => Ok(Self::Cube),
            "curl" => Ok(Self::Curl),
            "D-FSL-1.0" => Ok(Self::DFsl10),
            "diffmark" => Ok(Self::Diffmark),
            "DL-DE-BY-2.0" => Ok(Self::DlDeBy20),
            "DOC" => Ok(Self::Doc),
            "Dotseqn" => Ok(Self::Dotseqn),
            "DRL-1.0" => Ok(Self::Drl10),
            "DSDP" => Ok(Self::Dsdp),
            "dtoa" => Ok(Self::Dtoa),
            "dvipdfm" => Ok(Self::Dvipdfm),
            "ECL-1.0" => Ok(Self::Ecl10),
            "ECL-2.0" => Ok(Self::Ecl20),
            "eCos-2.0" => Ok(Self::ECos20),
            "EFL-1.0" => Ok(Self::Efl10),
            "EFL-2.0" => Ok(Self::Efl20),
            "eGenix" => Ok(Self::EGenix),
            "Elastic-2.0" => Ok(Self::Elastic20),
            "Entessa" => Ok(Self::Entessa),
            "EPICS" => Ok(Self::Epics),
            "EPL-1.0" => Ok(Self::Epl10),
            "EPL-2.0" => Ok(Self::Epl20),
            "ErlPL-1.1" => Ok(Self::ErlPl11),
            "etalab-2.0" => Ok(Self::Etalab20),
            "EUDatagrid" => Ok(Self::EuDatagrid),
            "EUPL-1.0" => Ok(Self::Eupl10),
            "EUPL-1.1" => Ok(Self::Eupl11),
            "EUPL-1.2" => Ok(Self::Eupl12),
            "Eurosym" => Ok(Self::Eurosym),
            "Fair" => Ok(Self::Fair),
            "FDK-AAC" => Ok(Self::FdkAac),
            "Frameworx-1.0" => Ok(Self::Frameworx10),
            "FreeBSD-DOC" => Ok(Self::FreeBsdDoc),
            "FreeImage" => Ok(Self::FreeImage),
            "FSFAP" => Ok(Self::Fsfap),
            "FSFUL" => Ok(Self::Fsful),
            "FSFULLR" => Ok(Self::Fsfullr),
            "FSFULLRWD" => Ok(Self::Fsfullrwd),
            "FTL" => Ok(Self::Ftl),
            "GD" => Ok(Self::Gd),
            "GFDL-1.1" => Ok(Self::Gfdl11),
            "GFDL-1.1-invariants-only" => Ok(Self::Gfdl11InvariantsOnly),
            "GFDL-1.1-invariants-or-later" => Ok(Self::Gfdl11InvariantsOrLater),
            "GFDL-1.1-no-invariants-only" => Ok(Self::Gfdl11NoInvariantsOnly),
            "GFDL-1.1-no-invariants-or-later" => Ok(Self::Gfdl11NoInvariantsOrLater),
            "GFDL-1.1-only" => Ok(Self::Gfdl11Only),
            "GFDL-1.1-or-later" => Ok(Self::Gfdl11OrLater),
            "GFDL-1.2" => Ok(Self::Gfdl12),
            "GFDL-1.2-invariants-only" => Ok(Self::Gfdl12InvariantsOnly),
            "GFDL-1.2-invariants-or-later" => Ok(Self::Gfdl12InvariantsOrLater),
            "GFDL-1.2-no-invariants-only" => Ok(Self::Gfdl12NoInvariantsOnly),
            "GFDL-1.2-no-invariants-or-later" => Ok(Self::Gfdl12NoInvariantsOrLater),
            "GFDL-1.2-only" => Ok(Self::Gfdl12Only),
            "GFDL-1.2-or-later" => Ok(Self::Gfdl12OrLater),
            "GFDL-1.3" => Ok(Self::Gfdl13),
            "GFDL-1.3-invariants-only" => Ok(Self::Gfdl13InvariantsOnly),
            "GFDL-1.3-invariants-or-later" => Ok(Self::Gfdl13InvariantsOrLater),
            "GFDL-1.3-no-invariants-only" => Ok(Self::Gfdl13NoInvariantsOnly),
            "GFDL-1.3-no-invariants-or-later" => Ok(Self::Gfdl13NoInvariantsOrLater),
            "GFDL-1.3-only" => Ok(Self::Gfdl13Only),
            "GFDL-1.3-or-later" => Ok(Self::Gfdl13OrLater),
            "Giftware" => Ok(Self::Giftware),
            "GL2PS" => Ok(Self::Gl2ps),
            "Glide" => Ok(Self::Glide),
            "Glulxe" => Ok(Self::Glulxe),
            "GLWTPL" => Ok(Self::Glwtpl),
            "gnuplot" => Ok(Self::Gnuplot),
            "GPL-1.0" => Ok(Self::Gpl10),
            "GPL-1.0+" => Ok(Self::Gpl10),
            "GPL-1.0-only" => Ok(Self::Gpl10Only),
            "GPL-1.0-or-later" => Ok(Self::Gpl10OrLater),
            "GPL-2.0" => Ok(Self::Gpl20),
            "GPL-2.0+" => Ok(Self::Gpl20),
            "GPL-2.0-only" => Ok(Self::Gpl20Only),
            "GPL-2.0-or-later" => Ok(Self::Gpl20OrLater),
            "GPL-2.0-with-autoconf-exception" => Ok(Self::Gpl20WithAutoconfException),
            "GPL-2.0-with-bison-exception" => Ok(Self::Gpl20WithBisonException),
            "GPL-2.0-with-classpath-exception" => Ok(Self::Gpl20WithClasspathException),
            "GPL-2.0-with-font-exception" => Ok(Self::Gpl20WithFontException),
            "GPL-2.0-with-GCC-exception" => Ok(Self::Gpl20WithGccException),
            "GPL-3.0" => Ok(Self::Gpl30),
            "GPL-3.0+" => Ok(Self::Gpl30),
            "GPL-3.0-only" => Ok(Self::Gpl30Only),
            "GPL-3.0-or-later" => Ok(Self::Gpl30OrLater),
            "GPL-3.0-with-autoconf-exception" => Ok(Self::Gpl30WithAutoconfException),
            "GPL-3.0-with-GCC-exception" => Ok(Self::Gpl30WithGccException),
            "Graphics-Gems" => Ok(Self::GraphicsGems),
            "gSOAP-1.3b" => Ok(Self::GSoap13b),
            "HaskellReport" => Ok(Self::HaskellReport),
            "Hippocratic-2.1" => Ok(Self::Hippocratic21),
            "HP-1986" => Ok(Self::Hp1986),
            "HPND" => Ok(Self::Hpnd),
            "HPND-export-US" => Ok(Self::HpndExportUs),
            "HPND-Markus-Kuhn" => Ok(Self::HpndMarkusKuhn),
            "HPND-sell-variant" => Ok(Self::HpndSellVariant),
            "HPND-sell-variant-MIT-disclaimer" => Ok(Self::HpndSellVariantMitDisclaimer),
            "HTMLTIDY" => Ok(Self::Htmltidy),
            "IBM-pibs" => Ok(Self::IbmPibs),
            "ICU" => Ok(Self::Icu),
            "IEC-Code-Components-EULA" => Ok(Self::IecCodeComponentsEula),
            "IJG" => Ok(Self::Ijg),
            "IJG-short" => Ok(Self::IjgShort),
            "ImageMagick" => Ok(Self::ImageMagick),
            "iMatix" => Ok(Self::IMatix),
            "Imlib2" => Ok(Self::Imlib2),
            "Info-ZIP" => Ok(Self::InfoZip),
            "Inner-Net-2.0" => Ok(Self::InnerNet20),
            "Intel" => Ok(Self::Intel),
            "Intel-ACPI" => Ok(Self::IntelAcpi),
            "Interbase-1.0" => Ok(Self::Interbase10),
            "IPA" => Ok(Self::Ipa),
            "IPL-1.0" => Ok(Self::Ipl10),
            "ISC" => Ok(Self::Isc),
            "Jam" => Ok(Self::Jam),
            "JasPer-2.0" => Ok(Self::JasPer20),
            "JPL-image" => Ok(Self::JplImage),
            "JPNIC" => Ok(Self::Jpnic),
            "JSON" => Ok(Self::Json),
            "Kazlib" => Ok(Self::Kazlib),
            "Knuth-CTAN" => Ok(Self::KnuthCtan),
            "LAL-1.2" => Ok(Self::Lal12),
            "LAL-1.3" => Ok(Self::Lal13),
            "Latex2e" => Ok(Self::Latex2e),
            "Latex2e-translated-notice" => Ok(Self::Latex2eTranslatedNotice),
            "Leptonica" => Ok(Self::Leptonica),
            "LGPL-2.0" => Ok(Self::Lgpl20),
            "LGPL-2.0+" => Ok(Self::Lgpl20),
            "LGPL-2.0-only" => Ok(Self::Lgpl20Only),
            "LGPL-2.0-or-later" => Ok(Self::Lgpl20OrLater),
            "LGPL-2.1" => Ok(Self::Lgpl21),
            "LGPL-2.1+" => Ok(Self::Lgpl21),
            "LGPL-2.1-only" => Ok(Self::Lgpl21Only),
            "LGPL-2.1-or-later" => Ok(Self::Lgpl21OrLater),
            "LGPL-3.0" => Ok(Self::Lgpl30),
            "LGPL-3.0+" => Ok(Self::Lgpl30),
            "LGPL-3.0-only" => Ok(Self::Lgpl30Only),
            "LGPL-3.0-or-later" => Ok(Self::Lgpl30OrLater),
            "LGPLLR" => Ok(Self::Lgpllr),
            "Libpng" => Ok(Self::Libpng),
            "libpng-2.0" => Ok(Self::Libpng20),
            "libselinux-1.0" => Ok(Self::Libselinux10),
            "libtiff" => Ok(Self::Libtiff),
            "libutil-David-Nugent" => Ok(Self::LibutilDavidNugent),
            "LiLiQ-P-1.1" => Ok(Self::LiLiQP11),
            "LiLiQ-R-1.1" => Ok(Self::LiLiQR11),
            "LiLiQ-Rplus-1.1" => Ok(Self::LiLiQRplus11),
            "Linux-man-pages-1-para" => Ok(Self::LinuxManPages1Para),
            "Linux-man-pages-copyleft" => Ok(Self::LinuxManPagesCopyleft),
            "Linux-man-pages-copyleft-2-para" => Ok(Self::LinuxManPagesCopyleft2Para),
            "Linux-man-pages-copyleft-var" => Ok(Self::LinuxManPagesCopyleftVar),
            "Linux-OpenIB" => Ok(Self::LinuxOpenIb),
            "LOOP" => Ok(Self::Loop),
            "LPL-1.0" => Ok(Self::Lpl10),
            "LPL-1.02" => Ok(Self::Lpl102),
            "LPPL-1.0" => Ok(Self::Lppl10),
            "LPPL-1.1" => Ok(Self::Lppl11),
            "LPPL-1.2" => Ok(Self::Lppl12),
            "LPPL-1.3a" => Ok(Self::Lppl13a),
            "LPPL-1.3c" => Ok(Self::Lppl13c),
            "LZMA-SDK-9.11-to-9.20" => Ok(Self::LzmaSdk911To920),
            "LZMA-SDK-9.22" => Ok(Self::LzmaSdk922),
            "MakeIndex" => Ok(Self::MakeIndex),
            "Martin-Birgmeier" => Ok(Self::MartinBirgmeier),
            "metamail" => Ok(Self::Metamail),
            "Minpack" => Ok(Self::Minpack),
            "MirOS" => Ok(Self::MirOs),
            "MIT" => Ok(Self::Mit),
            "MIT-0" => Ok(Self::Mit0),
            "MIT-advertising" => Ok(Self::MitAdvertising),
            "MIT-CMU" => Ok(Self::MitCmu),
            "MIT-enna" => Ok(Self::MitEnna),
            "MIT-feh" => Ok(Self::MitFeh),
            "MIT-Festival" => Ok(Self::MitFestival),
            "MIT-Modern-Variant" => Ok(Self::MitModernVariant),
            "MIT-open-group" => Ok(Self::MitOpenGroup),
            "MIT-Wu" => Ok(Self::MitWu),
            "MITNFA" => Ok(Self::Mitnfa),
            "Motosoto" => Ok(Self::Motosoto),
            "mpi-permissive" => Ok(Self::MpiPermissive),
            "mpich2" => Ok(Self::Mpich2),
            "MPL-1.0" => Ok(Self::Mpl10),
            "MPL-1.1" => Ok(Self::Mpl11),
            "MPL-2.0" => Ok(Self::Mpl20),
            "MPL-2.0-no-copyleft-exception" => Ok(Self::Mpl20NoCopyleftException),
            "mplus" => Ok(Self::Mplus),
            "MS-LPL" => Ok(Self::MsLpl),
            "MS-PL" => Ok(Self::MsPl),
            "MS-RL" => Ok(Self::MsRl),
            "MTLL" => Ok(Self::Mtll),
            "MulanPSL-1.0" => Ok(Self::MulanPsl10),
            "MulanPSL-2.0" => Ok(Self::MulanPsl20),
            "Multics" => Ok(Self::Multics),
            "Mup" => Ok(Self::Mup),
            "NAIST-2003" => Ok(Self::Naist2003),
            "NASA-1.3" => Ok(Self::Nasa13),
            "Naumen" => Ok(Self::Naumen),
            "NBPL-1.0" => Ok(Self::Nbpl10),
            "NCGL-UK-2.0" => Ok(Self::NcglUk20),
            "NCSA" => Ok(Self::Ncsa),
            "Net-SNMP" => Ok(Self::NetSnmp),
            "NetCDF" => Ok(Self::NetCdf),
            "Newsletr" => Ok(Self::Newsletr),
            "NGPL" => Ok(Self::Ngpl),
            "NICTA-1.0" => Ok(Self::Nicta10),
            "NIST-PD" => Ok(Self::NistPd),
            "NIST-PD-fallback" => Ok(Self::NistPdFallback),
            "NIST-Software" => Ok(Self::NistSoftware),
            "NLOD-1.0" => Ok(Self::Nlod10),
            "NLOD-2.0" => Ok(Self::Nlod20),
            "NLPL" => Ok(Self::Nlpl),
            "Nokia" => Ok(Self::Nokia),
            "NOSL" => Ok(Self::Nosl),
            "Noweb" => Ok(Self::Noweb),
            "NPL-1.0" => Ok(Self::Npl10),
            "NPL-1.1" => Ok(Self::Npl11),
            "NPOSL-3.0" => Ok(Self::Nposl30),
            "NRL" => Ok(Self::Nrl),
            "NTP" => Ok(Self::Ntp),
            "NTP-0" => Ok(Self::Ntp0),
            "Nunit" => Ok(Self::Nunit),
            "O-UDA-1.0" => Ok(Self::OUda10),
            "OCCT-PL" => Ok(Self::OcctPl),
            "OCLC-2.0" => Ok(Self::Oclc20),
            "ODbL-1.0" => Ok(Self::ODbL10),
            "ODC-By-1.0" => Ok(Self::OdcBy10),
            "OFFIS" => Ok(Self::Offis),
            "OFL-1.0" => Ok(Self::Ofl10),
            "OFL-1.0-no-RFN" => Ok(Self::Ofl10NoRfn),
            "OFL-1.0-RFN" => Ok(Self::Ofl10Rfn),
            "OFL-1.1" => Ok(Self::Ofl11),
            "OFL-1.1-no-RFN" => Ok(Self::Ofl11NoRfn),
            "OFL-1.1-RFN" => Ok(Self::Ofl11Rfn),
            "OGC-1.0" => Ok(Self::Ogc10),
            "OGDL-Taiwan-1.0" => Ok(Self::OgdlTaiwan10),
            "OGL-Canada-2.0" => Ok(Self::OglCanada20),
            "OGL-UK-1.0" => Ok(Self::OglUk10),
            "OGL-UK-2.0" => Ok(Self::OglUk20),
            "OGL-UK-3.0" => Ok(Self::OglUk30),
            "OGTSL" => Ok(Self::Ogtsl),
            "OLDAP-1.1" => Ok(Self::Oldap11),
            "OLDAP-1.2" => Ok(Self::Oldap12),
            "OLDAP-1.3" => Ok(Self::Oldap13),
            "OLDAP-1.4" => Ok(Self::Oldap14),
            "OLDAP-2.0" => Ok(Self::Oldap20),
            "OLDAP-2.0.1" => Ok(Self::Oldap201),
            "OLDAP-2.1" => Ok(Self::Oldap21),
            "OLDAP-2.2" => Ok(Self::Oldap22),
            "OLDAP-2.2.1" => Ok(Self::Oldap221),
            "OLDAP-2.2.2" => Ok(Self::Oldap222),
            "OLDAP-2.3" => Ok(Self::Oldap23),
            "OLDAP-2.4" => Ok(Self::Oldap24),
            "OLDAP-2.5" => Ok(Self::Oldap25),
            "OLDAP-2.6" => Ok(Self::Oldap26),
            "OLDAP-2.7" => Ok(Self::Oldap27),
            "OLDAP-2.8" => Ok(Self::Oldap28),
            "OLFL-1.3" => Ok(Self::Olfl13),
            "OML" => Ok(Self::Oml),
            "OpenPBS-2.3" => Ok(Self::OpenPbs23),
            "OpenSSL" => Ok(Self::OpenSsl),
            "OPL-1.0" => Ok(Self::Opl10),
            "OPL-UK-3.0" => Ok(Self::OplUk30),
            "OPUBL-1.0" => Ok(Self::Opubl10),
            "OSET-PL-2.1" => Ok(Self::OsetPl21),
            "OSL-1.0" => Ok(Self::Osl10),
            "OSL-1.1" => Ok(Self::Osl11),
            "OSL-2.0" => Ok(Self::Osl20),
            "OSL-2.1" => Ok(Self::Osl21),
            "OSL-3.0" => Ok(Self::Osl30),
            "Parity-6.0.0" => Ok(Self::Parity600),
            "Parity-7.0.0" => Ok(Self::Parity700),
            "PDDL-1.0" => Ok(Self::Pddl10),
            "PHP-3.0" => Ok(Self::Php30),
            "PHP-3.01" => Ok(Self::Php301),
            "Plexus" => Ok(Self::Plexus),
            "PolyForm-Noncommercial-1.0.0" => Ok(Self::PolyFormNoncommercial100),
            "PolyForm-Small-Business-1.0.0" => Ok(Self::PolyFormSmallBusiness100),
            "PostgreSQL" => Ok(Self::PostgreSql),
            "PSF-2.0" => Ok(Self::Psf20),
            "psfrag" => Ok(Self::Psfrag),
            "psutils" => Ok(Self::Psutils),
            "Python-2.0" => Ok(Self::Python20),
            "Python-2.0.1" => Ok(Self::Python201),
            "Qhull" => Ok(Self::Qhull),
            "QPL-1.0" => Ok(Self::Qpl10),
            "QPL-1.0-INRIA-2004" => Ok(Self::Qpl10Inria2004),
            "Rdisc" => Ok(Self::Rdisc),
            "RHeCos-1.1" => Ok(Self::RHeCos11),
            "RPL-1.1" => Ok(Self::Rpl11),
            "RPL-1.5" => Ok(Self::Rpl15),
            "RPSL-1.0" => Ok(Self::Rpsl10),
            "RSA-MD" => Ok(Self::RsaMd),
            "RSCPL" => Ok(Self::Rscpl),
            "Ruby" => Ok(Self::Ruby),
            "SAX-PD" => Ok(Self::SaxPd),
            "Saxpath" => Ok(Self::Saxpath),
            "SCEA" => Ok(Self::Scea),
            "SchemeReport" => Ok(Self::SchemeReport),
            "Sendmail" => Ok(Self::Sendmail),
            "Sendmail-8.23" => Ok(Self::Sendmail823),
            "SGI-B-1.0" => Ok(Self::SgiB10),
            "SGI-B-1.1" => Ok(Self::SgiB11),
            "SGI-B-2.0" => Ok(Self::SgiB20),
            "SGP4" => Ok(Self::Sgp4),
            "SHL-0.5" => Ok(Self::Shl05),
            "SHL-0.51" => Ok(Self::Shl051),
            "SimPL-2.0" => Ok(Self::SimPl20),
            "SISSL" => Ok(Self::Sissl),
            "SISSL-1.2" => Ok(Self::Sissl12),
            "Sleepycat" => Ok(Self::Sleepycat),
            "SMLNJ" => Ok(Self::Smlnj),
            "SMPPL" => Ok(Self::Smppl),
            "SNIA" => Ok(Self::Snia),
            "snprintf" => Ok(Self::Snprintf),
            "Spencer-86" => Ok(Self::Spencer86),
            "Spencer-94" => Ok(Self::Spencer94),
            "Spencer-99" => Ok(Self::Spencer99),
            "SPL-1.0" => Ok(Self::Spl10),
            "SSH-OpenSSH" => Ok(Self::SshOpenSsh),
            "SSH-short" => Ok(Self::SshShort),
            "SSPL-1.0" => Ok(Self::Sspl10),
            "StandardML-NJ" => Ok(Self::StandardMlNj),
            "SugarCRM-1.1.3" => Ok(Self::SugarCrm113),
            "SunPro" => Ok(Self::SunPro),
            "SWL" => Ok(Self::Swl),
            "Symlinks" => Ok(Self::Symlinks),
            "TAPR-OHL-1.0" => Ok(Self::TaprOhl10),
            "TCL" => Ok(Self::Tcl),
            "TCP-wrappers" => Ok(Self::TcpWrappers),
            "TermReadKey" => Ok(Self::TermReadKey),
            "TMate" => Ok(Self::TMate),
            "TORQUE-1.1" => Ok(Self::Torque11),
            "TOSL" => Ok(Self::Tosl),
            "TPDL" => Ok(Self::Tpdl),
            "TPL-1.0" => Ok(Self::Tpl10),
            "TTWL" => Ok(Self::Ttwl),
            "TU-Berlin-1.0" => Ok(Self::TuBerlin10),
            "TU-Berlin-2.0" => Ok(Self::TuBerlin20),
            "UCAR" => Ok(Self::Ucar),
            "UCL-1.0" => Ok(Self::Ucl10),
            "Unicode-DFS-2015" => Ok(Self::UnicodeDfs2015),
            "Unicode-DFS-2016" => Ok(Self::UnicodeDfs2016),
            "Unicode-TOU" => Ok(Self::UnicodeTou),
            "UnixCrypt" => Ok(Self::UnixCrypt),
            "Unlicense" => Ok(Self::Unlicense),
            "UPL-1.0" => Ok(Self::Upl10),
            "Vim" => Ok(Self::Vim),
            "VOSTROM" => Ok(Self::Vostrom),
            "VSL-1.0" => Ok(Self::Vsl10),
            "W3C" => Ok(Self::W3c),
            "W3C-19980720" => Ok(Self::W3c19980720),
            "W3C-20150513" => Ok(Self::W3c20150513),
            "w3m" => Ok(Self::W3m),
            "Watcom-1.0" => Ok(Self::Watcom10),
            "Widget-Workshop" => Ok(Self::WidgetWorkshop),
            "Wsuipa" => Ok(Self::Wsuipa),
            "WTFPL" => Ok(Self::Wtfpl),
            "wxWindows" => Ok(Self::WxWindows),
            "X11" => Ok(Self::X11),
            "X11-distribute-modifications-variant" => Ok(Self::X11DistributeModificationsVariant),
            "Xdebug-1.03" => Ok(Self::Xdebug103),
            "Xerox" => Ok(Self::Xerox),
            "Xfig" => Ok(Self::Xfig),
            "XFree86-1.1" => Ok(Self::XFree8611),
            "xinetd" => Ok(Self::Xinetd),
            "xlock" => Ok(Self::Xlock),
            "Xnet" => Ok(Self::Xnet),
            "xpp" => Ok(Self::Xpp),
            "XSkat" => Ok(Self::XSkat),
            "YPL-1.0" => Ok(Self::Ypl10),
            "YPL-1.1" => Ok(Self::Ypl11),
            "Zed" => Ok(Self::Zed),
            "Zend-2.0" => Ok(Self::Zend20),
            "Zimbra-1.3" => Ok(Self::Zimbra13),
            "Zimbra-1.4" => Ok(Self::Zimbra14),
            "Zlib" => Ok(Self::Zlib),
            "zlib-acknowledgement" => Ok(Self::ZlibAcknowledgement),
            "ZPL-1.1" => Ok(Self::Zpl11),
            "ZPL-2.0" => Ok(Self::Zpl20),
            "ZPL-2.1" => Ok(Self::Zpl21),
            _ => Err("invalid value".into()),
        }
    }
}
impl std::convert::TryFrom<&str> for SpdxLicenseIds {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<&String> for SpdxLicenseIds {
    type Error = self::error::ConversionError;
    fn try_from(value: &String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl std::convert::TryFrom<String> for SpdxLicenseIds {
    type Error = self::error::ConversionError;
    fn try_from(value: String) -> Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = r" Types for composing complex structures."]
pub mod builder {
    #[derive(Clone, Debug)]
    pub struct Authorization {
        info_url: Result<Option<String>, String>,
        param_name: Result<Option<String>, String>,
        type_: Result<super::AuthorizationType, String>,
    }
    impl Default for Authorization {
        fn default() -> Self {
            Self {
                info_url: Ok(Default::default()),
                param_name: Ok(Default::default()),
                type_: Err("no value supplied for type_".to_string()),
            }
        }
    }
    impl Authorization {
        pub fn info_url<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.info_url = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for info_url: {}", e));
            self
        }
        pub fn param_name<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.param_name = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for param_name: {}", e));
            self
        }
        pub fn type_<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<super::AuthorizationType>,
            T::Error: std::fmt::Display,
        {
            self.type_ = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for type_: {}", e));
            self
        }
    }
    impl std::convert::TryFrom<Authorization> for super::Authorization {
        type Error = super::error::ConversionError;
        fn try_from(value: Authorization) -> Result<Self, super::error::ConversionError> {
            Ok(Self {
                info_url: value.info_url?,
                param_name: value.param_name?,
                type_: value.type_?,
            })
        }
    }
    impl From<super::Authorization> for Authorization {
        fn from(value: super::Authorization) -> Self {
            Self {
                info_url: Ok(value.info_url),
                param_name: Ok(value.param_name),
                type_: Ok(value.type_),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct DistributedMobilityFeedRegistry {
        feeds: Result<Vec<super::Feed>, String>,
        license_spdx_identifier: Result<Option<super::SpdxLicenseIds>, String>,
        operators: Result<Vec<super::Operator>, String>,
    }
    impl Default for DistributedMobilityFeedRegistry {
        fn default() -> Self {
            Self {
                feeds: Ok(Default::default()),
                license_spdx_identifier: Ok(Default::default()),
                operators: Ok(Default::default()),
            }
        }
    }
    impl DistributedMobilityFeedRegistry {
        pub fn feeds<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::Feed>>,
            T::Error: std::fmt::Display,
        {
            self.feeds = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for feeds: {}", e));
            self
        }
        pub fn license_spdx_identifier<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::SpdxLicenseIds>>,
            T::Error: std::fmt::Display,
        {
            self.license_spdx_identifier = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for license_spdx_identifier: {}",
                    e
                )
            });
            self
        }
        pub fn operators<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::Operator>>,
            T::Error: std::fmt::Display,
        {
            self.operators = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for operators: {}", e));
            self
        }
    }
    impl std::convert::TryFrom<DistributedMobilityFeedRegistry>
        for super::DistributedMobilityFeedRegistry
    {
        type Error = super::error::ConversionError;
        fn try_from(
            value: DistributedMobilityFeedRegistry,
        ) -> Result<Self, super::error::ConversionError> {
            Ok(Self {
                feeds: value.feeds?,
                license_spdx_identifier: value.license_spdx_identifier?,
                operators: value.operators?,
            })
        }
    }
    impl From<super::DistributedMobilityFeedRegistry> for DistributedMobilityFeedRegistry {
        fn from(value: super::DistributedMobilityFeedRegistry) -> Self {
            Self {
                feeds: Ok(value.feeds),
                license_spdx_identifier: Ok(value.license_spdx_identifier),
                operators: Ok(value.operators),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct Feed {
        authorization: Result<Option<super::Authorization>, String>,
        description: Result<Option<String>, String>,
        id: Result<String, String>,
        languages: Result<Vec<super::Language>, String>,
        license: Result<Option<super::LicenseDescription>, String>,
        name: Result<Option<String>, String>,
        operators: Result<Vec<super::Operator>, String>,
        spec: Result<super::FeedSpec, String>,
        supersedes_ids: Result<Vec<String>, String>,
        tags: Result<serde_json::Map<String, serde_json::Value>, String>,
        urls: Result<super::FeedUrls, String>,
    }
    impl Default for Feed {
        fn default() -> Self {
            Self {
                authorization: Ok(Default::default()),
                description: Ok(Default::default()),
                id: Err("no value supplied for id".to_string()),
                languages: Ok(Default::default()),
                license: Ok(Default::default()),
                name: Ok(Default::default()),
                operators: Ok(Default::default()),
                spec: Err("no value supplied for spec".to_string()),
                supersedes_ids: Ok(Default::default()),
                tags: Ok(Default::default()),
                urls: Err("no value supplied for urls".to_string()),
            }
        }
    }
    impl Feed {
        pub fn authorization<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::Authorization>>,
            T::Error: std::fmt::Display,
        {
            self.authorization = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for authorization: {}", e));
            self
        }
        pub fn description<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.description = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for description: {}", e));
            self
        }
        pub fn id<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<String>,
            T::Error: std::fmt::Display,
        {
            self.id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for id: {}", e));
            self
        }
        pub fn languages<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::Language>>,
            T::Error: std::fmt::Display,
        {
            self.languages = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for languages: {}", e));
            self
        }
        pub fn license<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::LicenseDescription>>,
            T::Error: std::fmt::Display,
        {
            self.license = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for license: {}", e));
            self
        }
        pub fn name<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.name = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for name: {}", e));
            self
        }
        pub fn operators<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::Operator>>,
            T::Error: std::fmt::Display,
        {
            self.operators = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for operators: {}", e));
            self
        }
        pub fn spec<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<super::FeedSpec>,
            T::Error: std::fmt::Display,
        {
            self.spec = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for spec: {}", e));
            self
        }
        pub fn supersedes_ids<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<String>>,
            T::Error: std::fmt::Display,
        {
            self.supersedes_ids = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for supersedes_ids: {}", e));
            self
        }
        pub fn tags<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<serde_json::Map<String, serde_json::Value>>,
            T::Error: std::fmt::Display,
        {
            self.tags = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for tags: {}", e));
            self
        }
        pub fn urls<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<super::FeedUrls>,
            T::Error: std::fmt::Display,
        {
            self.urls = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for urls: {}", e));
            self
        }
    }
    impl std::convert::TryFrom<Feed> for super::Feed {
        type Error = super::error::ConversionError;
        fn try_from(value: Feed) -> Result<Self, super::error::ConversionError> {
            Ok(Self {
                authorization: value.authorization?,
                description: value.description?,
                id: value.id?,
                languages: value.languages?,
                license: value.license?,
                name: value.name?,
                operators: value.operators?,
                spec: value.spec?,
                supersedes_ids: value.supersedes_ids?,
                tags: value.tags?,
                urls: value.urls?,
            })
        }
    }
    impl From<super::Feed> for Feed {
        fn from(value: super::Feed) -> Self {
            Self {
                authorization: Ok(value.authorization),
                description: Ok(value.description),
                id: Ok(value.id),
                languages: Ok(value.languages),
                license: Ok(value.license),
                name: Ok(value.name),
                operators: Ok(value.operators),
                spec: Ok(value.spec),
                supersedes_ids: Ok(value.supersedes_ids),
                tags: Ok(value.tags),
                urls: Ok(value.urls),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct FeedUrls {
        gbfs_auto_discovery: Result<Option<super::FeedUrlsGbfsAutoDiscovery>, String>,
        mds_provider: Result<Option<super::FeedUrlsMdsProvider>, String>,
        realtime_alerts: Result<Option<super::FeedUrlsRealtimeAlerts>, String>,
        realtime_trip_updates: Result<Option<super::FeedUrlsRealtimeTripUpdates>, String>,
        realtime_vehicle_positions: Result<Option<super::FeedUrlsRealtimeVehiclePositions>, String>,
        static_current: Result<Option<super::FeedUrlsStaticCurrent>, String>,
        static_historic: Result<Vec<super::FeedUrlsStaticHistoricItem>, String>,
        static_hypothetical: Result<Vec<super::FeedUrlsStaticHypotheticalItem>, String>,
        static_planned: Result<Vec<super::FeedUrlsStaticPlannedItem>, String>,
    }
    impl Default for FeedUrls {
        fn default() -> Self {
            Self {
                gbfs_auto_discovery: Ok(Default::default()),
                mds_provider: Ok(Default::default()),
                realtime_alerts: Ok(Default::default()),
                realtime_trip_updates: Ok(Default::default()),
                realtime_vehicle_positions: Ok(Default::default()),
                static_current: Ok(Default::default()),
                static_historic: Ok(Default::default()),
                static_hypothetical: Ok(Default::default()),
                static_planned: Ok(Default::default()),
            }
        }
    }
    impl FeedUrls {
        pub fn gbfs_auto_discovery<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::FeedUrlsGbfsAutoDiscovery>>,
            T::Error: std::fmt::Display,
        {
            self.gbfs_auto_discovery = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for gbfs_auto_discovery: {}",
                    e
                )
            });
            self
        }
        pub fn mds_provider<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::FeedUrlsMdsProvider>>,
            T::Error: std::fmt::Display,
        {
            self.mds_provider = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for mds_provider: {}", e));
            self
        }
        pub fn realtime_alerts<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::FeedUrlsRealtimeAlerts>>,
            T::Error: std::fmt::Display,
        {
            self.realtime_alerts = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for realtime_alerts: {}", e));
            self
        }
        pub fn realtime_trip_updates<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::FeedUrlsRealtimeTripUpdates>>,
            T::Error: std::fmt::Display,
        {
            self.realtime_trip_updates = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for realtime_trip_updates: {}",
                    e
                )
            });
            self
        }
        pub fn realtime_vehicle_positions<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::FeedUrlsRealtimeVehiclePositions>>,
            T::Error: std::fmt::Display,
        {
            self.realtime_vehicle_positions = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for realtime_vehicle_positions: {}",
                    e
                )
            });
            self
        }
        pub fn static_current<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::FeedUrlsStaticCurrent>>,
            T::Error: std::fmt::Display,
        {
            self.static_current = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for static_current: {}", e));
            self
        }
        pub fn static_historic<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::FeedUrlsStaticHistoricItem>>,
            T::Error: std::fmt::Display,
        {
            self.static_historic = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for static_historic: {}", e));
            self
        }
        pub fn static_hypothetical<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::FeedUrlsStaticHypotheticalItem>>,
            T::Error: std::fmt::Display,
        {
            self.static_hypothetical = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for static_hypothetical: {}",
                    e
                )
            });
            self
        }
        pub fn static_planned<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::FeedUrlsStaticPlannedItem>>,
            T::Error: std::fmt::Display,
        {
            self.static_planned = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for static_planned: {}", e));
            self
        }
    }
    impl std::convert::TryFrom<FeedUrls> for super::FeedUrls {
        type Error = super::error::ConversionError;
        fn try_from(value: FeedUrls) -> Result<Self, super::error::ConversionError> {
            Ok(Self {
                gbfs_auto_discovery: value.gbfs_auto_discovery?,
                mds_provider: value.mds_provider?,
                realtime_alerts: value.realtime_alerts?,
                realtime_trip_updates: value.realtime_trip_updates?,
                realtime_vehicle_positions: value.realtime_vehicle_positions?,
                static_current: value.static_current?,
                static_historic: value.static_historic?,
                static_hypothetical: value.static_hypothetical?,
                static_planned: value.static_planned?,
            })
        }
    }
    impl From<super::FeedUrls> for FeedUrls {
        fn from(value: super::FeedUrls) -> Self {
            Self {
                gbfs_auto_discovery: Ok(value.gbfs_auto_discovery),
                mds_provider: Ok(value.mds_provider),
                realtime_alerts: Ok(value.realtime_alerts),
                realtime_trip_updates: Ok(value.realtime_trip_updates),
                realtime_vehicle_positions: Ok(value.realtime_vehicle_positions),
                static_current: Ok(value.static_current),
                static_historic: Ok(value.static_historic),
                static_hypothetical: Ok(value.static_hypothetical),
                static_planned: Ok(value.static_planned),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct LicenseDescription {
        attribution_instructions: Result<Option<String>, String>,
        attribution_text: Result<Option<String>, String>,
        commercial_use_allowed:
            Result<Option<super::LicenseDescriptionCommercialUseAllowed>, String>,
        create_derived_product:
            Result<Option<super::LicenseDescriptionCreateDerivedProduct>, String>,
        redistribution_allowed:
            Result<Option<super::LicenseDescriptionRedistributionAllowed>, String>,
        share_alike_optional: Result<Option<super::LicenseDescriptionShareAlikeOptional>, String>,
        spdx_identifier: Result<Option<super::SpdxLicenseIds>, String>,
        url: Result<Option<String>, String>,
        use_without_attribution:
            Result<Option<super::LicenseDescriptionUseWithoutAttribution>, String>,
    }
    impl Default for LicenseDescription {
        fn default() -> Self {
            Self {
                attribution_instructions: Ok(Default::default()),
                attribution_text: Ok(Default::default()),
                commercial_use_allowed: Ok(Default::default()),
                create_derived_product: Ok(Default::default()),
                redistribution_allowed: Ok(Default::default()),
                share_alike_optional: Ok(Default::default()),
                spdx_identifier: Ok(Default::default()),
                url: Ok(Default::default()),
                use_without_attribution: Ok(Default::default()),
            }
        }
    }
    impl LicenseDescription {
        pub fn attribution_instructions<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.attribution_instructions = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for attribution_instructions: {}",
                    e
                )
            });
            self
        }
        pub fn attribution_text<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.attribution_text = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for attribution_text: {}",
                    e
                )
            });
            self
        }
        pub fn commercial_use_allowed<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::LicenseDescriptionCommercialUseAllowed>>,
            T::Error: std::fmt::Display,
        {
            self.commercial_use_allowed = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for commercial_use_allowed: {}",
                    e
                )
            });
            self
        }
        pub fn create_derived_product<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::LicenseDescriptionCreateDerivedProduct>>,
            T::Error: std::fmt::Display,
        {
            self.create_derived_product = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for create_derived_product: {}",
                    e
                )
            });
            self
        }
        pub fn redistribution_allowed<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::LicenseDescriptionRedistributionAllowed>>,
            T::Error: std::fmt::Display,
        {
            self.redistribution_allowed = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for redistribution_allowed: {}",
                    e
                )
            });
            self
        }
        pub fn share_alike_optional<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::LicenseDescriptionShareAlikeOptional>>,
            T::Error: std::fmt::Display,
        {
            self.share_alike_optional = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for share_alike_optional: {}",
                    e
                )
            });
            self
        }
        pub fn spdx_identifier<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::SpdxLicenseIds>>,
            T::Error: std::fmt::Display,
        {
            self.spdx_identifier = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for spdx_identifier: {}", e));
            self
        }
        pub fn url<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.url = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for url: {}", e));
            self
        }
        pub fn use_without_attribution<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::LicenseDescriptionUseWithoutAttribution>>,
            T::Error: std::fmt::Display,
        {
            self.use_without_attribution = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for use_without_attribution: {}",
                    e
                )
            });
            self
        }
    }
    impl std::convert::TryFrom<LicenseDescription> for super::LicenseDescription {
        type Error = super::error::ConversionError;
        fn try_from(value: LicenseDescription) -> Result<Self, super::error::ConversionError> {
            Ok(Self {
                attribution_instructions: value.attribution_instructions?,
                attribution_text: value.attribution_text?,
                commercial_use_allowed: value.commercial_use_allowed?,
                create_derived_product: value.create_derived_product?,
                redistribution_allowed: value.redistribution_allowed?,
                share_alike_optional: value.share_alike_optional?,
                spdx_identifier: value.spdx_identifier?,
                url: value.url?,
                use_without_attribution: value.use_without_attribution?,
            })
        }
    }
    impl From<super::LicenseDescription> for LicenseDescription {
        fn from(value: super::LicenseDescription) -> Self {
            Self {
                attribution_instructions: Ok(value.attribution_instructions),
                attribution_text: Ok(value.attribution_text),
                commercial_use_allowed: Ok(value.commercial_use_allowed),
                create_derived_product: Ok(value.create_derived_product),
                redistribution_allowed: Ok(value.redistribution_allowed),
                share_alike_optional: Ok(value.share_alike_optional),
                spdx_identifier: Ok(value.spdx_identifier),
                url: Ok(value.url),
                use_without_attribution: Ok(value.use_without_attribution),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct Operator {
        associated_feeds: Result<Vec<super::OperatorAssociatedFeedsItem>, String>,
        name: Result<String, String>,
        onestop_id: Result<String, String>,
        short_name: Result<Option<String>, String>,
        supersedes_ids: Result<Vec<String>, String>,
        tags: Result<serde_json::Map<String, serde_json::Value>, String>,
        website: Result<Option<super::OperatorWebsite>, String>,
    }
    impl Default for Operator {
        fn default() -> Self {
            Self {
                associated_feeds: Ok(Default::default()),
                name: Err("no value supplied for name".to_string()),
                onestop_id: Err("no value supplied for onestop_id".to_string()),
                short_name: Ok(Default::default()),
                supersedes_ids: Ok(Default::default()),
                tags: Ok(Default::default()),
                website: Ok(Default::default()),
            }
        }
    }
    impl Operator {
        pub fn associated_feeds<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<super::OperatorAssociatedFeedsItem>>,
            T::Error: std::fmt::Display,
        {
            self.associated_feeds = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for associated_feeds: {}",
                    e
                )
            });
            self
        }
        pub fn name<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<String>,
            T::Error: std::fmt::Display,
        {
            self.name = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for name: {}", e));
            self
        }
        pub fn onestop_id<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<String>,
            T::Error: std::fmt::Display,
        {
            self.onestop_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for onestop_id: {}", e));
            self
        }
        pub fn short_name<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.short_name = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for short_name: {}", e));
            self
        }
        pub fn supersedes_ids<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Vec<String>>,
            T::Error: std::fmt::Display,
        {
            self.supersedes_ids = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for supersedes_ids: {}", e));
            self
        }
        pub fn tags<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<serde_json::Map<String, serde_json::Value>>,
            T::Error: std::fmt::Display,
        {
            self.tags = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for tags: {}", e));
            self
        }
        pub fn website<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<super::OperatorWebsite>>,
            T::Error: std::fmt::Display,
        {
            self.website = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for website: {}", e));
            self
        }
    }
    impl std::convert::TryFrom<Operator> for super::Operator {
        type Error = super::error::ConversionError;
        fn try_from(value: Operator) -> Result<Self, super::error::ConversionError> {
            Ok(Self {
                associated_feeds: value.associated_feeds?,
                name: value.name?,
                onestop_id: value.onestop_id?,
                short_name: value.short_name?,
                supersedes_ids: value.supersedes_ids?,
                tags: value.tags?,
                website: value.website?,
            })
        }
    }
    impl From<super::Operator> for Operator {
        fn from(value: super::Operator) -> Self {
            Self {
                associated_feeds: Ok(value.associated_feeds),
                name: Ok(value.name),
                onestop_id: Ok(value.onestop_id),
                short_name: Ok(value.short_name),
                supersedes_ids: Ok(value.supersedes_ids),
                tags: Ok(value.tags),
                website: Ok(value.website),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct OperatorAssociatedFeedsItem {
        feed_onestop_id: Result<Option<serde_json::Value>, String>,
        gtfs_agency_id: Result<Option<String>, String>,
    }
    impl Default for OperatorAssociatedFeedsItem {
        fn default() -> Self {
            Self {
                feed_onestop_id: Ok(Default::default()),
                gtfs_agency_id: Ok(Default::default()),
            }
        }
    }
    impl OperatorAssociatedFeedsItem {
        pub fn feed_onestop_id<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<serde_json::Value>>,
            T::Error: std::fmt::Display,
        {
            self.feed_onestop_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for feed_onestop_id: {}", e));
            self
        }
        pub fn gtfs_agency_id<T>(mut self, value: T) -> Self
        where
            T: std::convert::TryInto<Option<String>>,
            T::Error: std::fmt::Display,
        {
            self.gtfs_agency_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for gtfs_agency_id: {}", e));
            self
        }
    }
    impl std::convert::TryFrom<OperatorAssociatedFeedsItem> for super::OperatorAssociatedFeedsItem {
        type Error = super::error::ConversionError;
        fn try_from(
            value: OperatorAssociatedFeedsItem,
        ) -> Result<Self, super::error::ConversionError> {
            Ok(Self {
                feed_onestop_id: value.feed_onestop_id?,
                gtfs_agency_id: value.gtfs_agency_id?,
            })
        }
    }
    impl From<super::OperatorAssociatedFeedsItem> for OperatorAssociatedFeedsItem {
        fn from(value: super::OperatorAssociatedFeedsItem) -> Self {
            Self {
                feed_onestop_id: Ok(value.feed_onestop_id),
                gtfs_agency_id: Ok(value.gtfs_agency_id),
            }
        }
    }
}
