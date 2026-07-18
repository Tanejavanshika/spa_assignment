# UrbanPulse Dead-Letter Queue (DLQ) Analysis Report

- **Collection Duration:** 300 seconds (5.00 minutes)
- **Total Validation Failures:** 325

## Error Distribution Table

| Error Category | Count | Percentage |
| :--- | :---: | :---: |
| Geospatial Boundary Violation (GPS) | 161 | 49.54% |
| Negative Power Reading (Smart Meter) | 103 | 31.69% |
| AQI Value Null/Sensor Timeout | 61 | 18.77% |

## Sample Validation Errors
- **AQI Sensor Failures:** Missing/Null values from ambient monitors.
- **Bus GPS Boundary Errors:** Latitudes/Longitudes generated outside the MetroConnect bounding box.
- **Smart Meter Anomalies:** High voltage levels (>260V) or negative consumption metrics.
