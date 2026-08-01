# UrbanPulse Dead-Letter Queue (DLQ) Analysis Report

- **Collection Duration:** 20 seconds (0.33 minutes)
- **Total Validation Failures:** 18

## Error Distribution Table

| Error Category | Count | Percentage |
| :--- | :---: | :---: |
| AQI Value Null/Sensor Timeout | 7 | 38.89% |
| Negative Power Reading (Smart Meter) | 6 | 33.33% |
| Geospatial Boundary Violation (GPS) | 5 | 27.78% |

## Generated Artifacts
- Bar chart: [dlq_report_bar.png](dlq_report_bar.png)
- Pie chart: [dlq_report_pie.png](dlq_report_pie.png)
- CSV: [dlq_report.csv](dlq_report.csv)
