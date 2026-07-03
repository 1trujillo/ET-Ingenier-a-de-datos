db.createCollection("hourly_metrics");
db.createCollection("incident_reports");

db.hourly_metrics.createIndex({ "timestamp": -1 });
db.hourly_metrics.createIndex({ "sensor_type": 1, "timestamp": -1 });

db.incident_reports.createIndex({ "timestamp": -1 });
db.incident_reports.createIndex({ "incident_type": 1, "timestamp": -1 });
db.incident_reports.createIndex({ "location": "2dsphere" });

print("MongoDB collections and indexes created successfully");
