# Title

Streaming Lakehouse for UK Carbon Intensity Analytics

# Objective

The goals of this project is to build a data pipeline that the polls the UK Carbon Intensity API continuously and publishes each half-hour record as an event into Kafka.  A spark structured streaming job consumes kafka and writes Bronze (append-only) to Delta.  A second spark job continuously builds Silver (deduped, “latest state”) using merges.  A Streamlit dashboard reads Silver/Gold and updates live.  You’ll also have basic “production-ish” behaviors: idempotency (event_id), checkpointing, and small monitoring logs.   

# Prerequisites
Docker Desktop
Python 3.10+
Java 11+ 

