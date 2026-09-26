# ClinicalFlow — Setup & Implementation Q&A

Questions encountered during setup and implementation. Updated as new questions come up.

---

## Azure Setup

### Q1: Why Hybrid workspace type instead of Serverless?

**Serverless workspace**
- Storage is Databricks-managed (you have no control over it)
- Compute is serverless only — no custom cluster configurations
- No support for custom cluster policies
- Limited Unity Catalog external location support
- Less visibility into cost per job

**Hybrid workspace**
- Storage is your own ADLS Gen2 (`stclinicalflow`) — full control
- Supports custom job clusters with auto-terminate and cluster policies
- Full Unity Catalog external location + storage credential support
- Cluster policies let you cap DBU spend per job
- Better for a production-grade project where you control infra

**Decision:** ClinicalFlow uses ADLS Gen2 as external storage and enforces cost control via cluster policies — Hybrid is the only option that supports both.

---

*More questions will be added here as they come up.*
