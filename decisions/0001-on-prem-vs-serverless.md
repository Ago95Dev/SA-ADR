---
# These are optional metadata elements. Feel free to remove any of them.
status: accepted
date: 2025-11-21
decision-makers: Agostino, Alessandro, Bryant, Graziano
consulted: Agostino, Alessandro, Bryant, Graziano
informed: DAgostino, Alessandro, Bryant, Graziano
---

# Deployment Architecture: On-Premises vs Serverless vs Hybrid

## Context and Problem Statement

The Urban Digital Twin for Emergency Decision-Making must process 100,000 events per minute from 5,000 data streams across a metropolitan district during emergency conditions, with simulation updates occurring within 5-10 seconds of real-world changes. The system must support up to 50 concurrent operators while minimizing infrastructure and operational costs.

Where should we deploy this real-time emergency system: on-premises data centers, cloud serverless infrastructure, or a hybrid approach combining edge and cloud resources?

## Decision Drivers

* **Performance**: Must maintain 5-10 second synchronization delay during 100k events/min peak loads
* **Cost Optimization**: Minimize infrastructure and operational costs; scale only when needed
* **Scalability**: Support district expansion and handle variable emergency workloads
* **Reliability**: Operate during network degradation and sensor outages
* **Operational Complexity**: Limited in-house infrastructure team availability
* **Geographic Distribution**: Edge gateways across district

## Considered Options

* **On-Premises Infrastructure** - Deploy all components in city-owned data centers
* **Cloud Serverless Architecture** - Fully managed cloud services with auto-scaling
* **Hybrid Edge-Cloud Architecture** - Edge gateways with local autonomy + cloud backend

## Decision Outcome

**Chosen option: *On‑Premises Infrastructure***

The system will be deployed entirely within city-operated data centers
using a high-availability, cloud‑like cluster architecture (e.g.,
Kubernetes, Kafka, Redis, InfluxDB, Ceph).\
This ensures continuity of service even during major WAN outages or
cloud provider incidents.

### Rationale

-   Cloud serverless architectures introduce a **single point of
    external dependency**.
-   Emergencies may correlate with WAN failure, datacenter isolation, or
    cloud outages.
-   The city retains full control over deployment, operational
    continuity, diagnostics, and failover.
-   On-prem clusters can provide cloud‑like scalability and elasticity
    without external reliance.

## Consequences

### Positive

-   ✔ **Fully independent** from cloud availability and WAN reliability\
-   ✔ **Resilient** during disasters affecting telecom infrastructure\
-   ✔ **Lower latency**, since all components run locally\
-   ✔ **Complete control** over data, security, and logs\
-   ✔ **Predictable cost structure** after initial investment\
-   ✔ **Can still integrate optional cloud services** for non-critical
    analytics

### Negative

-   ❌ Higher **up‑front capital expenditure** (hardware, racks,
    cooling, power)
-   ❌ Requires **local operations team** (although smaller than legacy
    data centers)
-   ❌ Must provision capacity for peak loads\
-   ❌ Redundancy across two city datacenters increases cost

## Pros and Cons of the Options

### On-Premises Infrastructure (Chosen)

**Pros** - High resilience to WAN outages\
- Full operational control\
- Data residency ensured\
- Local HA clustering possible\
- Low latency\
- No vendor lock-in

**Cons** - Requires hardware procurement and maintenance\
- Higher initial cost\
- Must manage redundancy across sites

------------------------------------------------------------------------

### Cloud Serverless Architecture

**Pros** - Auto-scaling\
- No hardware management\
- Fast to deploy

**Cons** - Depends on WAN and provider\
- Outages outside city control\
- Vendor lock-in\
- Costs unpredictable during peaks

------------------------------------------------------------------------

### Hybrid Edge--Cloud Architecture

**Pros** - Edge nodes provide partial autonomy\
- Lower cloud transfer costs\
- Suitable for distributed analytics

**Cons** - Still dependent on cloud for core operations\
- More complex architecture\
- Requires both edge and cloud expertise

## Decision Alignment

This decision supports: - **ADR-0002 (Microservices Architecture)** by
running services in local containers and service mesh.\
- **ADR-0003 (Managed Streaming Platform)** by hosting Kafka on-prem in
HA mode.\
- **C5 (Edge Gateway Redundancy)** since edge devices can operate
autonomously and forward data to the local cluster.

## Final Notes

The architecture will be implemented as a **high-availability on-prem
Kubernetes cluster** with replicated storage and redundant networking,
capable of supporting growth and future integration with optional cloud
services without compromising autonomy.
