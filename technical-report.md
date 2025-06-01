Based on the article you provided, DoorDash's in-house Search Engine architecture is composed of several distinct services working together. Here's a breakdown of those services and a high-level plan for implementing and integrating them.

**Distinct Services of the Search Engine Architecture:**

1.  **Indexer:**
    *   **Description:** This non-replicated service is responsible for handling all incoming indexing traffic. It processes data updates and builds or updates the Apache Lucene index segments. Once new segments are created, the Indexer uploads them to S3 for persistent storage and consumption by the Searcher service. It manages both high-priority updates (applied immediately) and bulk updates (applied during the next full index build cycle).
2.  **Searcher:**
    *   **Description:** This is a replicated service designed to serve search queries. It downloads the latest index segments from S3 (uploaded by the Indexer) and runs queries against them using Apache Lucene's search capabilities. The Searcher is designed to scale horizontally based solely on search traffic volume, as it does not handle indexing responsibilities.
3.  **Broker:**
    *   **Description:** The Broker acts as an aggregation layer. When a user submits a query, the Broker receives it, interacts with the Query Understanding and Planning Service to refine or rewrite the query, and then fans out the processed query to the relevant Searcher instances (which serve different index shards). Finally, it gathers the results from the various Searchers and merges them before returning the final result to the client.
4.  **Query Understanding and Planning Service:**
    *   **Description:** This component is crucial for processing raw client queries. It holds knowledge specific to a particular index and its business domain. The Broker uses this service to transform the user's query into a planned query that can be efficiently executed by the Searchers. Consolidating this logic here prevents individual clients from needing to replicate complex query-building logic.
5.  **Control Plane:**
    *   **Description:** This is an orchestration service responsible for managing the lifecycle and mutation of Search Stacks. It handles the deployment of new generations of the search stack (which include specific versions of the Indexer, Searcher, Broker, and Query Planning services) by gradually scaling up the new generation and scaling down the previous one. This ensures isolation between different generations and facilitates controlled updates to index schemas and stack configurations.

**Implementation and Integration Plan:**

Implementing this architecture would involve building each service and then establishing the necessary communication and data flow pipelines between them, orchestrated by the Control Plane.

1.  **Implement the Indexer Service:**
    *   Choose a programming language (likely one with good Lucene bindings or a strong ecosystem like Java/Kotlin/Scala or potentially Go).
    *   Integrate Apache Lucene for index creation and management.
    *   Set up mechanisms to receive indexing data (e.g., via a message queue or API).
    *   Implement logic for handling both real-time high-priority updates and scheduled bulk updates.
    *   Integrate with an S3 client library to upload index segments.
    *   Design the service to be non-replicated, potentially running multiple instances if sharding is implemented.

2.  **Implement the Searcher Service:**
    *   Use the same core language and Apache Lucene integration as the Indexer.
    *   Implement logic to periodically or on-demand download index segments from S3.
    *   Implement the query execution layer using Lucene's search APIs.
    *   Build an API endpoint for receiving queries from the Broker.
    *   Focus on making this service stateless and easily horizontally scalable.

3.  **Implement the Broker Service:**
    *   Implement the client-facing API endpoint for receiving raw user queries.
    *   Implement communication logic to send queries to the Query Understanding and Planning Service and receive the planned query.
    *   Implement logic to determine which Searcher instances (shards) are relevant for a given query.
    *   Implement fan-out logic to send the planned query to the selected Searchers in parallel.
    *   Implement result merging logic, potentially handling operations like joins and deduplication as mentioned in the article.

4.  **Implement the Query Understanding and Planning Service:**
    *   Design a data model for defining index schemas, computed fields, and query planning pipelines using declarative configurations.
    *   Implement logic to process raw queries based on these configurations and domain-specific rules.
    *   Provide an API for the Broker to interact with.

5.  **Implement the Control Plane Service:**
    *   Design an orchestration workflow for deploying new generations.
    *   Integrate with deployment tools (e.g., Kubernetes, internal deployment systems) to manage service instances.
    *   Implement logic to monitor the Indexer (e.g., wait for a full index build to complete).
    *   Implement a gradual rollout strategy for Searchers and Brokers of the new generation while scaling down the old one.
    *   Manage configuration versions for each generation.

6.  **Integration and Deployment:**
    *   Define communication protocols between services (e.g., gRPC, REST).
    *   Configure services to discover and communicate with the correct instances within their generation.
    *   Use S3 as the central point for index segment distribution between Indexer and Searchers.
    *   Deploy the Control Plane, and use it to manage the deployment and lifecycle of the other services in distinct "Search Stacks" for different tenants or indices.
    *   Implement monitoring and alerting for all services.

By building these components and orchestrating their deployment and interaction via the Control Plane, you can construct a scalable and flexible in-house search engine similar to the one described by DoorDash.

---

Source: https://careersatdoordash.com/blog/introducing-doordashs-in-house-search-engine/