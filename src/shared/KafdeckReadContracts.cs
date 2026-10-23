#if KAFDECK_CLI_READ_CONTRACTS
namespace Kafdeck.Cli.Contracts;
#else
namespace Kafdeck.Contracts;
#endif

/// <summary>Fixed versioned read contracts shared by API admission, CLI routing and documentation.</summary>
public sealed record NamedReadContract(string Name, string Path, string Action, string QueryKeys = "", string Method = "get",
    string? Body = null, string? Response = null, bool FilteredCollection = false, bool HumanOnly = false);

public static class KafdeckReadContracts
{
    public static readonly IReadOnlyList<NamedReadContract> All = Array.AsReadOnly<NamedReadContract>([
        new("v01-system-info", "/api/v1/system/info", "SystemRead"),
        new("v01-system-health", "/api/v1/system/health", "SystemRead"),
        new("v01-clusters-list", "/api/v1/clusters", "ClusterRead", FilteredCollection: true),
        new("v01-clusters-detail", "/api/v1/clusters/{clusterId}", "ClusterRead"),
        new("v01-clusters-health", "/api/v1/clusters/{clusterId}/health", "ClusterRead", Response: "ClusterHealthData"),
        new("v01-clusters-capabilities", "/api/v1/clusters/{clusterId}/capabilities", "ClusterRead"),
        new("v01-brokers-list", "/api/v1/clusters/{clusterId}/brokers", "BrokerRead"),
        new("v01-brokers-detail", "/api/v1/clusters/{clusterId}/brokers/{brokerId}", "BrokerRead"),
        new("v01-brokers-configuration", "/api/v1/clusters/{clusterId}/brokers/{brokerId}/configuration", "BrokerConfigRead", Response: "ConfigurationEntryData[]"),
        new("v01-topics-list", "/api/v1/clusters/{clusterId}/topics", "TopicList", "q,pageSize,cursor", FilteredCollection: true),
        new("v01-topics-detail", "/api/v1/clusters/{clusterId}/topics/{topicName}", "TopicRead"),
        new("v01-topics-partitions", "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions", "TopicRead"),
        new("v01-topics-configuration", "/api/v1/clusters/{clusterId}/topics/{topicName}/configuration", "TopicConfigRead", Response: "ConfigurationEntryData[]"),
        new("v04-consumer-groups-list", "/api/v1/clusters/{clusterId}/consumer-groups", "ConsumerRead", FilteredCollection: true),
        new("v04-consumer-groups-detail", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", "ConsumerRead"),
        new("v04-consumer-groups-lag", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/lag", "ConsumerRead"),
        new("v04-consumer-groups-diagnostics", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/diagnostics", "ConsumerRead"),
        new("v04-schema-subjects-list", "/api/v1/clusters/{clusterId}/schemas/subjects", "SchemaRead", FilteredCollection: true),
        new("v04-schema-versions-list", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions", "SchemaRead"),
        new("v04-schema-version-detail", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions/{version}", "SchemaRead"),
        new("v04-schema-compatibility", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/compatibility", "SchemaRead"),
        new("v04-schema-diff", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/diff", "SchemaRead", "leftVersion,rightVersion"),
        new("v07-connect-profiles-list", "/api/v1/clusters/{clusterId}/connect/profiles", "ConnectRead", Response: "ConnectProfileSummary[]"),
        new("v07-connect-profile-info", "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}", "ConnectRead", Response: "ConnectClusterInfo"),
        new("v07-connect-profile-plugins-list", "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/plugins", "ConnectRead", Response: "ConnectPluginSummary[]"),
        new("v07-connect-profile-connectors-list", "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors", "ConnectRead", Response: "ConnectConnectorSummary[]", FilteredCollection: true),
        new("v07-connect-profile-connectors-detail", "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}", "ConnectRead", Response: "ConnectConnectorDetail"),
        new("v04-connect-info", "/api/v1/clusters/{clusterId}/connect", "ConnectRead", Response: "ConnectClusterInfo"),
        new("v04-connect-connectors-list", "/api/v1/clusters/{clusterId}/connect/connectors", "ConnectRead", Response: "ConnectConnectorSummary[]", FilteredCollection: true),
        new("v04-connect-connectors-detail", "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}", "ConnectRead", Response: "ConnectConnectorDetail"),
        new("v07-connect-profile-plugin-validate", "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/plugins/{connectorClass}/validate", "ConnectRead", Method: "post", Body: "ConnectPluginValidationRequest", Response: "ConnectPluginValidationResult"),
        new("v04-ksql-info", "/api/v1/clusters/{clusterId}/ksql", "KsqlRead", Response: "KsqlServerInfo"),
        new("v04-ksql-metadata", "/api/v1/clusters/{clusterId}/ksql/metadata", "KsqlRead", Response: "KsqlMetadataItem[]"),
        new("v07-ksql-query", "/api/v1/clusters/{clusterId}/ksql/query", "KsqlRead", Method: "post", Body: "KsqlQueryRequestBody", Response: "KsqlQueryResult"),
        new("v04-topic-catalog-detail", "/api/v1/clusters/{clusterId}/catalog/topics/{topicName}", "CatalogRead", Response: "TopicCatalogEntry"),
        new("v07-streams-applications", "/api/v1/clusters/{clusterId}/streams/applications", "StreamsRead", Response: "StreamsApplicationSummary[]", FilteredCollection: true),
        new("v07-streams-topology", "/api/v1/clusters/{clusterId}/streams/applications/{applicationId}/topology", "StreamsRead", Response: "StreamsTopologyObservation"),
        new("v07-streams-state-stores", "/api/v1/clusters/{clusterId}/streams/applications/{applicationId}/state-stores", "StreamsRead", Response: "StreamsStateStoreObservation"),
        new("v07-lineage", "/api/v1/clusters/{clusterId}/lineage", "LineageRead", Response: "LineageGraph", FilteredCollection: true),
        new("v07-schema-reference-graph", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/references", "SchemaRead", Response: "SchemaReferenceGraph"),
        new("v07-schema-compatibility-explanation", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/compatibility/explanation", "SchemaRead", Response: "SchemaCompatibilityExplanation"),
        new("v07-schema-registry-capabilities", "/api/v1/clusters/{clusterId}/schemas/capabilities", "SchemaRead"),
        new("v07-schema-mock", "/api/v1/clusters/{clusterId}/schemas/mock", "SchemaRead", Method: "post", Body: "SchemaMockRequest", Response: "SchemaMockResult", FilteredCollection: true),
        new("v07-serde-capabilities", "/api/v1/tools/serde/capabilities", "SystemRead", Response: "ControlledSerdeCapabilitiesData"),
        new("v07-serde-decode", "/api/v1/tools/serde/decode", "SystemRead", Method: "post", Body: "ControlledSerdeDecodeApiRequest", Response: "ControlledSerdeDecodedValue"),
        new("v07-serde-encode", "/api/v1/tools/serde/encode", "SystemRead", Method: "post", Body: "ControlledSerdeEncodeApiRequest", Response: "ControlledSerdeEncodedApiData"),
        new("v03-records-browse", "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions/{partition}/records", "RecordRead", RecordQueries, Response: "RecordSafePage"),
        new("v03-records-tail", "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions/{partition}/records/tail", "RecordRead", RecordQueries, Response: "RecordSafePage"),
        new("v03-records-export", "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions/{partition}/records/export", "RecordExport", RecordQueries + ",format", Response: "RecordSafePage"),
        new("v08-consumer-operational-analytics-live", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/analytics/live", "ConsumerRead", Response: "OperationalAnalyticsResult"),
        new("v08-consumer-operational-analytics-trend", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/analytics/trend", "ConsumerRead", "from,to,maxPoints", Response: "OperationalTrendResult"),
        new("v08-consumer-operational-slo", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/analytics/slo", "ConsumerRead", "from,to,maxPoints,threshold,target", Response: "OperationalSloResult"),
        new("v08-data-quality-policy-list", "/api/v1/clusters/{clusterId}/data-quality/policies", "DataQualityRead", "state,maxResults,afterPolicyId", Response: "DataQualityPolicyPageData", FilteredCollection: true),
        new("v08-data-quality-policy-detail", "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}", "DataQualityRead", Response: "DataQualityPolicyData", FilteredCollection: true),
        new("v08-data-quality-evidence", "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}/evidence", "DataQualityRead", "from,to,maxPoints", Response: "DataQualityEvidencePageData", FilteredCollection: true),
        new("v08-notification-subscriptions-read", "/api/v1/notifications/subscriptions", "NotificationRead", "maxResults,afterSubscriptionId,state,eventClass", Response: "NotificationSubscriptionListData", FilteredCollection: true),
        new("v08-notification-subscription-detail", "/api/v1/notifications/subscriptions/{subscriptionId}", "NotificationRead", Response: "NotificationSubscriptionData"),
        new("v08-notification-destination-deliveries-read", "/api/v1/notifications/destinations/{destinationId}/deliveries", "NotificationRead", "maxResults,afterCreatedAtUtc,afterNotificationId", Response: "NotificationDeliveryHistoryListData"),
        new("v08-notification-delivery-detail", "/api/v1/notifications/deliveries/{notificationId}/{destinationId}", "NotificationRead", Response: "NotificationDeliveryEvidenceData"),
        new("v08-notification-destination-deliveries-query-read", "/api/v1/notifications/deliveries/history", "NotificationRead", "destinationId,maxResults,afterCreatedAtUtc,afterNotificationId", Response: "NotificationDeliveryHistoryListData", FilteredCollection: true),
        new("v08-notification-delivery-evidence-query-read", "/api/v1/notifications/deliveries/evidence/{notificationId}", "NotificationRead", "destinationId", Response: "NotificationDeliveryEvidenceData", FilteredCollection: true),
        new("v08-notification-governance-destinations", "/api/v1/notifications/management/destinations", "NotificationManage", HumanOnly: true, FilteredCollection: true),
        new("v08-notification-governance-audit", "/api/v1/notifications/management/destinations/{destinationId}/audit", "NotificationManage", "maxResults,afterRevision", HumanOnly: true),
        new("v08-notification-management-capability", "/api/v1/notifications/management/capabilities", "NotificationManage", HumanOnly: true),
        new("v08-notification-product-event-status", "/api/v1/notifications/product-events/status", "NotificationManage", HumanOnly: true),
        new("v06-fleet-capabilities", "/api/v1/fleet/capabilities", "SystemRead"),
        new("v08-bearer-read-capabilities", "/api/v1/automation/capabilities/reads", "SystemRead"),
    ]);
    private const string RecordQueries = "offset,timestampUtc,anchor,direction,maxRecords,maxBytes,minimumOffset,maximumOffset,minimumTimestampUtc,maximumTimestampUtc,keyEquals,keyPrefix,headerName,headerEquals,headerPrefix,filter,filterLanguage,decode,serdeFormat";
    public static NamedReadContract? Find(string? name) => All.SingleOrDefault(c => c.Name == name);
}
