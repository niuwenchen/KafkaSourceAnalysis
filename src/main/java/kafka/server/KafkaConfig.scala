package kafka.server

import com.jackniu.kafka.common.config.{SaslConfigs, SslConfigs}
import com.jackniu.kafka.common.config.internal.BrokerSecurityConfigs
import com.jackniu.kafka.common.metrics.Sensor
import com.jackniu.kafka.common.record.Records
import com.jackniu.kafka.common.requests.ApiVersionsResponse.ApiVersion
import com.jackniu.kafka.common.security.auth.SecurityProtocol
import kafka.cluster.EndPoint

object KafkaConfig {
  /** ********* Zookeeper Configuration ***********/
  val ZkSessionTimeoutMs = 6000
  val ZkSyncTimeMs = 2000
  val ZkEnableSecureAcls = false
  val ZkMaxInFlightRequests = 10

  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnable = true
  val MaxReservedBrokerId = 1000
  val BrokerId = -1
  val MessageMaxBytes = 1000000 + Records.LOG_OVERHEAD
  val NumNetworkThreads = 3
  val NumIoThreads = 8
  val BackgroundThreads = 10
  val QueuedMaxRequests = 500
  val QueuedMaxRequestBytes = -1

  /************* Authorizer Configuration ***********/
  val AuthorizerClassName = ""

  /** ********* Socket Server Configuration ***********/
  val Port = 9092
  val HostName: String = new String("")

  val ListenerSecurityProtocolMap: String = EndPoint.DefaultSecurityProtocolMap.map { case (listenerName, securityProtocol) =>
    s"${listenerName.value}:${securityProtocol.name}"
  }.mkString(",")

  val SocketSendBufferBytes: Int = 100 * 1024
  val SocketReceiveBufferBytes: Int = 100 * 1024
  val SocketRequestMaxBytes: Int = 100 * 1024 * 1024
  val MaxConnectionsPerIp: Int = Int.MaxValue
  val MaxConnectionsPerIpOverrides: String = ""
  val ConnectionsMaxIdleMs = 10 * 60 * 1000L
  val RequestTimeoutMs = 30000
  val FailedAuthenticationDelayMs = 100

  /** ********* Log Configuration ***********/
  val NumPartitions = 1
  val LogDir = "/tmp/kafka-logs"
  val LogSegmentBytes = 1 * 1024 * 1024 * 1024
  val LogRollHours = 24 * 7
  val LogRollJitterHours = 0
  val LogRetentionHours = 24 * 7

  val LogRetentionBytes = -1L
  val LogCleanupIntervalMs = 5 * 60 * 1000L
  val Delete = "delete"
  val Compact = "compact"
  val LogCleanupPolicy = Delete
  val LogCleanerThreads = 1
  val LogCleanerIoMaxBytesPerSecond = Double.MaxValue
  val LogCleanerDedupeBufferSize = 128 * 1024 * 1024L
  val LogCleanerIoBufferSize = 512 * 1024
  val LogCleanerDedupeBufferLoadFactor = 0.9d
  val LogCleanerBackoffMs = 15 * 1000
  val LogCleanerMinCleanRatio = 0.5d
  val LogCleanerEnable = true
  val LogCleanerDeleteRetentionMs = 24 * 60 * 60 * 1000L
  val LogCleanerMinCompactionLagMs = 0L
  val LogIndexSizeMaxBytes = 10 * 1024 * 1024
  val LogIndexIntervalBytes = 4096
  val LogFlushIntervalMessages = Long.MaxValue
  val LogDeleteDelayMs = 60000
  val LogFlushSchedulerIntervalMs = Long.MaxValue
  val LogFlushOffsetCheckpointIntervalMs = 60000
  val LogFlushStartOffsetCheckpointIntervalMs = 60000
  val LogPreAllocateEnable = false
  // lazy val as `InterBrokerProtocolVersion` is defined later
//  lazy val LogMessageFormatVersion = InterBrokerProtocolVersion
  val LogMessageTimestampType = "CreateTime"
  val LogMessageTimestampDifferenceMaxMs = Long.MaxValue
  val NumRecoveryThreadsPerDataDir = 1
  val AutoCreateTopicsEnable = true
  val MinInSyncReplicas = 1
  val MessageDownConversionEnable = true

  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMs = RequestTimeoutMs
  val ControllerMessageQueueSize = Int.MaxValue
  val DefaultReplicationFactor = 1
  val ReplicaLagTimeMaxMs = 10000L
  val ReplicaSocketTimeoutMs = 30 * 1000
  val ReplicaSocketReceiveBufferBytes = 64 * 1024
  val ReplicaFetchMaxBytes = 1024 * 1024
  val ReplicaFetchWaitMaxMs = 500
  val ReplicaFetchMinBytes = 1
  val ReplicaFetchResponseMaxBytes = 10 * 1024 * 1024
  val NumReplicaFetchers = 1
  val ReplicaFetchBackoffMs = 1000
  val ReplicaHighWatermarkCheckpointIntervalMs = 5000L
  val FetchPurgatoryPurgeIntervalRequests = 1000
  val ProducerPurgatoryPurgeIntervalRequests = 1000
  val DeleteRecordsPurgatoryPurgeIntervalRequests = 1
  val AutoLeaderRebalanceEnable = true
  val LeaderImbalancePerBrokerPercentage = 10
  val LeaderImbalanceCheckIntervalSeconds = 300
  val UncleanLeaderElectionEnable = false
  val InterBrokerSecurityProtocol = SecurityProtocol.PLAINTEXT.toString
//  val InterBrokerProtocolVersion = ApiVersion.latestVersion.toString
//
//  /** ********* Controlled shutdown configuration ***********/
//  val ControlledShutdownMaxRetries = 3
//  val ControlledShutdownRetryBackoffMs = 5000
//  val ControlledShutdownEnable = true
//
//  /** ********* Group coordinator configuration ***********/
//  val GroupMinSessionTimeoutMs = 6000
//  val GroupMaxSessionTimeoutMs = 300000
//  val GroupInitialRebalanceDelayMs = 3000
//  val GroupMaxSize: Int = Int.MaxValue
//
//  /** ********* Offset management configuration ***********/
//  val OffsetMetadataMaxSize = OffsetConfig.DefaultMaxMetadataSize
//  val OffsetsLoadBufferSize = OffsetConfig.DefaultLoadBufferSize
//  val OffsetsTopicReplicationFactor = OffsetConfig.DefaultOffsetsTopicReplicationFactor
//  val OffsetsTopicPartitions: Int = OffsetConfig.DefaultOffsetsTopicNumPartitions
//  val OffsetsTopicSegmentBytes: Int = OffsetConfig.DefaultOffsetsTopicSegmentBytes
//  val OffsetsTopicCompressionCodec: Int = OffsetConfig.DefaultOffsetsTopicCompressionCodec.codec
//  val OffsetsRetentionMinutes: Int = 7 * 24 * 60
//  val OffsetsRetentionCheckIntervalMs: Long = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs
//  val OffsetCommitTimeoutMs = OffsetConfig.DefaultOffsetCommitTimeoutMs
//  val OffsetCommitRequiredAcks = OffsetConfig.DefaultOffsetCommitRequiredAcks
//
//  /** ********* Transaction management configuration ***********/
//  val TransactionalIdExpirationMs = TransactionStateManager.DefaultTransactionalIdExpirationMs
//  val TransactionsMaxTimeoutMs = TransactionStateManager.DefaultTransactionsMaxTimeoutMs
//  val TransactionsTopicMinISR = TransactionLog.DefaultMinInSyncReplicas
//  val TransactionsLoadBufferSize = TransactionLog.DefaultLoadBufferSize
//  val TransactionsTopicReplicationFactor = TransactionLog.DefaultReplicationFactor
//  val TransactionsTopicPartitions = TransactionLog.DefaultNumPartitions
//  val TransactionsTopicSegmentBytes = TransactionLog.DefaultSegmentBytes
//  val TransactionsAbortTimedOutTransactionsCleanupIntervalMS = TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs
//  val TransactionsRemoveExpiredTransactionsCleanupIntervalMS = TransactionStateManager.DefaultRemoveExpiredTransactionalIdsIntervalMs
//
//  /** ********* Fetch Session Configuration **************/
//  val MaxIncrementalFetchSessionCacheSlots = 1000
//
//  /** ********* Quota Configuration ***********/
//  val ProducerQuotaBytesPerSecondDefault = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault
//  val ConsumerQuotaBytesPerSecondDefault = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault
//  val NumQuotaSamples: Int = ClientQuotaManagerConfig.DefaultNumQuotaSamples
//  val QuotaWindowSizeSeconds: Int = ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds
//  val NumReplicationQuotaSamples: Int = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples
//  val ReplicationQuotaWindowSizeSeconds: Int = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds
//  val NumAlterLogDirsReplicationQuotaSamples: Int = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples
//  val AlterLogDirsReplicationQuotaWindowSizeSeconds: Int = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds

  /** ********* Transaction Configuration ***********/
  val TransactionalIdExpirationMsDefault = 604800000

  val DeleteTopicEnable = true

  val CompressionType = "producer"

  val MaxIdMapSnapshots = 2
  /** ********* Kafka Metrics Configuration ***********/
  val MetricNumSamples = 2
  val MetricSampleWindowMs = 30000
  val MetricReporterClasses = ""
  val MetricRecordingLevel = Sensor.RecordingLevel.INFO.toString()


  /** ********* Kafka Yammer Metrics Reporter Configuration ***********/
  val KafkaMetricReporterClasses = ""
  val KafkaMetricsPollingIntervalSeconds = 10

  /** ********* SSL configuration ***********/
  val SslProtocol = SslConfigs.DEFAULT_SSL_PROTOCOL
  val SslEnabledProtocols = SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS
  val SslKeystoreType = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE
  val SslTruststoreType = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE
  val SslKeyManagerAlgorithm = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM
  val SslTrustManagerAlgorithm = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM
  val SslEndpointIdentificationAlgorithm = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM
  val SslClientAuthRequired = "required"
  val SslClientAuthRequested = "requested"
  val SslClientAuthNone = "none"
  val SslClientAuth = SslClientAuthNone
  val SslPrincipalMappingRules = BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES

  /** ********* General Security configuration ***********/
  val ConnectionsMaxReauthMsDefault = 0L

  /** ********* Sasl configuration ***********/
  val SaslMechanismInterBrokerProtocol = SaslConfigs.DEFAULT_SASL_MECHANISM
  val SaslEnabledMechanisms = SaslConfigs.DEFAULT_SASL_ENABLED_MECHANISMS
  val SaslKerberosKinitCmd = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactor = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitter = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeRelogin = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRules = BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES
  val SaslLoginRefreshWindowFactor = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR
  val SaslLoginRefreshWindowJitter = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER
  val SaslLoginRefreshMinPeriodSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS
  val SaslLoginRefreshBufferSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS

  /** ********* Delegation Token configuration ***********/
  val DelegationTokenMaxLifeTimeMsDefault = 7 * 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryTimeMsDefault = 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryCheckIntervalMsDefault = 1 * 60 * 60 * 1000L

  /** ********* Password encryption configuration for dynamic configs *********/
  val PasswordEncoderCipherAlgorithm = "AES/CBC/PKCS5Padding"
  val PasswordEncoderKeyLength = 128
  val PasswordEncoderIterations = 4096



}

class  KafkaConfig{

}