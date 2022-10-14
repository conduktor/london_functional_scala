module Model exposing (..)

import Dict exposing (Dict)

type TopicName = TopicName String
type TopicSize = TopicSize Int
type RecordCount = RecordCount Int
type PartitionCount = PartitionCount Int
type ReplicationFactor = ReplicationFactor Int

type Datapoint t = Undefined
                 | Expired t
                 | Loading t
                 | Loaded t

type alias TopicInfo = { name: TopicName
                       , sizeInByte: Datapoint TopicSize
                       , partitionCount: Datapoint PartitionCount
                       , recordCount: Datapoint RecordCount
                       , spread: Datapoint Int
                       , replicationFactor: Datapoint ReplicationFactor
                       }

initialTopicInfo name = { name = name
                        , sizeInByte = Undefined
                        , partitionCount = Undefined
                        , recordCount = Undefined
                        , spread = Undefined
                        , replicationFactor = Undefined
                        }

type alias TopicsInfo = List TopicInfo


applyTopicSize: Dict String TopicSize -> TopicInfo -> TopicInfo
applyTopicSize sizes previous =
    let
      (TopicName name) = previous.name
      maybeSize = Dict.get name sizes
    in case maybeSize of
              Just size -> { previous | sizeInByte = Loaded size }
              Nothing -> previous

applyUpdateToTopicsInfo: (TopicInfo -> TopicInfo) -> TopicName -> TopicsInfo -> TopicsInfo
applyUpdateToTopicsInfo update name = List.map (\topicInfo -> if (topicInfo.name == name) then update topicInfo else topicInfo)

applyRecordCount: RecordCount -> TopicName -> TopicsInfo -> TopicsInfo
applyRecordCount count = applyUpdateToTopicsInfo (\previous -> { previous | recordCount = Loaded count })

applyPartitionCount: PartitionCount -> TopicName -> TopicsInfo -> TopicsInfo
applyPartitionCount count = applyUpdateToTopicsInfo (\previous -> { previous | partitionCount = Loaded count })

applyReplicationFactor: ReplicationFactor -> TopicName -> TopicsInfo -> TopicsInfo
applyReplicationFactor count = applyUpdateToTopicsInfo (\previous -> { previous | replicationFactor = Loaded count })

applyTopicSizes: Dict String TopicSize -> TopicsInfo -> TopicsInfo
applyTopicSizes sizes = List.map (applyTopicSize sizes)

