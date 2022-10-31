module Rest.Model exposing (..)

import Dict exposing (Dict)


type TopicName
    = TopicName String


type TopicSize
    = TopicSize Int


type RecordCount
    = RecordCount Int


type PartitionCount
    = PartitionCount Int


type ReplicationFactor
    = ReplicationFactor Int


type Spread
    = Spread Float


type Datapoint t
    = Undefined
    | Expired t
    | Loading t
    | Loaded t


type alias TopicInfo =
    { name : TopicName
    , sizeInByte : Datapoint TopicSize
    , partitionCount : Datapoint PartitionCount
    , recordCount : Datapoint RecordCount
    , spread : Datapoint Spread
    , replicationFactor : Datapoint ReplicationFactor
    }


topicNameToTopicInfo name =
    { name = name
    , sizeInByte = Undefined
    , partitionCount = Undefined
    , recordCount = Undefined
    , spread = Undefined
    , replicationFactor = Undefined
    }


type alias TopicsInfo =
    List TopicInfo


applyUpdateToTopicsInfo : (TopicInfo -> TopicInfo) -> TopicName -> TopicsInfo -> TopicsInfo
applyUpdateToTopicsInfo update name =
    List.map
        (\topicInfo ->
            if topicInfo.name == name then
                update topicInfo

            else
                topicInfo
        )


applyRecordCount count =
    applyUpdateToTopicsInfo (\previous -> { previous | recordCount = Loaded count })


applyPartitionCount count =
    applyUpdateToTopicsInfo (\previous -> { previous | partitionCount = Loaded count })


applyReplicationFactor count =
    applyUpdateToTopicsInfo (\previous -> { previous | replicationFactor = Loaded count })


applySpread spread =
    applyUpdateToTopicsInfo (\previous -> { previous | spread = Loaded spread })


applyTopicSize : Dict String TopicSize -> TopicInfo -> TopicInfo
applyTopicSize sizes previous =
    let
        (TopicName name) =
            previous.name

        maybeSize =
            Dict.get name sizes
    in
    case maybeSize of
        Just size ->
            { previous | sizeInByte = Loaded size }

        Nothing ->
            previous


applyTopicSizes : Dict String TopicSize -> TopicsInfo -> TopicsInfo
applyTopicSizes sizes =
    List.map (applyTopicSize sizes)
