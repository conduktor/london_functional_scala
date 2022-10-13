module Model exposing (..)

import Dict exposing (Dict)

type TopicSize = TopicSize Int
type RecordCount = RecordCount Int
type PartitionCount = PartitionCount Int

type Datapoint t = Undefined
                 | Expired t
                 | Loading t
                 | Loaded t

type alias TopicInfo = {name: TopicName, sizeInByte: Datapoint Int, partitionCount: Datapoint Int, recordCount: Datapoint Int, spread: Datapoint Int, replicationFactor: Datapoint Int}

initialTopicInfo name = {name = name, sizeInByte = Undefined, partitionCount = Undefined, recordCount = Undefined, spread = Undefined, replicationFactor = Undefined}

type TopicName = TopicName String
type alias TopicsInfo = List TopicInfo


applyTopicSize: Dict String TopicSize -> TopicInfo -> TopicInfo
applyTopicSize sizes previous =
    let (TopicName name) = previous.name
    in let maybeSize = Dict.get name sizes
    in case maybeSize of
              Just size -> let (TopicSize sizeAsInt) = size in { previous | sizeInByte = Loaded sizeAsInt }
              Nothing -> previous

applyUpdateToTopicsInfo: TopicName -> (TopicInfo -> TopicInfo) -> TopicsInfo -> TopicsInfo
applyUpdateToTopicsInfo name update infos = List.map (\topicInfo -> if (topicInfo.name == name) then update topicInfo else topicInfo) infos

applyRecordCount: TopicName -> RecordCount -> TopicsInfo -> TopicsInfo
applyRecordCount name count infos =
    let updatedInfo previous = let (RecordCount countAsInt) = count in { previous | recordCount = Loaded countAsInt }
    in applyUpdateToTopicsInfo name updatedInfo infos

applyPartitionCount: TopicName -> PartitionCount -> TopicsInfo -> TopicsInfo
applyPartitionCount name count infos =
    let updatedInfo previous = let (PartitionCount countAsInt) = count in { previous | partitionCount = Loaded countAsInt }
    in applyUpdateToTopicsInfo name updatedInfo infos


applyTopicSizes: TopicsInfo -> Dict String TopicSize -> TopicsInfo
applyTopicSizes previous sizes = List.map (applyTopicSize sizes) previous

