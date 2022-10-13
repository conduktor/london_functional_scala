module Model exposing (..)

import Dict exposing (Dict)

type TopicSize = TopicSize Int
type RecordCount = RecordCount Int

type Datapoint t = Undefined
                 | Expired t
                 | Loading t
                 | Loaded t

type alias TopicInfo = {name: String, sizeInByte: Datapoint Int, partition: Datapoint Int, recordCount: Datapoint Int, spread: Datapoint Int, replicationFactor: Datapoint Int}

initialTopicInfo name = {name = name, sizeInByte = Undefined, partition = Undefined, recordCount = Undefined, spread = Undefined, replicationFactor = Undefined}

type TopicName = TopicName String
type alias TopicsInfo = List TopicInfo


applyTopicSize: Dict String TopicSize -> TopicInfo -> TopicInfo
applyTopicSize sizes previous =
    let maybeSize = Dict.get previous.name sizes in
                            case maybeSize of
                              Just size -> let (TopicSize sizeAsInt) = size in { previous | sizeInByte = Loaded sizeAsInt }
                              Nothing -> previous

applyRecordCount: TopicName -> RecordCount -> TopicsInfo -> TopicsInfo
applyRecordCount name count infos =
    let updatedInfo previous = let (RecordCount countAsInt) = count in { previous | recordCount = Loaded countAsInt }
    in List.map (\topicInfo -> if (TopicName topicInfo.name == name) then updatedInfo topicInfo else topicInfo) infos

applyTopicSizes: TopicsInfo -> Dict String TopicSize -> TopicsInfo
applyTopicSizes previous sizes = List.map (applyTopicSize sizes) previous

