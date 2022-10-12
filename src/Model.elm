module Model exposing (..)

import Dict exposing (Dict)
type TopicSize = TopicSize Int

type Datapoint t = Undefined
                 | Expired t
                 | Loading t
                 | Loaded t

type alias TopicInfo = {name: Datapoint String, sizeInByte: Datapoint Int, partition: Datapoint Int, recordCount: Datapoint Int, spread: Datapoint Int, replicationFactor: Datapoint Int}

initialTopicInfo = {name = Undefined, sizeInByte = Undefined, partition = Undefined, recordCount = Undefined, spread = Undefined, replicationFactor = Undefined}

type TopicName = TopicName String
type alias TopicsInfo = List TopicInfo


applyTopicSize: Dict String TopicSize -> TopicInfo -> TopicInfo
applyTopicSize sizes previous =
    let setSize = \name -> let maybeSize = Dict.get name sizes in
                            case maybeSize of
                              Just size -> let (TopicSize sizeAsInt) = size in { previous | sizeInByte = Loaded sizeAsInt }
                              Nothing -> previous
    in case previous.name of
                 Undefined -> previous
                 Expired t -> setSize t
                 Loading t -> setSize t
                 Loaded t -> setSize t

applyTopicSizes: TopicsInfo -> Dict String TopicSize -> TopicsInfo
applyTopicSizes previous sizes = List.map (applyTopicSize sizes) previous

