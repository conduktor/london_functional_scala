module Table exposing (tableHtml, TopicInfo, TopicsInfo, TopicName, mkTopicName, topicNamesToTopicInfos, unwrapTopicName, TopicSize, mkTopicSize, applyTopicSizes)

import Dict exposing (Dict)
import Html exposing (table, text, td, tr, th, Html, Attribute)
import Html.Attributes exposing (style)

type Datapoint t = Undefined
                 | Expired t
                 | Loading t
                 | Loaded t

type alias TopicInfo = {name: Datapoint String, sizeInByte: Datapoint Int, partition: Datapoint Int, recordCount: Datapoint Int, spread: Datapoint Int, replicationFactor: Datapoint Int}

initialTopicInfo = {name = Undefined, sizeInByte = Undefined, partition = Undefined, recordCount = Undefined, spread = Undefined, replicationFactor = Undefined}

type TopicName = TopicName String
type alias TopicsInfo = List TopicInfo

unwrapTopicName topicName = let (TopicName name) = topicName in name
mkTopicName = TopicName

type TopicSize = TopicSize Int
mkTopicSize: Int -> TopicSize
mkTopicSize size = TopicSize size

headers = ["topic", "size in bytes", "partitions", "records count", "spread", "replication factor"]

borderStyle = [style "border" "1px solid black", style "border-collapse" "collapse"]

datapointToCell: (t -> String) -> Datapoint t -> Html msg
datapointToCell toString datapoint = case datapoint of
    Undefined -> td borderStyle [text "loading"]
    Expired s -> td borderStyle [text (toString s)]
    Loaded s -> td borderStyle [text (toString s)]
    Loading s -> td borderStyle [text (toString s)]

headerToCell s = th borderStyle [text s]

arrayToTr f s = List.map f s |> tr borderStyle

topicToHtml : TopicInfo -> Html msg
topicToHtml topic = arrayToTr identity
                      (List.append
                        (List.map (datapointToCell identity) [topic.name])
                        (List.map (datapointToCell String.fromInt) [topic.sizeInByte, topic.partition, topic.recordCount, topic.spread, topic.replicationFactor]))

headerLine = arrayToTr headerToCell headers

tableHtml: List TopicInfo -> Html msg
tableHtml topics = table borderStyle <| List.append
        [headerLine]
        <| List.map topicToHtml topics

topicNamesToTopicInfos: List TopicName -> TopicsInfo
topicNamesToTopicInfos names =
    List.map (\topicName ->
     let topicInfo = initialTopicInfo in
     let (TopicName name) = topicName in { topicInfo | name = Loaded name}) names

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