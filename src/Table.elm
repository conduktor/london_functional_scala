module Table exposing (tableHtml, TopicInfo, TopicName, mkTopicName, topicNamesToTopicInfos, mkTopicInfo)

import Html exposing (table, text, td, tr, th, Html, Attribute)
import Html.Attributes exposing (style)

type Datapoint = Loading | Loaded String
type alias TopicInfo = {name: Datapoint, sizeInByte: Datapoint, partition: Datapoint, recordCount: Datapoint, spread: Datapoint, replicationFactor: Datapoint}

initialTopicInfo = {name = Loading, sizeInByte = Loading, partition = Loading, recordCount = Loading, spread = Loading, replicationFactor = Loading}
mkTopicInfo name
            sizeInByte
            partition
            recordCount
            spread
            replicationFactor = {name = Loaded name,
                                 sizeInByte = Loaded sizeInByte,
                                 partition = Loaded partition,
                                 recordCount = Loaded recordCount,
                                 spread = Loaded spread,
                                 replicationFactor = Loaded replicationFactor}

type TopicName = TopicName String

mkTopicName = TopicName

headers = ["topic", "size in bytes", "partitions", "records count", "spread", "replication factor"]

borderStyle = [style "border" "1px solid black", style "border-collapse" "collapse"]

datapointToCell: Datapoint -> Html msg
datapointToCell datapoint = case datapoint of
    Loaded s -> td borderStyle [text s]
    Loading -> td borderStyle [text "loading"]

headerToCell s = th borderStyle [text s]

arrayToTr f s = List.map f s |> tr borderStyle

topicToHtml : TopicInfo -> Html msg
topicToHtml topic = arrayToTr datapointToCell [topic.name, topic.sizeInByte, topic.partition, topic.recordCount, topic.spread, topic.replicationFactor]

headerLine = arrayToTr headerToCell headers

tableHtml: List TopicInfo -> Html msg
tableHtml topics = table borderStyle <| List.append
        [headerLine]
        <| List.map topicToHtml topics

topicNamesToTopicInfos: List TopicName -> List TopicInfo
topicNamesToTopicInfos names =
    List.map (\topicName ->
     let topicInfo = initialTopicInfo in
     let (TopicName name) = topicName in { topicInfo | name = Loaded name}) names