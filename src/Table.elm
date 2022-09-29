module Table exposing (tableHtml, TopicInfo)

import Html exposing (table, text, td, tr, th, Html, Attribute)
import Html.Attributes exposing (style)
type alias TopicInfo = {name: String, sizeInByte: String, partition: String, recordCount: String, spread: String, replicationFactor: String}

headers = ["topic", "size in bytes", "partitions", "records count", "spread", "replication factor"]

borderStyle = [style "border" "1px solid black", style "border-collapse" "collapse"]

datapointToCell: String -> Html msg
datapointToCell s = td borderStyle [text s]
headerToCell s = th borderStyle [text s]

arrayToTr f s = List.map f s |> tr borderStyle

topicToHtml : TopicInfo -> Html msg
topicToHtml topic = arrayToTr datapointToCell [topic.name, topic.sizeInByte, topic.partition, topic.recordCount, topic.spread, topic.replicationFactor]

headerLine = arrayToTr headerToCell headers

tableHtml: List TopicInfo -> Html msg
tableHtml topics = table borderStyle <| List.append
        [headerLine]
        <| List.map topicToHtml topics

