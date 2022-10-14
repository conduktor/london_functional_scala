module Table exposing (..)

import Model exposing (..)
import Html exposing (table, text, td, tr, th, Html, Attribute)
import Html.Attributes exposing (style)


headers = ["topic", "size in bytes", "partitions count", "records count", "spread", "replication factor"]

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
                      (List.concat
                        [ [datapointToCell (\(TopicName name) -> name) (Loaded topic.name)]
                        , [datapointToCell (\(TopicSize size) -> String.fromInt size) topic.sizeInByte]
                        , [datapointToCell (\(PartitionCount count) -> String.fromInt count) topic.partitionCount]
                        , [datapointToCell (\(RecordCount count) -> String.fromInt count) topic.recordCount]
                        , [datapointToCell (\count -> String.fromInt count) topic.spread]
                        , [datapointToCell (\(ReplicationFactor count) -> String.fromInt count) topic.replicationFactor]
                        ]
                      )

headerLine = arrayToTr headerToCell headers

tableHtml: List TopicInfo -> Html msg
tableHtml topics = table borderStyle <| List.append
        [headerLine]
        <| List.map topicToHtml topics

topicNamesToTopicInfos: List TopicName -> TopicsInfo
topicNamesToTopicInfos = List.map initialTopicInfo
