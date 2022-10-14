module Table exposing (..)

import Model exposing (..)
import Html exposing (table, text, td, tr, th, Html, Attribute)
import Html.Attributes exposing (style)


headers = ["topic", "size in bytes", "partitions count", "records count", "spread", "replication factor"]

borderStyle = [style "border" "1px solid black", style "border-collapse" "collapse"]

datapointToCell: (t -> String) -> Datapoint t -> Html msg
datapointToCell toString datapoint =
  let displayString s = td borderStyle [text s]
  in case datapoint of
    Undefined -> displayString "loading"
    Expired s -> displayString (toString s)
    Loaded s  -> displayString (toString s)
    Loading s -> displayString (toString s)

headerToCell s = th borderStyle [text s]

arrayToTr f s = List.map f s |> tr borderStyle

topicToHtml : TopicInfo -> Html msg
topicToHtml topic = arrayToTr identity
                      [ datapointToCell (\(TopicName name)          -> name)                   (topic.name |> Loaded)
                      , datapointToCell (\(TopicSize size)          -> size  |> String.fromInt) topic.sizeInByte
                      , datapointToCell (\(PartitionCount count)    -> count |> String.fromInt) topic.partitionCount
                      , datapointToCell (\(RecordCount count)       -> count |> String.fromInt) topic.recordCount
                      , datapointToCell (\(Spread count)            -> count |> String.fromInt) topic.spread
                      , datapointToCell (\(ReplicationFactor count) -> count |> String.fromInt) topic.replicationFactor
                      ]

headerLine = arrayToTr headerToCell headers

tableHtml: List TopicInfo -> Html msg
tableHtml topics = table borderStyle <| List.append
        [headerLine]
        <| List.map topicToHtml topics

topicNamesToTopicInfos: List TopicName -> TopicsInfo
topicNamesToTopicInfos = List.map initialTopicInfo
