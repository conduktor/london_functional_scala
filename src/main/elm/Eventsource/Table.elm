module Eventsource.Table exposing (..)

import Eventsource.Model exposing (..)
import Html exposing (Attribute, Html, table, td, text, th, tr)
import Html.Attributes exposing (style)


borderStyle =
    [ style "border" "1px solid black", style "border-collapse" "collapse" ]


toCell html s =
    html borderStyle [ text s ]


dataToCell =
    toCell td


headerToCell =
    toCell th


arrayToTr f s =
    List.map f s |> tr borderStyle


datapointToCell : (t -> String) -> Datapoint t -> Html msg
datapointToCell toString datapoint =
    case datapoint of
        Undefined ->
            dataToCell "loading"

        Expired s ->
            dataToCell (toString s)

        Loaded s ->
            dataToCell (toString s)

        Loading s ->
            dataToCell (toString s)


type alias Display msg =
    TopicInfo -> Html msg


tableFormatter : List ( String, Display msg )
tableFormatter =
    [ ( "topic", \topic -> (topic.name |> Loaded) |> datapointToCell (\(TopicName name) -> name) )
    , ( "size in bytes", \topic -> topic.sizeInByte |> datapointToCell (\(TopicSize size) -> size |> String.fromInt) )
    , ( "partitions count", \topic -> topic.partitionCount |> datapointToCell (\(PartitionCount count) -> count |> String.fromInt) )
    , ( "records count", \topic -> topic.recordCount |> datapointToCell (\(RecordCount count) -> count |> String.fromInt) )
    , ( "spread", \topic -> topic.spread |> datapointToCell (\(Spread spread) -> spread |> String.fromInt) )
    , ( "replication factor", \topic -> topic.replicationFactor |> datapointToCell (\(ReplicationFactor count) -> count |> String.fromInt) )
    ]


headerLine =
    arrayToTr headerToCell <| List.map (\( header, _ ) -> header) tableFormatter


dataLine topic =
    arrayToTr (\( _, display ) -> display topic) tableFormatter


tableHtml : List TopicInfo -> Html msg
tableHtml topics =
    table borderStyle <| List.concat [ [ headerLine ], List.map dataLine topics ]
