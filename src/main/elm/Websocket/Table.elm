module Websocket.Table exposing (..)

import Html exposing (Attribute, Html, button, div, table, td, text, th, tr)
import Html.Attributes exposing (style)
import Html.Events exposing (onClick)
import Websocket.Command exposing (Command(..))
import Websocket.Model exposing (..)


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
    , ( "spread", \topic -> topic.spread |> datapointToCell (\(Spread spread) -> spread |> String.fromFloat) )
    , ( "replication factor", \topic -> topic.replicationFactor |> datapointToCell (\(ReplicationFactor count) -> count |> String.fromInt) )
    ]


headerLine =
    arrayToTr headerToCell <| List.map (\( header, _ ) -> header) tableFormatter


dataLine topic =
    arrayToTr (\( _, display ) -> display topic) tableFormatter


pageSize =
    20


navigation : TopicsModel -> Html Command
navigation model =
    div []
        [ button [ onClick PreviousPage ] [ text "<" ]
        , div [] [ text (String.fromInt <| pageNumber model.page) ]
        , button [ onClick NextPage ] [ text ">" ]
        ]


tableHtml : TopicsModel -> Html Command
tableHtml model =
    div []
        [ navigation model
        , table borderStyle <| List.concat [ [ headerLine ], List.map dataLine model.topics ]
        ]
