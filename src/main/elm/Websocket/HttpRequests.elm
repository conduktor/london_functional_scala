module Websocket.HttpRequests exposing (Error, Msg(..), Payload(..), decodeInfo, getPage)

import Json.Decode as Decode
import Websocket.Model exposing (..)
import Websocket.Ports exposing (sendMessage)


type Payload
    = GotNames (List TopicName)
    | GotSize TopicName TopicSize
    | GotRecordCount TopicName RecordCount
    | GotPartitionCount TopicName PartitionCount
    | GotReplicationFactor TopicName ReplicationFactor
    | GotSpread TopicName Spread
    | Complete


type alias Error =
    String


type Msg
    = Msg (Result Error Payload)


topicNameDecoder =
    Decode.map TopicName Decode.string


topicSizeDecoder =
    Decode.map TopicSize Decode.int


recordCountDecoder =
    Decode.map RecordCount Decode.int


partitionCountDecoder =
    Decode.map PartitionCount Decode.int


replicationFactorDecoder =
    Decode.map ReplicationFactor Decode.int


spreadDecoder =
    Decode.map Spread Decode.float


topicNamesDecoder =
    Decode.list topicNameDecoder


decodePayload : String -> Decode.Decoder Payload
decodePayload t =
    case t of
        "Topics" ->
            Decode.map GotNames
                (Decode.field "topics" topicNamesDecoder)

        "RecordCountInfo" ->
            Decode.map2 GotRecordCount
                (Decode.field "topicName" topicNameDecoder)
                (Decode.field "count" recordCountDecoder)

        "PartitionInfo" ->
            Decode.map2 GotPartitionCount
                (Decode.field "topicName" topicNameDecoder)
                (Decode.field "partition" partitionCountDecoder)

        "ReplicationFactorInfo" ->
            Decode.map2 GotReplicationFactor
                (Decode.field "topicName" topicNameDecoder)
                (Decode.field "replicationFactor" replicationFactorDecoder)

        "SpreadInfo" ->
            Decode.map2 GotSpread
                (Decode.field "topicName" topicNameDecoder)
                (Decode.field "spread" spreadDecoder)

        "Size" ->
            Decode.map2 GotSize
                (Decode.field "topicName" topicNameDecoder)
                (Decode.field "size" topicSizeDecoder)

        "Complete" ->
            Decode.succeed Complete

        _ ->
            Decode.fail "bar"


decodeInfoInternal : Decode.Decoder Payload
decodeInfoInternal =
    Decode.andThen decodePayload (Decode.field "type" Decode.string)


decodeInfo : String -> Msg
decodeInfo s =
    Msg <|
        let
            result =
                Decode.decodeString decodeInfoInternal s
        in
        Result.mapError Decode.errorToString result


getPage : Page -> Cmd Msg
getPage page =
    sendMessage <| String.concat [ "{\"page\": \"", pageNumber page |> String.fromInt, "\", \"type\": \"Subscribe\"}" ]
