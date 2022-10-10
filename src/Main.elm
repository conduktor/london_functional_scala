module Main exposing (..)

import Browser
import Html exposing (Html, pre, text, div)
import Http
import Json.Decode as Decode
import Table exposing (..)

main =
  Browser.element
    { init = init
    , update = update
    , subscriptions = subscriptions
    , view = view
    }



-- MODEL


type Model
  = Failure
  | Loading
  | HasNames (List TopicName)
  | Success (List TopicInfo)

topicDecoder : Decode.Decoder TopicInfo
topicDecoder =
    Decode.map6 mkTopicInfo
        (Decode.field "name" Decode.string)
        (Decode.field "sizeInByte" Decode.string)
        (Decode.field "partitions" Decode.string)
        (Decode.field "recordCount" Decode.string)
        (Decode.field "spread" Decode.string)
        (Decode.field "replicationFactor" Decode.string)

topicsDecoder = Decode.list topicDecoder


topicNameDecoder = Decode.map mkTopicName Decode.string
topicNamesDecoder = Decode.list topicNameDecoder

init : () -> (Model, Cmd Msg)
init _ =
  ( Loading
  , Http.get
      { url = "http://localhost:8090/names"
      , expect = Http.expectJson GotNames topicNamesDecoder
      }
  )



-- UPDATE
type Msg
  = GotTopics (Result Http.Error (List TopicInfo)) |
  GotNames (Result Http.Error (List TopicName))


update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    GotTopics result ->
      case result of
        Ok topics ->
          (Success topics, Cmd.none)

        Err _ ->
          (Failure, Cmd.none)
    GotNames result ->
      case result of
        Ok topicNames ->
          let _ = Debug.log "topic names " topicNames in
            (HasNames topicNames, Cmd.none)

        Err _ ->
          (Failure, Cmd.none)



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.none



-- VIEW
view : Model -> Html Msg
view model =
  case model of
    Failure ->
      text "I was unable to load your book."

    Loading ->
      text "Loading..."

    HasNames topicNames ->
        tableHtml (topicNamesToTopicInfos topicNames)

    Success topicInfos ->
        tableHtml topicInfos
