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
  | Success (List TopicInfo)

topicDecoder : Decode.Decoder TopicInfo
topicDecoder =
    Decode.map6 TopicInfo
        (Decode.field "name" Decode.string)
        (Decode.field "sizeInByte" Decode.string)
        (Decode.field "partitions" Decode.string)
        (Decode.field "recordCount" Decode.string)
        (Decode.field "spread" Decode.string)
        (Decode.field "replicationFactor" Decode.string)

topicsDecoder = Decode.list topicDecoder

init : () -> (Model, Cmd Msg)
init _ =
  ( Loading
  , Http.get
      { url = "http://localhost:8090/all"
      , expect = Http.expectJson GotTopics topicsDecoder
      }
  )



-- UPDATE
type Msg
  = GotTopics (Result Http.Error (List TopicInfo))


update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    GotTopics result ->
      case result of
        Ok topics ->
          (Success topics, Cmd.none)

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

    Success topicInfos ->
        tableHtml topicInfos
