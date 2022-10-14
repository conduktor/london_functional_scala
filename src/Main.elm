module Main exposing (..)

import Browser
import Html exposing (Html, text)
import HttpRequests exposing (listNames, loadSizes, loadRecordCount, Msg, Msg(..))
import Table exposing (..)
import Model exposing (..)

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
  | LoadingNames
  | Started TopicsInfo

init : () -> (Model, Cmd Msg)
init _ = (LoadingNames, Cmd.map HttpMessage listNames)


-- UPDATE
type Msg = HttpMessage HttpRequests.Msg

update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case (msg, model) of
    (HttpMessage (GotNames result), LoadingNames) ->
      case result of
        Ok topicNames ->
          let _ = Debug.log "topic names " topicNames in
            (Started (topicNamesToTopicInfos topicNames),
              Cmd.batch (
                List.append
                  [Cmd.map HttpMessage (loadSizes topicNames)]
                  (List.map (Cmd.map HttpMessage << loadRecordCount) topicNames))
            )

        Err _ ->
          (Failure, Cmd.none)
    (HttpMessage (GotSizes result), Started topicInfos) ->
      case result of
        Ok topicSizes ->
          let _ = Debug.log "topic sizes " topicSizes in
            (Started (applyTopicSizes topicInfos topicSizes), Cmd.none)

        Err _ ->
          (Failure, Cmd.none)
    (HttpMessage (GotRecordCount result), Started topicInfos) ->
      case result of
        Ok (topicName, recordCount) ->
          (Started (applyRecordCount topicName recordCount topicInfos), Cmd.none)

        Err _ ->
          (Failure, Cmd.none)

    (HttpMessage _, _) ->
      (Failure, Cmd.none) -- FIXME


-- SUBSCRIPTIONS
subscriptions : Model -> Sub Msg
subscriptions model = Sub.none

-- VIEW
view : Model -> Html Msg
view model =
  case model of
    Failure ->
      text "I was unable to load topics."

    LoadingNames ->
      text "Loading..."

    Started topicInfos ->
        tableHtml topicInfos
