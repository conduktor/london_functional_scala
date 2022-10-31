module Rest.Main exposing (..)

import Browser
import Html exposing (Html, text)
import Rest.HttpRequests as HttpRequests exposing (Msg(..), listNames, loadPartitionCount, loadRecordCount, loadReplicationFactor, loadSizes, loadSpread)
import Rest.Model exposing (..)
import Rest.Table exposing (..)


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


init : () -> ( Model, Cmd Msg )
init _ =
    ( LoadingNames, Cmd.map HttpMessage listNames )



-- UPDATE


type Msg
    = HttpMessage HttpRequests.Msg


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case ( msg, model ) of
        ( HttpMessage (GotNames result), LoadingNames ) ->
            case result of
                Ok topicNames ->
                    let
                        _ =
                            Debug.log "topic names " topicNames
                    in
                    ( Started (List.map topicNameToTopicInfo topicNames)
                    , Cmd.batch
                        (List.concat
                            [ [ Cmd.map HttpMessage (loadSizes topicNames) ]
                            , List.map (Cmd.map HttpMessage << loadRecordCount) topicNames
                            , List.map (Cmd.map HttpMessage << loadPartitionCount) topicNames
                            , List.map (Cmd.map HttpMessage << loadReplicationFactor) topicNames
                            , List.map (Cmd.map HttpMessage << loadSpread) topicNames
                            ]
                        )
                    )

                Err _ ->
                    ( Failure, Cmd.none )

        ( HttpMessage (GotNames _), _ ) ->
            ( Failure, Cmd.none )

        -- FIXME should work
        ( HttpMessage (GotSizes result), Started topicInfos ) ->
            case result of
                Ok topicSizes ->
                    let
                        _ =
                            Debug.log "topic sizes " topicSizes
                    in
                    ( Started (applyTopicSizes topicSizes topicInfos), Cmd.none )

                Err _ ->
                    ( Failure, Cmd.none )

        ( HttpMessage (GotSizes _), LoadingNames ) ->
            ( Failure, Cmd.none )

        -- FIXME error case
        ( HttpMessage (GotRecordCount result), Started topicInfos ) ->
            case result of
                Ok ( topicName, recordCount ) ->
                    ( Started (applyRecordCount recordCount topicName topicInfos), Cmd.none )

                Err _ ->
                    ( Failure, Cmd.none )

        ( HttpMessage (GotRecordCount _), LoadingNames ) ->
            ( Failure, Cmd.none )

        -- FIXME error case
        ( HttpMessage (GotPartitionCount result), Started topicInfos ) ->
            case result of
                Ok ( topicName, partitionCount ) ->
                    ( Started (applyPartitionCount partitionCount topicName topicInfos), Cmd.none )

                Err _ ->
                    ( Failure, Cmd.none )

        ( HttpMessage (GotPartitionCount _), LoadingNames ) ->
            ( Failure, Cmd.none )

        -- FIXME error case
        ( HttpMessage (GotReplicationFactor result), Started topicInfos ) ->
            case result of
                Ok ( topicName, replicationFactor ) ->
                    ( Started (applyReplicationFactor replicationFactor topicName topicInfos), Cmd.none )

                Err _ ->
                    ( Failure, Cmd.none )

        ( HttpMessage (GotReplicationFactor _), LoadingNames ) ->
            ( Failure, Cmd.none )

        -- FIXME error case
        ( HttpMessage (GotSpread result), Started topicInfos ) ->
            case result of
                Ok ( topicName, spread ) ->
                    ( Started (applySpread spread topicName topicInfos), Cmd.none )

                Err _ ->
                    ( Failure, Cmd.none )

        ( HttpMessage (GotSpread _), LoadingNames ) ->
            ( Failure, Cmd.none )

        -- FIXME error case
        ( HttpMessage _, Failure ) ->
            ( Failure, Cmd.none )



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none



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
