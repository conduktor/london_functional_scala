port module Eventsource.Main exposing (..)

import Browser
import Eventsource.HttpRequests as HttpRequests exposing (Error, Msg(..), Payload(..), decodeInfo)
import Eventsource.Model exposing (..)
import Eventsource.Table exposing (..)
import Html exposing (Html, text)


main =
    Browser.element
        { init = init
        , update = update
        , subscriptions = subscriptions
        , view = view
        }



-- MODEL


type Model
    = Failure String
    | LoadingNames
    | Started TopicsInfo


init : () -> ( Model, Cmd Msg )
init _ =
    ( LoadingNames, Cmd.none )



-- PORTS


port messageReceiver : (String -> msg) -> Sub msg



-- UPDATE


type Msg
    = HttpMessage HttpRequests.Msg
    | SubscriptionMessage String


failure : String -> ( Model, Cmd Msg )
failure msg =
    let
        _ =
            Debug.log msg
    in
    ( Failure msg, Cmd.none )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case ( msg, model ) of
        ( HttpMessage (HttpRequests.Msg (Err _)), _ ) ->
            let
                _ =
                    Debug.log "json decode failure"
            in
            ( Failure "json decode failure", Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotNames topicNames))), LoadingNames ) ->
            ( Started (List.map topicNameToTopicInfo topicNames)
            , Cmd.none
            )

        ( HttpMessage (HttpRequests.Msg (Ok (GotNames _))), _ ) ->
            failure "received GotNames while not being loading names"

        -- FIXME should work
        ( HttpMessage (HttpRequests.Msg (Ok (GotSize topicName topicSizes))), Started topicInfos ) ->
            ( Started (applySize topicSizes topicName topicInfos), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotSize _ _))), LoadingNames ) ->
            failure "Got sizes without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotRecordCount topicName recordCount))), Started topicInfos ) ->
            ( Started (applyRecordCount recordCount topicName topicInfos), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotRecordCount _ _))), LoadingNames ) ->
            failure "Got record counts without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotPartitionCount topicName partitionCount))), Started topicInfos ) ->
            ( Started (applyPartitionCount partitionCount topicName topicInfos), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotPartitionCount _ _))), LoadingNames ) ->
            failure "Got partition counts without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotReplicationFactor topicName replicationFactor))), Started topicInfos ) ->
            ( Started (applyReplicationFactor replicationFactor topicName topicInfos), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotReplicationFactor _ _))), LoadingNames ) ->
            failure "Got replication factor without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotSpread topicName spread))), Started topicInfos ) ->
            ( Started (applySpread spread topicName topicInfos), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotSpread _ _))), LoadingNames ) ->
            failure "Got spread without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok Complete)), _ ) ->
            ( model, Cmd.none )

        -- FIXME error case
        ( HttpMessage _, Failure f ) ->
            ( Failure f, Cmd.none )

        ( SubscriptionMessage text, state ) ->
            let
                _ =
                    Debug.log "subscription " text
            in
            ( state, Cmd.none )



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.map HttpMessage (messageReceiver decodeInfo)



-- VIEW


view : Model -> Html Msg
view model =
    case model of
        Failure error ->
            text error

        LoadingNames ->
            text "Loading..."

        Started topicInfos ->
            tableHtml topicInfos
