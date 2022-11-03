module Websocket.Main exposing (..)

import Browser
import Html exposing (Html, text)
import Websocket.Command exposing (Command(..))
import Websocket.HttpRequests as HttpRequests exposing (Error, Msg(..), Payload(..), decodeInfo, getPage)
import Websocket.Model exposing (..)
import Websocket.Ports exposing (messageReceiver, notifyReady, sendMessage)
import Websocket.Table exposing (..)


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
    | LoadingNames TopicsModel
    | Started TopicsModel
    | WaitingForWebsocket


init : () -> ( Model, Cmd Command )
init _ =
    ( WaitingForWebsocket, Cmd.none )



-- UPDATE


ignoreCommand state =
    ( state, Cmd.none )


failure : String -> ( Model, Cmd Command )
failure msg =
    let
        _ =
            Debug.log msg
    in
    ( Failure msg, Cmd.none )


update : Command -> Model -> ( Model, Cmd Command )
update msg model =
    case ( msg, model ) of
        ( HttpMessage (HttpRequests.Msg (Err _)), _ ) ->
            let
                _ =
                    Debug.log "json decode failure"
            in
            ( Failure "json decode failure", Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotNames topicNames))), LoadingNames topicsModel ) ->
            ( Started { topicsModel | topics = List.map topicNameToTopicInfo topicNames }
            , Cmd.none
            )

        ( HttpMessage (HttpRequests.Msg (Ok (GotNames _))), _ ) ->
            failure "received GotNames while not being loading names"

        -- FIXME should work
        ( HttpMessage (HttpRequests.Msg (Ok (GotSize topicName topicSizes))), Started topicsModel ) ->
            ( Started (applySize topicSizes topicName topicsModel), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotSize _ _))), LoadingNames _ ) ->
            failure "Got sizes without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotRecordCount topicName recordCount))), Started topicsModel ) ->
            ( Started (applyRecordCount recordCount topicName topicsModel), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotRecordCount _ _))), LoadingNames _ ) ->
            failure "Got record counts without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotPartitionCount topicName partitionCount))), Started topicsModel ) ->
            ( Started (applyPartitionCount partitionCount topicName topicsModel), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotPartitionCount _ _))), LoadingNames _ ) ->
            failure "Got partition counts without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotReplicationFactor topicName replicationFactor))), Started topicsModel ) ->
            ( Started (applyReplicationFactor replicationFactor topicName topicsModel), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotReplicationFactor _ _))), LoadingNames _ ) ->
            failure "Got replication factor without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok (GotSpread topicName spread))), Started topicsModel ) ->
            ( Started (applySpread spread topicName topicsModel), Cmd.none )

        ( HttpMessage (HttpRequests.Msg (Ok (GotSpread _ _))), LoadingNames _ ) ->
            failure "Got spread without having names"

        -- FIXME error case
        ( HttpMessage (HttpRequests.Msg (Ok Complete)), _ ) ->
            ( model, Cmd.none )

        -- FIXME error case
        ( _, Failure f ) ->
            ( Failure f, Cmd.none )

        ( SubscriptionMessage text, state ) ->
            let
                _ =
                    Debug.log "subscription " text
            in
            ( state, Cmd.none )

        ( NextPage, LoadingNames topicsModel ) ->
            ignoreCommand <| LoadingNames topicsModel

        ( NextPage, Started state ) ->
            let
                topicsModel =
                    increasePage state
            in
            ( LoadingNames topicsModel, Cmd.map HttpMessage (getPage <| topicsModel.page) )

        ( PreviousPage, LoadingNames topicsModel ) ->
            ignoreCommand <| LoadingNames topicsModel

        ( PreviousPage, Started state ) ->
            let
                topicsModel =
                    decreasePage state
            in
            ( LoadingNames topicsModel, Cmd.map HttpMessage (getPage <| topicsModel.page) )

        ( Ready, WaitingForWebsocket ) ->
            let
                topicsModel =
                    { page = Page 0, topics = [] }
            in
            ( LoadingNames topicsModel, Cmd.map HttpMessage (getPage <| topicsModel.page) )

        ( Ready, state ) ->
            ignoreCommand state

        ( _, WaitingForWebsocket ) ->
            failure "Waiting for websocket but received a command"



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Command
subscriptions model =
    Sub.batch
        [ Sub.map HttpMessage (messageReceiver decodeInfo)
        , notifyReady (\_ -> Ready)
        ]



-- VIEW


view : Model -> Html Command
view model =
    case model of
        Failure error ->
            text error

        WaitingForWebsocket ->
            text "Connecting to server"

        LoadingNames _ ->
            text "Loading..."

        Started topicsModel ->
            tableHtml topicsModel
