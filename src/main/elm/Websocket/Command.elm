module Websocket.Command exposing (..)

import Websocket.HttpRequests as HttpRequests


type Command
    = HttpMessage HttpRequests.Msg
    | SubscriptionMessage String
    | NextPage
    | PreviousPage
    | Ready
