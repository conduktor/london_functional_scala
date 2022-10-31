port module Websocket.Ports exposing (..)


port messageReceiver : (String -> msg) -> Sub msg


port notifyReady : (String -> msg) -> Sub msg


port sendMessage : String -> Cmd msg
