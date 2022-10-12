module HttpRequests exposing (listNames, loadSizes, Msg(..))

import Dict exposing (Dict)
import Http
import Model exposing (..)

import Json.Decode as Decode
import Json.Encode as Encode


topicNameDecoder = Decode.map TopicName Decode.string
topicNamesDecoder = Decode.list topicNameDecoder

topicNameEncoder (TopicName name) = Encode.string name
topicNamesEncoder names = Encode.list topicNameEncoder names

topicSizeDecoder = Decode.map TopicSize Decode.int

topicSizeResponseDecoder : Decode.Decoder (Dict String TopicSize)
topicSizeResponseDecoder = Decode.dict topicSizeDecoder

getWithBody
  : { url : String
    , body: Http.Body
    , expect : Http.Expect msg
    }
  -> Cmd msg
getWithBody r =
  Http.request
    { method = "GET"
    , headers = []
    , url = r.url
    , body = r.body
    , expect = r.expect
    , timeout = Nothing
    , tracker = Nothing
    }

type alias GotSizeResponse = { topicName: TopicName
                             , size: TopicSize }

listNames : Cmd Msg
listNames = Http.get
      { url = "http://localhost:8090/names"
      , expect = Http.expectJson GotNames topicNamesDecoder
      }

loadSizes : List TopicName -> Cmd Msg
loadSizes topics = getWithBody
                 { url = "http://localhost:8090/size"
                 , body = Http.jsonBody (topicNamesEncoder topics)
                 , expect = Http.expectJson GotSizes topicSizeResponseDecoder
                 }

type Msg = GotNames (Result Http.Error (List TopicName))
         | GotSizes (Result Http.Error (Dict String TopicSize))