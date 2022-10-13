module HttpRequests exposing (listNames, loadSizes, loadRecordCount, loadPartitionCount, Msg(..))

import Dict exposing (Dict)
import Http
import Model exposing (..)

import Json.Decode as Decode
import Json.Encode as Encode
import Url.Builder exposing (crossOrigin, string)


topicNameDecoder = Decode.map TopicName Decode.string
topicNamesDecoder = Decode.list topicNameDecoder

topicNameEncoder (TopicName name) = Encode.string name
topicNamesEncoder names = Encode.list topicNameEncoder names

topicSizeDecoder = Decode.map TopicSize Decode.int

recordCountDecoder = Decode.map RecordCount Decode.int

partitionCountDecoder = Decode.map PartitionCount Decode.int

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

targetHost = "http://localhost:8090"

listNames : Cmd Msg
listNames = Http.get
      { url = crossOrigin targetHost ["names"] []
      , expect = Http.expectJson GotNames topicNamesDecoder
      }

loadSizes : List TopicName -> Cmd Msg
loadSizes topics = getWithBody
                 { url = crossOrigin targetHost ["size"] []
                 , body = Http.jsonBody (topicNamesEncoder topics)
                 , expect = Http.expectJson GotSizes topicSizeResponseDecoder
                 }

toRecordCount: TopicName -> Result Http.Error RecordCount -> Msg
toRecordCount topicName result = GotRecordCount (Result.map (\count -> (topicName, count)) result)

loadRecordCount : TopicName -> Cmd Msg
loadRecordCount (TopicName topicName as topic) = Http.get
                                   { url = crossOrigin targetHost ["topics", topicName, "records"] [string "fields" "count"]
                                   , expect = Http.expectJson (toRecordCount topic) recordCountDecoder}

toPartitionCount: TopicName -> Result Http.Error PartitionCount -> Msg
toPartitionCount topicName result = GotPartitionCount (Result.map (\count -> (topicName, count)) result)

loadPartitionCount : TopicName -> Cmd Msg
loadPartitionCount (TopicName topicName as topic) = Http.get
                                   { url = crossOrigin targetHost ["topics", topicName, "partitions"] [string "fields" "count"]
                                   , expect = Http.expectJson (toPartitionCount topic) partitionCountDecoder}

type Msg = GotNames (Result Http.Error (List TopicName))
         | GotSizes (Result Http.Error (Dict String TopicSize))
         | GotRecordCount (Result Http.Error (TopicName, RecordCount))
         | GotPartitionCount (Result Http.Error (TopicName, PartitionCount))