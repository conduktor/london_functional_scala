module Main exposing (..)

import Html exposing (table, text, td, tr, th, Html, Attribute)
import Html.Attributes exposing (style)

headers = ["topic", "size in bytes", "partitions", "records count", "spread", "replication factor"]
data = [["topicnambur", "4232", "3", "42", "1", "3"],
        ["topicat", "32", "2", "32", "0.8", "2"],
        ["tropical", "4232", "3", "42", "1", "3"]]



borderStyle = [style "border" "1px solid black", style "border-collapse" "collapse"]

datapointToCell: String -> Html msg
datapointToCell s = td borderStyle [text s]
headerToCell s = th borderStyle [text s]

arrayToTr f s = List.map f s |> tr borderStyle

topicToHtml : List String -> Html msg
topicToHtml = arrayToTr datapointToCell

headerLine = arrayToTr headerToCell headers

main = table borderStyle (List.append
        [headerLine]
        (List.map topicToHtml data)
    )

