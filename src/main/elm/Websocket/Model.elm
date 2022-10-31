module Websocket.Model exposing (..)


type TopicName
    = TopicName String


type TopicSize
    = TopicSize Int


type RecordCount
    = RecordCount Int


type PartitionCount
    = PartitionCount Int


type ReplicationFactor
    = ReplicationFactor Int


type Spread
    = Spread Float


type Datapoint t
    = Undefined
    | Expired t
    | Loading t
    | Loaded t


type alias TopicInfo =
    { name : TopicName
    , sizeInByte : Datapoint TopicSize
    , partitionCount : Datapoint PartitionCount
    , recordCount : Datapoint RecordCount
    , spread : Datapoint Spread
    , replicationFactor : Datapoint ReplicationFactor
    }


topicNameToTopicInfo name =
    { name = name
    , sizeInByte = Undefined
    , partitionCount = Undefined
    , recordCount = Undefined
    , spread = Undefined
    , replicationFactor = Undefined
    }


type alias TopicsInfo =
    List TopicInfo


type Page
    = Page Int


pageNumber : Page -> Int
pageNumber page =
    case page of
        Page int ->
            int


type alias TopicsModel =
    { page : Page
    , topics : TopicsInfo
    }


applyUpdateToTopicsInfo : (TopicInfo -> TopicInfo) -> TopicName -> TopicsModel -> TopicsModel
applyUpdateToTopicsInfo update name model =
    { model
        | topics =
            List.map
                (\topicInfo ->
                    if topicInfo.name == name then
                        update topicInfo

                    else
                        topicInfo
                )
                model.topics
    }


applyRecordCount count =
    applyUpdateToTopicsInfo (\previous -> { previous | recordCount = Loaded count })


applyPartitionCount count =
    applyUpdateToTopicsInfo (\previous -> { previous | partitionCount = Loaded count })


applyReplicationFactor count =
    applyUpdateToTopicsInfo (\previous -> { previous | replicationFactor = Loaded count })


applySpread spread =
    applyUpdateToTopicsInfo (\previous -> { previous | spread = Loaded spread })


applySize size =
    applyUpdateToTopicsInfo (\previous -> { previous | sizeInByte = Loaded size })


increasePage : TopicsModel -> TopicsModel
increasePage model =
    { model
        | page = Page (pageNumber model.page + 1)
        , topics = []
    }


decreasePage : TopicsModel -> TopicsModel
decreasePage model =
    let
        page =
            pageNumber model.page
    in
    if page > 0 then
        { model
            | page =  (pageNumber model.page - 1) |> max 0 |> Page
            , topics = []
        }

    else
        model
