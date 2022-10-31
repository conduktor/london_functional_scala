#!/bin/sh -eu

elm make src/main/elm/Websocket/Main.elm --output src/main/elm/Websocket/elm.js
open  src/main/elm/Websocket/index.html
