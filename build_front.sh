#!/bin/sh

./elm  make src/main/elm/Eventsource/Main.elm --output=src/main/elm/Eventsource/elm.js
./elm  make src/main/elm/Rest/Main.elm --output=src/main/elm/Rest/elm.js