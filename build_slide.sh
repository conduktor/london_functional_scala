#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

docker run \
	--rm \
	--user $UID \
	-v $SCRIPTPATH:/documents asciidoctor/docker-asciidoctor asciidoctor-revealjs \
	-a icons=font \
	-a experimental=true \
	-a idprefix= \
	-a idseparator=- \
	-a screenshot-dir-name=screenshots \
	-a source-highlighter=highlightjs \
	-a highlightjs-theme=lib/highlight/styles/gruvbox-dark.min.css \
	-a revealjsdir=https://cdnjs.cloudflare.com/ajax/libs/reveal.js/3.8.0 \
	-a revealjs_transition=slide \
	-a revealjs_slideNumber=true \
	-a revealjs_width=1100 \
	-a revealjs_height=700 \
	-a revealjs_plugins=slides/revealjs-plugins/revealjs-plugins.js \
	-a revealjs_plugins_configuration=slides/revealjs-plugins/revealjs-plugins-conf.js \
	-a docinfo=shared \
	-a toc=macro \
	-a toclevels=1 \
	-D reveal slides/*.adoc
