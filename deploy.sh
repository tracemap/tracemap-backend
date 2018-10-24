#!/bin/sh
COREPATH=/usr/local/bin/tracemap
rsync -av --exclude-from=.gitignore --exclude .git --exclude deploy.sh ./ tm-deploy-staging:$COREPATH/${PWD##*/}

