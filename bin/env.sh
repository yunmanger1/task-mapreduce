#!/bin/bash
xpwd=$(cd "$( dirname "$BASH_SOURCE" )"; pwd )
source $xpwd/activate
exec $@