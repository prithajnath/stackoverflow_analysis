#!/usr/bin/env bash
egrep -Rw Posts.xml -o -e "Id=\"[0-9]+\"" | sed -E 's/Id=\"([0-9]+)\"/\1/' | paste -sd+ - | bc