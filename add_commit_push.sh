#!/usr/bin/env bash

# Reference:
# https://www.gnu.org/software/bash/manual/bash.html#Bash-Conditional-Expressions
# 
# -z string
#     True if the length of string is zero.
# 
# -n string
# string
#     True if the length of string is non-zero.

# Check if there are untracked files
if git status --porcelain | grep -q "^??"
then
    # There are untracked files
    git add .
fi

# Check if the work tree is clean
if [[ -z $(git status --porcelain) ]]
then
    # Git work tree is clean
    git commit --allow-empty --message "$1"
else
    # There are uncommitted changes
    git commit --all --message "$1"
fi

git push origin master
