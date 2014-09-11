#!/bin/bash
# Argument = -b branch -n bump -v verborse

BRANCH=
BUMP_TYPE=
VERBOSE=
CURRENT_VERSION=
NEW_VERSION=
LOGFILE=
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PKG_RESOURCE="setup.py"

usage()
{
cat << EOF
usage: $0 options

Release script for version handling and git tags

OPTIONS:
   -h      Show this message
   -b      Branch to be tagged
   -n      Next bump version. Excepts major, minor, patch
   -v      Verbose
EOF
}

function get_current_version {
    CURRENT_VERSION=$(find ./setup.py -type f -print | xargs grep "version" | awk -F '[^0-9\.]+' '{ print $2 }')
    echo "Current version is: $CURRENT_VERSION"
}

function verify_new_version_doesnt_exists {
    echo "Pulling remote tags.."
    git fetch --tags &>$LOGFILE
    git describe "v$NEW_VERSION" &>$LOGFILE
    rc=$?
	if [[ $rc == 0 ]] ; then
		echo "Version already exists v$NEW_VERSION"
    	exit 137
	fi
}

function bump_version {
    IFS='.' read -ra CURRENT_VERION_ARRAY <<< "$CURRENT_VERSION"
    MAJOR_VERSION=${CURRENT_VERION_ARRAY[0]}
    MINOR_VERSION=${CURRENT_VERION_ARRAY[1]}
    PATCH_VERSION=${CURRENT_VERION_ARRAY[2]}

    if [ "$BUMP_TYPE" = "major" ]
    then
        MAJOR_VERSION=$((MAJOR_VERSION+1))
        MINOR_VERSION=0
        PATCH_VERSION=0
    fi

    if [ "$BUMP_TYPE" = "minor" ]
    then
        MINOR_VERSION=$((MINOR_VERSION+1))
        PATCH_VERSION=0
    fi

    if [ "$BUMP_TYPE" = "patch" ]
    then
        PATCH_VERSION=$((PATCH_VERSION+1))
    fi

    NEW_VERSION="$MAJOR_VERSION.$MINOR_VERSION.$PATCH_VERSION"
}

function write_new_version {
    VERSION_ROW=$(find ./setup.py -type f -print | xargs grep "version")
    VERSION_ROW_NEWVERSION=$(echo $VERSION_ROW | sed -e "s/$CURRENT_VERSION/$NEW_VERSION/g")
    sed -i -e "s/$VERSION_ROW/    $VERSION_ROW_NEWVERSION/g" setup.py
}

while getopts “hb:n:v” OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         b)
             BRANCH=$OPTARG
             ;;
         n)
             BUMP_TYPE=$(echo $OPTARG | awk '{print tolower($0)}')
             ;;
         ?)
             usage
             exit
             ;;
     esac
done

case "$BUMP_TYPE" in
    major|minor|patch) echo "Creating new $BUMP_TYPE" ;;
    *)
        echo "Option '-n' must be one of major, minor, patch. Got '$BUMP_TYPE'."
        exit
        ;;
esac

if [ "$BRANCH" = "" ]
    then
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
fi

if ! git diff-index --quiet HEAD --; then
    echo "You have uncommited changes. Please commit first."
    exit 137
fi

echo "Switching to branch $BRANCH"
git checkout "$BRANCH"

echo "Getting current version..."
get_current_version

bump_version
echo "New version will be: $NEW_VERSION"
LOGFILE="$DIR/release-$NEW_VERSION.log"

verify_new_version_doesnt_exists

echo "Creating release branch release/v$NEW_VERSION"
git checkout -b "release/v$NEW_VERSION" &>$LOGFILE

echo "Writing new version to $PKG_RESOURCE"
write_new_version

echo "Commiting changes..."
git commit -am "chore: relase v$NEW_VERSION" &>$LOGFILE
git tag "v$NEW_VERSION" -am "chore: stamp release v$NEW_VERSION" &>$LOGFILE

echo "Pushing release..."
git push origin "release/v$NEW_VERSION" &>$LOGFILE
git push --tags &>$LOGFILE

rm -rf $LOGFILE
echo "Done. Take a moment and pad your self on the back for yet another great release."